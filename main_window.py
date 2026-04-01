# gui.py
from qtpy.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                            QGroupBox, QProgressBar, QSplitter, QPushButton,
                            QLineEdit, QLabel, QFileDialog, QTextEdit,
                            QTableWidget, QTableWidgetItem, QTabWidget,
                            QHeaderView, QMessageBox, QTextBrowser, QDialog,
                            QAbstractItemView, QSpinBox, QCheckBox, QComboBox)
from qtpy.QtGui import QFont, QColor, QBrush
from qtpy.QtCore import QThread
import html
import pandas as pd
from qtpy.QtCore import Qt
import sys
import re
###yaml
import yaml
from pygments import highlight
from pygments.lexers import YamlLexer
from pygments.formatters import HtmlFormatter
from pygments.styles import get_style_by_name

# 推荐柔和风格：'friendly', 'colorful', 'vs', 'trac'
STYLE = 'friendly'  # 试试 'vs' 或 'colorful' 看你喜欢哪个
import pprint

from core.engine import BatchProcessor
from core.pipeline import Pipeline
from config.loader import load_config, generate_template, is_pipeline_config  #AVAILABLE_PROCESSORS,
from decorators.processor import ProcessingContext, PROCESSORS, PRE_PROCESSORS, POST_PROCESSORS, get_all_processors, _unregister_processor, _unregister_pre, _unregister_post
from processors import *  ##导入内置处理函数
from qtpy.QtGui import QTextCharFormat, QSyntaxHighlighter

from widgets.widgets import FileStructureWidget
from widgets.console import PythonConsoleWidget
from widgets.batch_thread import BatchWorker, Stage1ProfileWorker
from stage1_bridge import export_artifact
from datetime import datetime
from pathlib import Path
from enum import Enum
import json

from config.loader import _yaml_load, to_plain_dict, load_config, save_config, format_config_yaml


class LogLevel(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"


# 可选：定义颜色和图标
LOG_STYLES = {
    LogLevel.INFO: {
        "color": "#000000",
        "icon": "ℹ️",
        "label": "INFO"
    },
    LogLevel.SUCCESS: {
        "color": "#008000",
        "icon": "✅",
        "label": "SUCCESS"
    },
    LogLevel.WARNING: {
        "color": "#FF8C00",
        "icon": "⚠️",
        "label": "WARN"
    },
    LogLevel.ERROR: {
        "color": "#C00000",
        "icon": "❌",
        "label": "ERROR"
    },
    LogLevel.DEBUG: {
        "color": "#777777",
        "icon": "🔧",
        "label": "DEBUG"
    },
}


class YamlHighlighter(QSyntaxHighlighter):

    def __init__(self, document):
        super().__init__(document)
        self.formats = {}

        # 定义格式
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("#A020F0"))
        keyword_format.setFontWeight(QFont.Bold)
        self.formats["keyword"] = keyword_format

        key_format = QTextCharFormat()
        key_format.setForeground(QColor("#0000FF"))
        key_format.setFontWeight(QFont.Bold)
        self.formats["key"] = key_format

        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#228B22"))
        comment_format.setFontItalic(True)
        self.formats["comment"] = comment_format

        value_format = QTextCharFormat()
        value_format.setForeground(QColor("#006400"))
        self.formats["value"] = value_format

    def highlightBlock(self, text):
        self.setCurrentBlockState(0)

        # 匹配注释
        comment_start = text.find('#')
        if comment_start >= 0:
            self.setFormat(comment_start,
                           len(text) - comment_start, self.formats["comment"])

        # 匹配键（以冒号结尾）
        import re
        for match in re.finditer(r"^\s*([a-zA-Z0-9_\-]+)(\s*:)", text):
            self.setFormat(match.start(1), len(match.group(1)),
                           self.formats["key"])
            # 冒号后的内容作为值
            if match.end(2) < len(text):
                self.setFormat(match.end(2),
                               len(text) - match.end(2), self.formats["value"])

        # 布尔值/数字
        for match in re.finditer(r"\b(true|false|null|[\d\.]+)\b", text):
            self.setFormat(match.start(), len(match.group()),
                           self.formats["value"])


class WriteStream:

    def __init__(self, text_edit):
        self.text_edit = text_edit

    def write(self, text):
        if text.rstrip():  # 避免空行或纯空白刷屏
            self.text_edit.append(text.rstrip())
            self.text_edit.ensureCursorVisible()  # 自动滚动到底部

    def flush(self):
        pass


# 在类外或 BatchProcessorGUI 类中作为类变量添加
MAX_LOG_LINES = 1000  # 最大保留日志行数，防止内存爆炸


class BatchProcessorGUI(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("批处理系统")
        self.resize(1100, 750)
        self.config_path = ""
        self.root_path = ""

        self.processor = BatchProcessor()  ##批处理器
        self.context = ProcessingContext()  ##背景数据库
        self._is_pipeline_mode = False

        # 主布局
        self.main_layout = QVBoxLayout()
        self.setLayout(self.main_layout)

        # ========== 1. 路径设置区 (固定) ==========
        path_group = QGroupBox("📁 路径设置")
        path_layout = QVBoxLayout()

        self._add_path_row(path_layout, "配置文件:", self._browse_config,
                           "config_line")
        self._add_path_row(path_layout, "目标目录:", self._browse_root,
                           "root_line")
        self._add_path_row(path_layout, "插件目录:", self._browse_plugins,
                           "plugins_line")

        # 按钮行
        btn_layout = QHBoxLayout()
        #     btn_load = QPushButton("🔄 加载配置")
        #     btn_load.clicked.connect(self._load_config)
        #     btn_refresh_plugin = QPushButton("🔄 刷新插件表")
        #     btn_refresh_plugin.clicked.connect(self._refresh_plugin_table)
        #     btn_plugins = QPushButton("🔌 加载插件")
        #     btn_plugins.clicked.connect(self._load_plugins)
        self.btn_run = QPushButton("▶️ 开始处理")
        self.btn_run.setStyleSheet("font-weight: bold; color: green;")
        self.btn_run.clicked.connect(self._run_in_thread)

        self.btn_cancel = QPushButton("❌ 中断处理")
        self.btn_cancel.setStyleSheet("font-weight: bold; color: green;")
        self.btn_cancel.clicked.connect(self._cancel)
        self.btn_cancel.setEnabled(False)  ##初始禁用

        # Single button for preview + execution-status merged view
        btn_preview = QPushButton("🔎 预览/执行情况")
        btn_preview.setStyleSheet("font-weight: bold")
        btn_preview.clicked.connect(self._show_preview)

        for btn in [self.btn_run, self.btn_cancel]:
            btn_layout.addWidget(btn)
        btn_layout.addWidget(btn_preview)
        path_layout.addLayout(btn_layout)
        path_group.setLayout(path_layout)
        self.main_layout.addWidget(path_group)

        # ========== 2. 主内容区：上半（插件+配置） 和 下半（日志/结果） 可垂直调整 ==========
        main_splitter = QSplitter(Qt.Vertical)
        # --- 上半区：插件 + 配置（左右可调）---
        upper_splitter = QSplitter(Qt.Horizontal)

        # 左侧：插件区域（内部垂直分割）
        plugin_widget = QWidget()
        plugin_layout = QVBoxLayout()

        # 内部垂直分割器
        plugin_splitter = QSplitter(Qt.Vertical)

        # 插件表格
        plugin_table_group = QGroupBox("🧩 已加载插件")
        table_layout = QVBoxLayout()
        self.plugin_table = QTableWidget()
        self.plugin_table.setColumnCount(7)
        self.plugin_table.setHorizontalHeaderLabels(
            ["文件", "启用", "处理器", "类型", "优先级", "作者", "版本"])
        self.plugin_table.verticalHeader().setVisible(False)
        self.plugin_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.plugin_table.cellClicked.connect(self._on_plugin_selected)
        # 启用列头点击排序（升降序切换）
        self.plugin_table.setSortingEnabled(True)
        self._plugin_sort_order = Qt.AscendingOrder
        header = self.plugin_table.horizontalHeader()
        header.setSectionsClickable(True)
        header.sectionClicked.connect(self._on_plugin_header_clicked)
        table_layout.addWidget(self.plugin_table)
        plugin_table_group.setLayout(table_layout)

        btn_layout = QHBoxLayout()
        btn_refresh_plugin = QPushButton("🔄 刷新插件表")
        btn_refresh_plugin.clicked.connect(self._refresh_plugin_table)
        btn_plugins = QPushButton("🔌 加载插件")
        btn_plugins.clicked.connect(self._load_plugins)
        for btn in [btn_refresh_plugin, btn_plugins]:  #btn_load
            btn_layout.addWidget(btn)
        table_layout.addLayout(btn_layout)

        # 插件说明
        self.plugin_info = QTextEdit()
        self.plugin_info.setReadOnly(True)
        self.plugin_info.setStyleSheet(
            "QTextEdit { background: #f4f8f4; border: none; }")
        plugin_info_group = QGroupBox("📌 插件说明")
        info_layout = QVBoxLayout()
        info_layout.addWidget(self.plugin_info)
        plugin_info_group.setLayout(info_layout)

        # 将 GroupBox 添加到垂直分割器
        plugin_splitter.addWidget(plugin_table_group)
        plugin_splitter.addWidget(plugin_info_group)

        # 设置初始大小比例
        plugin_splitter.setSizes([300, 100])

        # 设置左侧整体布局
        plugin_layout.addWidget(plugin_splitter)
        plugin_widget.setLayout(plugin_layout)

        # 添加到上半区水平分割器
        upper_splitter.addWidget(plugin_widget)

        # 右侧：Stage 主标签（Walk Stage / Data Stage）
        stage_widget = QWidget()
        stage_layout = QVBoxLayout()
        self.stage_tabs = QTabWidget()

        walk_stage_tab = QWidget()
        walk_stage_layout = QVBoxLayout()

        config_group = QGroupBox("📄 配置文件 (config.yaml)")
        config_inner_layout = QVBoxLayout()

        self.config_textedit = QTextEdit()
        self.config_textedit.setAcceptRichText(False)
        self.config_textedit.setFont(QFont("Consolas", 10))
        self.config_textedit.setLineWrapMode(QTextEdit.NoWrap)
        self.config_textedit.setStyleSheet(
            "QTextEdit { background: #f8f8f8; border: 1px solid #ccc; }")
        self.highlighter = YamlHighlighter(self.config_textedit.document())
        config_inner_layout.addWidget(self.config_textedit)

        config_btn_layout = QHBoxLayout()
        btn_load = QPushButton("🔄 加载配置")
        btn_load.clicked.connect(self._load_config)
        btn_edit = QPushButton("📝 输出当前配置文件")
        btn_edit.clicked.connect(self._print_config)
        btn_format = QPushButton("✨ 格式化")
        btn_format.clicked.connect(self._format_config)
        btn_save = QPushButton("💾 保存配置")
        btn_save.clicked.connect(self._save_config_file)
        for btn in [btn_load, btn_edit, btn_format, btn_save]:
            config_btn_layout.addWidget(btn)
        config_inner_layout.addLayout(config_btn_layout)

        config_group.setLayout(config_inner_layout)
        walk_stage_layout.addWidget(config_group)

        # Stage 1 DataFrame explorer: list + preview + schema + describe
        stage1_group = QGroupBox("🧾 Stage 1 DataFrame 预览")
        stage1_layout = QVBoxLayout()

        stage1_toolbar = QHBoxLayout()
        btn_import_df = QPushButton("📥 导入数据")
        btn_import_df.clicked.connect(self._import_stage1_dataframe)
        btn_refresh_df = QPushButton("🔄 刷新数据集")
        btn_refresh_df.clicked.connect(self._refresh_stage1_data_view)
        lbl_rows = QLabel("预览行数")
        self.stage1_preview_rows = QSpinBox()
        self.stage1_preview_rows.setMinimum(5)
        self.stage1_preview_rows.setMaximum(500)
        self.stage1_preview_rows.setValue(50)
        lbl_mode = QLabel("模式")
        self.stage1_preview_mode = QComboBox()
        self.stage1_preview_mode.addItems(["head", "sample"])
        self.stage1_preview_mode.currentIndexChanged.connect(
            lambda _: self._refresh_stage1_preview_only())
        self.stage1_preview_rows.valueChanged.connect(
            lambda _: self._refresh_stage1_preview_only())
        stage1_toolbar.addWidget(btn_import_df)
        stage1_toolbar.addWidget(btn_refresh_df)
        stage1_toolbar.addWidget(lbl_rows)
        stage1_toolbar.addWidget(self.stage1_preview_rows)
        stage1_toolbar.addWidget(lbl_mode)
        stage1_toolbar.addWidget(self.stage1_preview_mode)
        stage1_toolbar.addStretch()
        stage1_layout.addLayout(stage1_toolbar)

        self.stage1_summary = QLabel("等待 Stage 1 产出或导入 DataFrame。")
        self.stage1_summary.setWordWrap(True)
        stage1_layout.addWidget(self.stage1_summary)

        self.stage1_profile_progress = QProgressBar()
        self.stage1_profile_progress.setVisible(False)
        self.stage1_profile_progress.setTextVisible(True)
        self.stage1_profile_progress.setFormat("正在生成大数据集摘要...")
        stage1_layout.addWidget(self.stage1_profile_progress)

        stage1_splitter = QSplitter(Qt.Horizontal)

        self.stage1_df_list = QTableWidget()
        self.stage1_df_list.setColumnCount(4)
        self.stage1_df_list.setHorizontalHeaderLabels(['Key', 'Shape', '来源', '更新时间'])
        self.stage1_df_list.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.stage1_df_list.setSelectionBehavior(QTableWidget.SelectRows)
        self.stage1_df_list.verticalHeader().setVisible(False)
        self.stage1_df_list.cellClicked.connect(self._on_stage1_df_selected)
        list_header = self.stage1_df_list.horizontalHeader()
        for col in range(self.stage1_df_list.columnCount()):
            list_header.setSectionResizeMode(col, QHeaderView.Interactive)
        self.stage1_df_list.setColumnWidth(0, 160)
        self.stage1_df_list.setColumnWidth(1, 100)
        self.stage1_df_list.setColumnWidth(2, 120)
        self.stage1_df_list.setColumnWidth(3, 140)
        stage1_splitter.addWidget(self.stage1_df_list)

        stage1_detail_widget = QWidget()
        stage1_detail_layout = QVBoxLayout()
        
        # Preview table header with pagination controls
        preview_header_layout = QHBoxLayout()
        preview_header_layout.addWidget(QLabel("当前 DataFrame 预览"))
        preview_header_layout.addStretch()
        self.stage1_pagination_prev_btn = QPushButton("◀ 上一页")
        self.stage1_pagination_prev_btn.clicked.connect(self._pagination_prev_page)
        self.stage1_pagination_label = QLabel("第 1 页 / 共 1 页")
        self.stage1_pagination_label.setMinimumWidth(120)
        self.stage1_pagination_next_btn = QPushButton("下一页 ▶")
        self.stage1_pagination_next_btn.clicked.connect(self._pagination_next_page)
        preview_header_layout.addWidget(self.stage1_pagination_prev_btn)
        preview_header_layout.addWidget(self.stage1_pagination_label)
        preview_header_layout.addWidget(self.stage1_pagination_next_btn)
        stage1_detail_layout.addLayout(preview_header_layout)
        
        self.stage1_preview_table = QTableWidget()
        self.stage1_preview_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.stage1_preview_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.stage1_preview_table.verticalHeader().setVisible(False)
        stage1_detail_layout.addWidget(self.stage1_preview_table)

        stage1_stats_split = QSplitter(Qt.Horizontal)
        self.stage1_columns_text = QTextEdit()
        self.stage1_columns_text.setReadOnly(True)
        self.stage1_columns_text.setPlaceholderText("列信息（dtype / null / unique）")
        stage1_stats_split.addWidget(self.stage1_columns_text)

        self.stage1_describe_text = QTextEdit()
        self.stage1_describe_text.setReadOnly(True)
        self.stage1_describe_text.setPlaceholderText("统计摘要（describe）")
        stage1_stats_split.addWidget(self.stage1_describe_text)
        stage1_stats_split.setSizes([300, 300])
        stage1_detail_layout.addWidget(stage1_stats_split)

        compare_row = QHBoxLayout()
        compare_row.addWidget(QLabel("对比目标"))
        self.stage1_compare_target = QComboBox()
        self.stage1_compare_target.setMinimumWidth(180)
        compare_row.addWidget(self.stage1_compare_target)
        btn_compare = QPushButton("对比当前与目标")
        btn_compare.clicked.connect(self._compare_stage1_dataframes)
        compare_row.addWidget(btn_compare)
        compare_row.addStretch()
        stage1_detail_layout.addLayout(compare_row)

        self.stage1_compare_text = QTextEdit()
        self.stage1_compare_text.setReadOnly(True)
        self.stage1_compare_text.setPlaceholderText("对比摘要：行列差异、列增减、dtype变化")
        stage1_detail_layout.addWidget(self.stage1_compare_text)

        stage1_detail_widget.setLayout(stage1_detail_layout)
        stage1_splitter.addWidget(stage1_detail_widget)
        stage1_splitter.setSizes([320, 680])
        stage1_layout.addWidget(stage1_splitter)
        stage1_group.setLayout(stage1_layout)
        walk_stage_layout.addWidget(stage1_group)
        walk_stage_tab.setLayout(walk_stage_layout)
        self.stage_tabs.addTab(walk_stage_tab, "Stage 1 (Walk)")

        data_stage_tab = QWidget()
        data_stage_layout = QVBoxLayout()

        self.data_stage_summary = QLabel("Stage 2 已独立为独立工作台。当前页仅保留桥接入口。")
        self.data_stage_summary.setWordWrap(True)
        data_stage_layout.addWidget(self.data_stage_summary)

        stage2_bridge_toolbar = QHBoxLayout()
        btn_open_stage2 = QPushButton("🚀 打开 Stage 2 工作台")
        btn_open_stage2.clicked.connect(self._open_stage2_workspace)
        stage2_bridge_toolbar.addWidget(btn_open_stage2)
        btn_export_stage2 = QPushButton("📦 导出 Stage 1 Artifact")
        btn_export_stage2.clicked.connect(self._export_stage1_to_stage2_artifact)
        stage2_bridge_toolbar.addWidget(btn_export_stage2)
        btn_refresh_stage2 = QPushButton("🔄 刷新桥接信息")
        btn_refresh_stage2.clicked.connect(self._refresh_stage2_bridge_panel)
        stage2_bridge_toolbar.addWidget(btn_refresh_stage2)
        stage2_bridge_toolbar.addStretch()
        data_stage_layout.addLayout(stage2_bridge_toolbar)

        self.stage2_bridge_summary = QLabel("等待 Stage 1 数据或 pipeline data stages。")
        self.stage2_bridge_summary.setWordWrap(True)
        data_stage_layout.addWidget(self.stage2_bridge_summary)

        self.stage2_bridge_text = QTextEdit()
        self.stage2_bridge_text.setReadOnly(True)
        self.stage2_bridge_text.setPlaceholderText("这里展示将发送给独立 Stage 2 工作台的输入与 data stage 摘要。")
        data_stage_layout.addWidget(self.stage2_bridge_text)

        data_stage_tab.setLayout(data_stage_layout)
        self.stage_tabs.addTab(data_stage_tab, "Stage 2 (Data)")

        stage_layout.addWidget(self.stage_tabs)
        stage_widget.setLayout(stage_layout)
        upper_splitter.addWidget(stage_widget)

        # 设置左右比例：插件 40%，配置 60%
        upper_splitter.setSizes([400, 700])
        main_splitter.addWidget(upper_splitter)

        #    # --- 下半区：日志与结果 Tab ---
        tab_widget = QTabWidget()

        # 初始化日志控件
        self.log = QTextBrowser()
        tab_widget.addTab(self.log, "📋 日志输出")
        self._setup_logging()  ##日志

        ## 控制台
        locals_dict = {
            'batch_processor': self.processor,
            'context': self.context,
            #    'config_path': self.config_path,
            #    'root_path': self.root_path,
            'get_config_path': lambda: self.config_path,
            'get_root_path': lambda: self.root_path,
            'pre_processors': PRE_PROCESSORS,
            'processors': PROCESSORS,
            'post_processors': POST_PROCESSORS
        }
        self.console = PythonConsoleWidget(parent=self,
                                           locals_dict=locals_dict)
        tab_widget.addTab(self.console, '💻 控制台')

        self.results_table = QTableWidget()
        self.results_table.verticalHeader().setVisible(False)
        # 启用水平头的可调整大小（默认是开启的，但确保没被关闭）
        self.results_table.horizontalHeader().setSectionsMovable(
            True)  # 可选：允许列拖动排序
        tab_widget.addTab(self.results_table, "📊 处理结果")

        main_splitter.addWidget(tab_widget)

        # 设置主分割比例：上半 60%，下半 40%
        main_splitter.setSizes([450, 300])
        self.main_layout.addWidget(main_splitter)

        self._stage1_df_meta = {}
        self._current_stage1_key = None
        self._stage1_pagination_current_page = 0
        self._stage1_pagination_total_pages = 0
        self._stage1_profile_cache = {}
        self._stage1_profile_thread = None
        self._stage1_profile_worker = None
        self._stage1_profile_request_id = 0
        self._stage1_profile_active_request_id = None
        self._stage1_profile_active_cache_key = None
        self._stage2_workspace_window = None
        self._refresh_stage1_data_view()
        self._refresh_data_stage_view()
        self._refresh_stage2_bridge_panel()

        # ========== 3. 进度条（固定） ==========
        progress_layout = QHBoxLayout()
        progress_layout.addWidget(QLabel("进度:"))
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("准备中...")
        # 在进度条上方或日志 Tab 里添加
        clear_log_btn = QPushButton("🗑️ 清空日志")
        clear_log_btn.clicked.connect(self._clear_log)
        progress_layout.addWidget(clear_log_btn)

        progress_layout.addWidget(self.progress_bar)
        self.main_layout.addLayout(progress_layout)

        # ========== 可选：全局样式美化 ==========
        self.setStyleSheet("""
            QLabel { font-size: 13px; }
            QPushButton {
                padding: 5px 10px;
                border-radius: 4px;
                background: #f0f0f0;
                border: 1px solid #ccc;
            }
            QPushButton:hover { background: #e0e0e0; }
            QPushButton:pressed { background: #d0d0d0; }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #aaa;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-position: top left;
                padding: 0 3px;
                top: -8px; /* 微调位置 */
                color: #333;
            }
            QSplitter::handle {
                background: #ccc;
                width: 2px;
            }
            QSplitter::handle:horizontal {
                width: 3px;
            }
            QSplitter::handle:vertical {
                height: 3px;
            }
        """)

    def _add_path_row(self, parent, label, browse_func, line_attr):
        row = QHBoxLayout()
        row.addWidget(QLabel(label))
        line = QLineEdit()
        setattr(self, line_attr, line)
        row.addWidget(line)
        btn = QPushButton("...")
        btn.clicked.connect(browse_func)
        row.addWidget(btn)
        parent.addLayout(row)

    def _browse_config(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "选择配置文件", "", "Config Files (*.json *.yaml *.yml)")
        if path:
            self.config_line.setText(path)
            self.config_path = path

    def _browse_root(self):
        path = QFileDialog.getExistingDirectory(self, "选择目标目录")
        if path:
            self.root_line.setText(path)
            self.root_path = path

    def _browse_plugins(self):
        path = QFileDialog.getExistingDirectory(self, "选择插件目录")
        if path:
            self.plugins_line.setText(path)

    def _load_config(self):
        """从文件或输入框加载配置并显示"""
        if not self.config_path:
            self.config_path = self.config_line.text().strip()
        if not self.config_path:
            self._log("请先选择配置文件")
            return

        try:
            self.config = load_config(self.config_path)
            self._is_pipeline_mode = is_pipeline_config(self.config)
            self._ensure_runner_from_config()

            # 格式化为 YAML 字符串显示
            yaml_str = format_config_yaml(self.config)
            self.config_textedit.setPlainText(yaml_str)
            self._refresh_stage1_data_view()
            self._refresh_data_stage_view()

            mode = "Pipeline" if self._is_pipeline_mode else "BatchProcessor"
            self._log(f"✅ 配置加载成功: {list(self.config.keys())} | 模式: {mode}")

        except Exception as e:
            self._log(f"❌ 加载失败: {e}")

    def _ensure_runner_from_config(self):
        if not hasattr(self, 'config'):
            return

        if is_pipeline_config(self.config):
            if not isinstance(self.processor, Pipeline):
                self.processor = Pipeline(stages=self.config.get('pipeline', []),
                                          context=self.context)
            else:
                self.processor.set_config(self.config)
            self._is_pipeline_mode = True
            return

        if isinstance(self.processor, Pipeline):
            self.processor = BatchProcessor()
        self._is_pipeline_mode = False
        self.processor.set_config(self.config)

    def _save_config_file(self):
        """保存当前编辑的配置到文件"""
        if not self.config_path:
            self.config_path = self.config_line.text().strip()
        if not self.config_path:
            self._log("❌ 请先加载或选择配置文件")
            return

        # 获取文本内容
        yaml_text = self.config_textedit.toPlainText().strip()
        if not yaml_text:
            self._log("❌ 配置内容为空")
            return

        try:
            # 解析验证（使用 core 的 load 逻辑）
            new_config = _yaml_load(yaml_text)
            if not isinstance(new_config, dict):
                raise ValueError("配置必须是一个对象")

            # 保存（使用 core 的 save 函数）
            save_config(new_config, self.config_path)

            # 更新内存
            self.config = new_config
            self._is_pipeline_mode = is_pipeline_config(self.config)
            self._ensure_runner_from_config()
            self._refresh_stage1_data_view()
            self._refresh_data_stage_view()

            self._log(f"✅ 配置已保存: {self.config_path}")
            # 可选：重新加载以刷新 UI（如果 save_config 没有副作用）
            # self._load_config()  # 如果你需要刷新显示

        except Exception as e:
            self._log(f"❌ 保存失败: {e}")

    def _refresh_stage1_compare_targets(self):
        if not hasattr(self, 'stage1_compare_target'):
            return
        current = self.stage1_compare_target.currentText()
        self.stage1_compare_target.blockSignals(True)
        self.stage1_compare_target.clear()
        self.stage1_compare_target.addItem('')
        for key in sorted(getattr(self.context, 'main', {}).keys()):
            if isinstance(self.context.get_main(key), pd.DataFrame):
                self.stage1_compare_target.addItem(str(key))
        if current:
            idx = self.stage1_compare_target.findText(current)
            if idx >= 0:
                self.stage1_compare_target.setCurrentIndex(idx)
        self.stage1_compare_target.blockSignals(False)

    def _collect_stage1_dataframes(self):
        dfs = {}
        for key, value in (getattr(self.context, 'main', {}) or {}).items():
            if isinstance(value, pd.DataFrame):
                dfs[str(key)] = value
                meta = self._stage1_df_meta.setdefault(str(key), {})
                meta.setdefault('source', 'stage1')
                meta.setdefault(
                    'updated_at',
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return dfs

    def _refresh_stage1_data_view(self, select_key: str = None):
        if not hasattr(self, 'stage1_df_list'):
            return

        dfs = self._collect_stage1_dataframes()
        keys = sorted(dfs.keys())
        self.stage1_df_list.setRowCount(len(keys))

        for row, key in enumerate(keys):
            df = dfs[key]
            shape = f"{len(df)} x {len(df.columns)}"
            meta = self._stage1_df_meta.get(key, {})
            src = meta.get('source', 'stage1')
            ts = meta.get('updated_at', '')
            self.stage1_df_list.setItem(row, 0, QTableWidgetItem(key))
            self.stage1_df_list.setItem(row, 1, QTableWidgetItem(shape))
            self.stage1_df_list.setItem(row, 2, QTableWidgetItem(str(src)))
            self.stage1_df_list.setItem(row, 3, QTableWidgetItem(str(ts)))

        if not keys:
            self.stage1_summary.setText("当前没有可预览的 DataFrame。可先运行 Stage 1 或导入数据。")
            self._current_stage1_key = None
            self._stage1_pagination_current_page = 0
            self._stage1_pagination_total_pages = 0
            self._cancel_stage1_profile_worker()
            if hasattr(self, 'stage1_pagination_label'):
                self.stage1_pagination_label.setText("第 1 页 / 共 1 页")
                self.stage1_pagination_prev_btn.setEnabled(False)
                self.stage1_pagination_next_btn.setEnabled(False)
            self.stage1_preview_table.setRowCount(0)
            self.stage1_preview_table.setColumnCount(0)
            self.stage1_columns_text.clear()
            self.stage1_describe_text.clear()
            self.stage1_compare_text.clear()
            self._refresh_stage1_compare_targets()
            return

        pick_key = select_key if select_key in dfs else self._current_stage1_key
        if pick_key not in dfs:
            pick_key = keys[0]

        # select row + render right side
        for row, key in enumerate(keys):
            if key == pick_key:
                self.stage1_df_list.selectRow(row)
                break

        self._current_stage1_key = pick_key
        self.stage1_summary.setText(
            f"当前数据集: {pick_key} | 总计 {len(keys)} 个 DataFrame。")
        self._render_stage1_dataframe(dfs[pick_key])
        self._refresh_stage1_compare_targets()

    def _refresh_stage1_preview_only(self):
        if not self._current_stage1_key:
            return
        df = self.context.get_main(self._current_stage1_key)
        if isinstance(df, pd.DataFrame):
            self._stage1_pagination_current_page = 0
            self._render_stage1_dataframe(df, page=0)

    def _on_stage1_df_selected(self, row, _col):
        item = self.stage1_df_list.item(row, 0)
        if item is None:
            return
        key = item.text()
        df = self.context.get_main(key)
        if isinstance(df, pd.DataFrame):
            self._current_stage1_key = key
            self.stage1_summary.setText(f"当前数据集: {key}")
            self._stage1_pagination_current_page = 0
            self._render_stage1_dataframe(df, page=0)

    def _compare_stage1_dataframes(self):
        base_key = self._current_stage1_key
        other_key = self.stage1_compare_target.currentText().strip() if hasattr(
            self, 'stage1_compare_target') else ''
        if not base_key:
            self.stage1_compare_text.setPlainText("请先选择当前 DataFrame。")
            return
        if not other_key:
            self.stage1_compare_text.setPlainText("请先选择对比目标。")
            return
        if base_key == other_key:
            self.stage1_compare_text.setPlainText("当前 DataFrame 与对比目标相同。")
            return

        base_df = self.context.get_main(base_key)
        other_df = self.context.get_main(other_key)
        if not isinstance(base_df, pd.DataFrame) or not isinstance(other_df,
                                                                  pd.DataFrame):
            self.stage1_compare_text.setPlainText("对比对象不存在或不是 DataFrame。")
            return

        base_cols = [str(c) for c in base_df.columns]
        other_cols = [str(c) for c in other_df.columns]
        base_set = set(base_cols)
        other_set = set(other_cols)
        only_base = sorted(base_set - other_set)
        only_other = sorted(other_set - base_set)
        common = sorted(base_set & other_set)

        dtype_changed = []
        for col in common:
            bdt = str(base_df[col].dtype)
            odt = str(other_df[col].dtype)
            if bdt != odt:
                dtype_changed.append((col, bdt, odt))

        lines = [
            f"当前: {base_key} | shape={base_df.shape}",
            f"目标: {other_key} | shape={other_df.shape}",
            f"行数差: {len(base_df) - len(other_df)}",
            f"列数差: {len(base_df.columns) - len(other_df.columns)}",
            f"仅当前包含列({len(only_base)}): {', '.join(only_base[:20])}",
            f"仅目标包含列({len(only_other)}): {', '.join(only_other[:20])}",
            f"共同列({len(common)})",
            f"dtype变化列({len(dtype_changed)})",
        ]
        if dtype_changed:
            lines.append("前20项 dtype 变化:")
            for col, bdt, odt in dtype_changed[:20]:
                lines.append(f"- {col}: {bdt} -> {odt}")
        self.stage1_compare_text.setPlainText('\n'.join(lines))

    def _render_stage1_dataframe(self, df: pd.DataFrame, page: int = 0):
        if not isinstance(df, pd.DataFrame):
            return

        n_rows = int(self.stage1_preview_rows.value()) if hasattr(
            self, 'stage1_preview_rows') else 50
        mode = self.stage1_preview_mode.currentText() if hasattr(
            self, 'stage1_preview_mode') else 'head'

        # Calculate pagination
        total_rows = len(df)
        self._stage1_pagination_total_pages = max(1, (total_rows + n_rows - 1) // n_rows)
        page = max(0, min(page, self._stage1_pagination_total_pages - 1))
        self._stage1_pagination_current_page = page

        preview_df = self._build_stage1_preview_chunk(df, mode, page, n_rows)

        table = self.stage1_preview_table
        table.setUpdatesEnabled(False)
        table.clear()
        table.setRowCount(len(preview_df))
        table.setColumnCount(len(preview_df.columns))
        table.setHorizontalHeaderLabels([str(c) for c in preview_df.columns])
        table.setSortingEnabled(False)
        for i, values in enumerate(preview_df.itertuples(index=False, name=None)):
            for j, val in enumerate(values):
                text = '' if pd.isna(val) else str(val)
                table.setItem(i, j, QTableWidgetItem(text))
        table.resizeColumnsToContents()
        table.setUpdatesEnabled(True)

        profile = self._get_stage1_profile_text(df)
        self.stage1_columns_text.setPlainText(profile['columns_text'])
        self.stage1_describe_text.setPlainText(profile['describe_text'])

        # Update pagination controls
        self._update_pagination_controls()

    def _build_stage1_preview_chunk(self, df: pd.DataFrame, mode: str, page: int,
                                    n_rows: int) -> pd.DataFrame:
        total_rows = len(df)
        if total_rows <= 0:
            return df.iloc[0:0]

        start_idx = page * n_rows
        end_idx = start_idx + n_rows
        if mode != 'sample':
            return df.iloc[start_idx:end_idx]

        # For sample mode, build a deterministic page-local sample without sampling
        # the full DataFrame on every render.
        stride = max(1, self._stage1_pagination_total_pages)
        positions = list(range(page, total_rows, stride))[:n_rows]
        if not positions:
            return df.iloc[0:0]
        return df.iloc[positions]

    def _get_stage1_profile_text(self, df: pd.DataFrame):
        key = getattr(self, '_current_stage1_key', None) or '<unknown>'
        cache_key = (key, id(df), df.shape)

        if (self._stage1_profile_active_request_id is not None and
                self._stage1_profile_active_cache_key != cache_key):
            self._cancel_stage1_profile_worker()

        cached = self._stage1_profile_cache.get(cache_key)
        if cached is not None:
            self._set_stage1_profile_progress_visible(False)
            return cached

        if self._should_async_stage1_profile(df):
            self._start_stage1_profile_worker(key, df, cache_key)
            return {
                'columns_text': '正在后台生成列统计，请稍候...',
                'describe_text': '正在后台生成统计摘要，请稍候...',
            }

        profile_rows = len(df)
        profile_cols = len(df.columns)
        max_profile_rows = 2000
        use_sample_profile = profile_rows > 20000 or profile_cols > 200
        profile_df = df.head(min(max_profile_rows, profile_rows)) if use_sample_profile else df

        lines = []
        if use_sample_profile:
            lines.append(
                f"大数据集优化: 列统计与 describe 基于前 {len(profile_df)} 行样本生成，完整表大小 {df.shape}。")

        col_df = pd.DataFrame({
            'column': df.columns.astype(str),
            'dtype': [str(x) for x in df.dtypes],
            'nulls': [int(profile_df[c].isna().sum()) for c in df.columns],
            'unique': [int(profile_df[c].nunique(dropna=True)) for c in df.columns],
        })
        columns_text = col_df.to_string(index=False, max_rows=100)
        if lines:
            columns_text = '\n'.join(lines + [columns_text])

        try:
            desc = profile_df.describe(include='all', datetime_is_numeric=True)
            describe_text = desc.to_string(max_rows=120)
            if use_sample_profile:
                describe_text = (
                    f"大数据集优化: describe 基于前 {len(profile_df)} 行样本。\n\n"
                    f"{describe_text}")
        except Exception:
            describe_text = "describe() 不可用或数据列为空。"

        result = {
            'columns_text': columns_text,
            'describe_text': describe_text,
        }
        self._stage1_profile_cache[cache_key] = result
        return result

    def _should_async_stage1_profile(self, df: pd.DataFrame) -> bool:
        return len(df) > 50000 or len(df.columns) > 300

    def _set_stage1_profile_progress_visible(self, visible: bool, text: str = ''):
        if not hasattr(self, 'stage1_profile_progress'):
            return
        self.stage1_profile_progress.setVisible(bool(visible))
        if text:
            self.stage1_profile_progress.setFormat(text)
        elif visible:
            self.stage1_profile_progress.setFormat('正在生成大数据集摘要...')

    def _cancel_stage1_profile_worker(self):
        thread = getattr(self, '_stage1_profile_thread', None)
        if thread is not None:
            try:
                if thread.isRunning():
                    thread.requestInterruption()
            except Exception:
                pass
        self._stage1_profile_active_request_id = None
        self._stage1_profile_active_cache_key = None
        self._set_stage1_profile_progress_visible(False)

    def _start_stage1_profile_worker(self, key: str, df: pd.DataFrame, cache_key):
        cached = self._stage1_profile_cache.get(cache_key)
        if cached is not None:
            self._set_stage1_profile_progress_visible(False)
            return

        if (self._stage1_profile_active_request_id is not None and
                self._stage1_profile_active_cache_key == cache_key):
            self._set_stage1_profile_progress_visible(True, '正在生成大数据集摘要...')
            return

        self._cancel_stage1_profile_worker()
        self._stage1_profile_request_id += 1
        request_id = self._stage1_profile_request_id
        self._stage1_profile_active_request_id = request_id
        self._stage1_profile_active_cache_key = cache_key
        self._set_stage1_profile_progress_visible(True, '正在生成大数据集摘要...')

        self._stage1_profile_worker = Stage1ProfileWorker(request_id, key, df)
        self._stage1_profile_thread = QThread()
        self._stage1_profile_worker.moveToThread(self._stage1_profile_thread)

        self._stage1_profile_thread.started.connect(self._stage1_profile_worker.run)
        self._stage1_profile_worker.progress.connect(self._on_stage1_profile_progress)
        self._stage1_profile_worker.log.connect(self._log)
        self._stage1_profile_worker.finished.connect(self._on_stage1_profile_finished)
        self._stage1_profile_worker.finished.connect(self._stage1_profile_thread.quit)
        self._stage1_profile_worker.finished.connect(self._stage1_profile_worker.deleteLater)
        self._stage1_profile_thread.finished.connect(self._stage1_profile_thread.deleteLater)

        self._stage1_profile_thread.start()

    def _on_stage1_profile_progress(self, current, total, status):
        if self._stage1_profile_active_request_id is None:
            return
        self.stage1_profile_progress.setVisible(True)
        self.stage1_profile_progress.setMaximum(max(int(total), 1))
        self.stage1_profile_progress.setValue(min(int(current), int(total)))
        self.stage1_profile_progress.setFormat(str(status))

    def _on_stage1_profile_finished(self, request_id, cache_key, result, error):
        try:
            if request_id != self._stage1_profile_active_request_id:
                return
            self._stage1_profile_active_request_id = None
            self._stage1_profile_active_cache_key = None
            self._set_stage1_profile_progress_visible(False)

            if error and error != 'cancelled':
                self.stage1_columns_text.setPlainText(f'摘要生成失败: {error}')
                self.stage1_describe_text.setPlainText(f'摘要生成失败: {error}')
                return
            if error == 'cancelled' or not result:
                return

            self._stage1_profile_cache[cache_key] = result
            current_key = getattr(self, '_current_stage1_key', None)
            current_df = self.context.get_main(current_key) if current_key else None
            if isinstance(current_df, pd.DataFrame):
                current_cache_key = (current_key, id(current_df), current_df.shape)
                if current_cache_key == cache_key:
                    self.stage1_columns_text.setPlainText(result['columns_text'])
                    self.stage1_describe_text.setPlainText(result['describe_text'])
        finally:
            self._stage1_profile_worker = None
            self._stage1_profile_thread = None

    def _update_pagination_controls(self):
        """Update pagination button states and label."""
        if not hasattr(self, 'stage1_pagination_prev_btn'):
            return
        
        current = self._stage1_pagination_current_page + 1
        total = self._stage1_pagination_total_pages
        
        self.stage1_pagination_label.setText(f"第 {current} 页 / 共 {total} 页")
        self.stage1_pagination_prev_btn.setEnabled(self._stage1_pagination_current_page > 0)
        self.stage1_pagination_next_btn.setEnabled(
            self._stage1_pagination_current_page < self._stage1_pagination_total_pages - 1)

    def _pagination_prev_page(self):
        """Navigate to previous page."""
        if self._stage1_pagination_current_page > 0:
            df = self.context.get_main(self._current_stage1_key)
            if isinstance(df, pd.DataFrame):
                self._render_stage1_dataframe(df, page=self._stage1_pagination_current_page - 1)

    def _pagination_next_page(self):
        """Navigate to next page."""
        if self._stage1_pagination_current_page < self._stage1_pagination_total_pages - 1:
            df = self.context.get_main(self._current_stage1_key)
            if isinstance(df, pd.DataFrame):
                self._render_stage1_dataframe(df, page=self._stage1_pagination_current_page + 1)

    def _import_stage1_dataframe(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "导入数据文件",
            "",
            "Data Files (*.csv *.xlsx *.xls *.parquet);;All Files (*.*)")
        if not path:
            return

        p = Path(path)
        try:
            suffix = p.suffix.lower()
            if suffix == '.csv':
                df = pd.read_csv(path)
            elif suffix in ('.xlsx', '.xls'):
                df = pd.read_excel(path)
            elif suffix == '.parquet':
                df = pd.read_parquet(path)
            else:
                raise ValueError(f'不支持的文件类型: {suffix}')

            base = p.stem
            key = base
            idx = 1
            while key in self.context.main:
                idx += 1
                key = f'{base}_{idx}'

            self.context.set_main(key, df)
            self._stage1_df_meta[key] = {
                'source': f'import:{p.name}',
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            self._refresh_stage1_data_view(select_key=key)
            self._refresh_data_stage_view()
            self._log(f"✅ 已导入 DataFrame: {key} ({len(df)} x {len(df.columns)})")
        except Exception as e:
            QMessageBox.critical(self, "导入失败", str(e))

    def _get_pipeline_data_stages(self):
        cfg = getattr(self, 'config', None)
        if not isinstance(cfg, dict):
            return []
        stages = []
        for st in (cfg.get('pipeline') or []):
            if isinstance(st, dict) and st.get('type') == 'data':
                stages.append(st)
        return stages

    def _build_stage2_bridge_project(self):
        project_name = 'stage2_bridge'
        try:
            if getattr(self, 'config_path', None):
                project_name = Path(self.config_path).stem + '_stage2'
        except Exception:
            pass

        inputs = []
        for key, value in sorted((getattr(self.context, 'main', {}) or {}).items()):
            if isinstance(value, pd.DataFrame):
                inputs.append({
                    'name': str(key),
                    'source_type': 'memory',
                    'source_params': {'df': value},
                })

        stages = []
        for stage in self._get_pipeline_data_stages():
            current = dict(stage)
            current.setdefault('type', 'data')
            stages.append(current)

        return {
            'name': project_name,
            'inputs': inputs,
            'stages': stages,
        }

    def _refresh_stage2_bridge_panel(self):
        if not hasattr(self, 'stage2_bridge_text'):
            return

        project = self._build_stage2_bridge_project()
        df_count = len(project.get('inputs', []))
        stage_count = len(project.get('stages', []))
        self.stage2_bridge_summary.setText(
            f"当前可桥接到 Stage 2 的 DataFrame: {df_count} 个 | data stages: {stage_count} 个")

        preview = {
            'name': project.get('name', 'stage2_bridge'),
            'inputs': [
                {
                    'name': item.get('name', ''),
                    'source_type': item.get('source_type', ''),
                    'rows': int(len(item['source_params']['df'])) if isinstance(item.get('source_params', {}).get('df'), pd.DataFrame) else None,
                    'cols': list(item['source_params']['df'].columns) if isinstance(item.get('source_params', {}).get('df'), pd.DataFrame) else None,
                }
                for item in project.get('inputs', [])
            ],
            'stages': [
                {
                    'name': st.get('name', 'data'),
                    'source': st.get('source', 'df'),
                    'steps': len(st.get('steps', []) or []),
                    'series': len(st.get('series', []) or []),
                }
                for st in project.get('stages', [])
            ],
        }
        self.stage2_bridge_text.setPlainText(json.dumps(preview, ensure_ascii=False, indent=2))

    def _export_stage1_to_stage2_artifact(self):
        out_dir = QFileDialog.getExistingDirectory(self, '选择导出目录', str(Path.cwd()))
        if not out_dir:
            return
        try:
            artifact_path = export_artifact(self.context, out_dir)
            self._log(f"✅ Stage 1 artifact 已导出: {artifact_path}")
            QMessageBox.information(self, '导出完成', f'Stage 1 artifact 已导出到:\n{artifact_path}')
        except Exception as e:
            QMessageBox.critical(self, '导出失败', str(e))

    def _open_stage2_workspace(self):
        try:
            from stage2_platform.ui import Stage2WorkspaceWindow

            project = self._build_stage2_bridge_project()
            self._stage2_workspace_window = Stage2WorkspaceWindow(initial_project=project)
            self._stage2_workspace_window.show()
            self._stage2_workspace_window.raise_()
            self._stage2_workspace_window.activateWindow()
        except Exception as e:
            QMessageBox.critical(self, '打开 Stage 2 失败', str(e))

    def _refresh_data_stage_view(self):
        self._refresh_stage2_bridge_panel()

    def _format_config(self):
        """格式化当前编辑区的 YAML 内容"""
        yaml_text = self.config_textedit.toPlainText().strip()
        if not yaml_text:
            return

        try:
            data = _yaml_load(yaml_text)
            formatted = format_config_yaml(data)
            self.config_textedit.setPlainText(formatted)
            self._log("✅ 配置已格式化")
        except Exception as e:
            self._log(f"❌ 无法格式化，语法错误: {e}")

##输出config字典

    def _print_config(self):
        config_ss = pprint.pformat(self.config, indent=2, width=40)
        config_ss = '配置文件如下：\n' + config_ss
        self._log(config_ss, level=LogLevel.INFO)

    def _run(self):
        if not hasattr(self, 'config'):
            self._load_config()
        if not hasattr(self, 'config'):
            return
        if not self.root_path:
            self.root_path = self.root_line.text().strip()
        if not self.root_path:
            self._log("请指定目标目录")
            return

        try:
            self._ensure_runner_from_config()
            #    self.processor = BatchProcessor(self.config)  #, AVAILABLE_PROCESSORS
            self.processor.set_config(self.config)
            self._log(f"✅ 批处理器构建完毕!")
            processor = self.processor

            # 设置进度回调
            def progress_callback(current, total, status="处理中"):
                self.progress_bar.setMaximum(total)
                self.progress_bar.setValue(current)
                self.progress_bar.setFormat(f"{status} [{current}/{total}]")

            processor.set_progress_callback(progress_callback)

            # 重定向日志
            import sys
            old_stdout = sys.stdout
            sys.stdout = WriteStream(self.log)

            self._log(f"🔄 开始进行批处理...")
            processor.run(self.root_path, self.context)
            self._log(f"✅ 批处理完毕!")
            sys.stdout = old_stdout
            #            self._log(captured_output.getvalue())
            self.progress_bar.setFormat("完成")
            self._show_results(self.context.results)  # 显示结果
        except Exception as e:
            self._log(f"❌ 运行失败: {e}")

        self._show_results(self.context.results)

    #新开一个线程运行程序
    def _run_in_thread(self):
        # 清空上下文，避免旧数据污染
        self.context.clear()  # 需要在 ProcessingContext 中实现 clear()
        self._current_stage1_key = None
        if hasattr(self, 'stage1_compare_text'):
            self.stage1_compare_text.clear()
        self._refresh_stage1_data_view()
        self._refresh_data_stage_view()
        if not hasattr(self, 'config'):
            self._load_config()
        if not hasattr(self, 'config'):
            return
        if not self.root_path:
            self.root_path = self.root_line.text().strip()
        if not self.root_path:
            self._log("请指定目标目录")
            return

        # ✅ 获取用户启用的插件
        try:
            pre_proc, main_proc, post_proc = self._get_enabled_processors_from_table(
            )
        except Exception as e:
            self._log(f"❌ 获取启用插件失败: {e}", level=LogLevel.ERROR)
            return

        # ✅ 禁用按钮
        self.btn_run.setEnabled(False)
        self.btn_cancel.setEnabled(True)  # 如果有取消按钮
        self._ensure_runner_from_config()
        # ✅ 关键：注入用户选择的处理器
        self.processor.set_config(self.config)
        if not isinstance(self.processor, Pipeline):
            self.processor.set_processors(pre=pre_proc,
                                          main=main_proc,
                                          post=post_proc)
        self._log(
            f"✅ 批处理器构建完毕!启用插件: {len(pre_proc)+len(main_proc)+len(post_proc)} 个"
        )

        # 设置进度回调
        def progress_callback(current, total, status="处理中"):
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
            self.progress_bar.setFormat(f"{status} [{current}/{total}]")

        # 创建 worker 和线程
        self.worker = BatchWorker(self.processor, self.root_path, self.context)
        self.thread = QThread()

        # 移动到线程
        self.worker.moveToThread(self.thread)

        # 连接信号
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self._on_worker_finished)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)

        self.worker.log.connect(self._log)
        self.worker.progress.connect(self._on_progress)
        # connect per-step signals for accurate status updates in preview
        try:
            self.worker.step_started.connect(self._on_step_started)
            self.worker.step_finished.connect(self._on_step_finished)
        except Exception:
            pass

        # 启动
        self.thread.start()

    def _cancel(self):
        """用户点击取消"""
        if hasattr(self, 'thread') and self.thread.isRunning():
            self.thread.requestInterruption()  # 请求中断
            self._log("🛑 正在请求取消批处理，请稍候...")
            self.btn_cancel.setEnabled(False)  # 防止重复点击
            return

    def _on_progress(self, current, total, status):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
        self.progress_bar.setFormat(f"{status} [{current}/{total}]")
        # If a preview table is open, update its Status column based on step index
        try:
            if hasattr(self, '_preview_step_map') and hasattr(
                    self, '_preview_exec_table'):
                # mark previous running step as Success (if any)
                prev = getattr(self, '_preview_running_step', None)
                if prev is not None and prev != current and prev in self._preview_step_map:
                    prev_row = self._preview_step_map.get(prev)
                    item = self._preview_exec_table.item(prev_row, 6)
                    if item:
                        item.setText('Success')
                        item.setBackground(QBrush(QColor(200, 255, 200)))

                # mark current step as Running (if present in preview)
                if isinstance(current,
                              int) and current in self._preview_step_map:
                    row = self._preview_step_map.get(current)
                    item = self._preview_exec_table.item(row, 6)
                    if item is None:
                        item = QTableWidgetItem('Running')
                        self._preview_exec_table.setItem(row, 6, item)
                    else:
                        item.setText('Running')
                    item.setBackground(QBrush(QColor(255, 250, 200)))
                    self._preview_running_step = current
        except Exception:
            pass

    def _on_step_started(self, step):
        try:
            # ensure we have a normalized root for persisted status mapping
            try:
                current_root = self.root_line.text().strip() if hasattr(
                    self, 'root_line') else self.root_path
                root_norm = str(Path(current_root))
            except Exception:
                try:
                    root_norm = str(self.root_path)
                except Exception:
                    root_norm = ''

            if not hasattr(self, '_last_preview_status') or getattr(
                    self, '_last_preview_root', None) != root_norm:
                # initialize or reset status map for this root
                self._last_preview_status = {}
                self._last_preview_root = root_norm

            if hasattr(self, '_preview_step_map') and hasattr(
                    self, '_preview_exec_table'):
                if step in self._preview_step_map:
                    row = self._preview_step_map.get(step)
                    item = self._preview_exec_table.item(row, 6)
                    if item is None:
                        item = QTableWidgetItem('Running')
                        self._preview_exec_table.setItem(row, 6, item)
                    else:
                        item.setText('Running')
                    item.setBackground(QBrush(QColor(255, 250, 200)))
                    self._preview_running_step = step
            # persist status across preview reopenings
            try:
                self._last_preview_status[int(step)] = 'Running'
            except Exception:
                pass
        except Exception:
            pass

    def _on_step_finished(self, step, success, msg):
        try:
            # ensure we have a normalized root for persisted status mapping
            try:
                current_root = self.root_line.text().strip() if hasattr(
                    self, 'root_line') else self.root_path
                root_norm = str(Path(current_root))
            except Exception:
                try:
                    root_norm = str(self.root_path)
                except Exception:
                    root_norm = ''

            if not hasattr(self, '_last_preview_status') or getattr(
                    self, '_last_preview_root', None) != root_norm:
                # initialize or reset status map for this root
                self._last_preview_status = {}
                self._last_preview_root = root_norm

            if hasattr(self, '_preview_step_map') and hasattr(
                    self, '_preview_exec_table'):
                if step in self._preview_step_map:
                    # update UI cell
                    row = self._preview_step_map.get(step)
                    item = self._preview_exec_table.item(row, 6)
                    if item is None:
                        item = QTableWidgetItem(
                            'Success' if success else 'Failed')
                        self._preview_exec_table.setItem(row, 6, item)
                    else:
                        item.setText('Success' if success else 'Failed')
                    if success:
                        item.setBackground(QBrush(QColor(200, 255, 200)))
                    else:
                        item.setBackground(QBrush(QColor(255, 200, 200)))
                        # attach error message to tooltip
                        if msg:
                            item.setToolTip(msg)
                    # set error column and clear running marker if it matches
                    try:
                        # ensure error mapping exists
                        if not hasattr(self, '_last_preview_errors'):
                            self._last_preview_errors = {}
                        self._last_preview_errors[int(step)] = msg or ''
                        err_item = self._preview_exec_table.item(row, 7)
                        if err_item is None:
                            err_item = QTableWidgetItem(msg or '')
                            self._preview_exec_table.setItem(row, 7, err_item)
                        else:
                            err_item.setText(msg or '')
                        if msg:
                            err_item.setToolTip(msg)
                    except Exception:
                        pass
                    # clear running marker if it matches
                    if getattr(self, '_preview_running_step', None) == step:
                        self._preview_running_step = None
            # persist status across preview reopenings
            try:
                self._last_preview_status[int(
                    step)] = 'Success' if success else 'Failed'
            except Exception:
                pass
        except Exception:
            pass

    def _on_worker_finished(self, context):
        self._log("✅ 批处理完成！")
        self.progress_bar.setFormat("完成")
        self._show_results(context.results)
        # Refresh Stage 1 dataframe registry from context.main after run.
        for key, value in (getattr(context, 'main', {}) or {}).items():
            if isinstance(value, pd.DataFrame):
                meta = self._stage1_df_meta.setdefault(str(key), {})
                if str(meta.get('source', '')).startswith('import:'):
                    continue
                meta['source'] = 'stage1/run'
                meta['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._refresh_stage1_data_view(select_key=self._current_stage1_key)
        self._refresh_data_stage_view()
        self.btn_run.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        # Finalize preview table statuses if present
        try:
            if hasattr(self, '_preview_step_map') and hasattr(
                    self, '_preview_exec_table'):
                # mark any running step as Success
                prev = getattr(self, '_preview_running_step', None)
                if prev is not None and prev in self._preview_step_map:
                    prev_row = self._preview_step_map.get(prev)
                    item = self._preview_exec_table.item(prev_row, 6)
                    if item:
                        item.setText('Success')
                        item.setBackground(QBrush(QColor(200, 255, 200)))
                # clear mapping after finishing
                try:
                    del self._preview_step_map
                    del self._preview_exec_table
                    del self._preview_running_step
                except Exception:
                    pass
        except Exception:
            pass

    ##程序执行后， 显示metadata
    def _show_metadata_info(self):
        if self.processor is None:
            QMessageBox.warning(self, "警告", "批处理器未运行，无法显示信息！")
            return
        if self.context is None:
            QMessageBox.warning(self, "错误", "批处理器运行错误，未定义context！")
            return
        context = self.context
        colnames = context.meta_colnames
        metadata = context.metadata

        ##显示
        dialog = QDialog(self)
        dialog.setWindowTitle(" metadata")
        dialog.resize(800, 600)

        layout = QVBoxLayout(dialog)
        try:
            file_widget = FileStructureWidget(metadata, column_names=colnames)
            layout.addWidget(file_widget)
        except Exception as e:
            QMessageBox.critical(dialog, "错误", f"加载元数据失败：{str(e)}")
            dialog.close()
            return

        dialog.exec_()  # 显示对话框（模态）

    def _show_preview(self, show_only_executed: bool = False):
        """Show a preview plan (dry-run) for the configured root path.

        If `show_only_executed` is True, the dialog will initially hide rows
        that are still `Planned` (show only steps that have a recorded
        status like Success/Failed)."""
        root = self.root_line.text().strip() if hasattr(
            self, 'root_line') else self.root_path
        if not root:
            QMessageBox.warning(self, "警告", "请先在目标目录中选择或填写要预览的根路径。")
            return

        # Ensure config is loaded before preview; otherwise pipeline UI tabs
        # may be hidden because mode detection has no config to evaluate.
        if (not hasattr(self, 'config')) or (not self.config):
            try:
                self._load_config()
            except Exception:
                pass
        if (not hasattr(self, 'config')) or (not self.config):
            QMessageBox.warning(self, "警告", "请先加载配置文件，再进行预览。")
            return

        try:
            self._ensure_runner_from_config()
            # ensure processor has the currently loaded config
            if hasattr(self, 'config') and self.config:
                try:
                    self.processor.set_config(self.config)
                except Exception:
                    pass

            # normalize root for consistent caching/lookup
            try:
                root_norm = str(Path(root))
            except Exception:
                root_norm = str(root)

            actions = self.processor.simulate(root, max_items=1000)
        except Exception as e:
            QMessageBox.critical(self, "错误", f"预览失败: {e}")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("🔎 预览计划 (Dry-run)")
        dialog.resize(900, 600)
        layout = QVBoxLayout(dialog)

        # Build a nested dict suitable for FileStructureWidget:
        # structure: { 'name/': { ... }, 'name': [col1, col2, col3], ... }
        def build_tree(actions_list):
            root_dict = {}

            for a in actions_list:
                path = a.get('path', '.')
                parts = [] if path in ('.', '') else path.split('/')
                cur = root_dict

                for i, part in enumerate(parts):
                    is_last = (i == len(parts) - 1)
                    if is_last:
                        # leaf (file or dir)
                        cols = []
                        pre = ', '.join(
                            [p['name'] for p in a.get('pre_processors', [])])
                        proc = ', '.join(
                            [p['name'] for p in a.get('processors', [])])
                        post = ', '.join(
                            [p['name'] for p in a.get('post_processors', [])])
                        cols = [pre, proc, post]

                        if a.get('is_dir'):
                            # ensure folder container
                            folder_key = part + '/'
                            if folder_key not in cur:
                                cur[folder_key] = {}
                            # set attribute for folder name
                            cur[part] = cols
                            # descend into folder dict for children
                            cur = cur[folder_key]
                        else:
                            cur[part] = cols
                    else:
                        # intermediate folder: ensure both 'name/' and placeholder
                        folder_key = part + '/'
                        if folder_key not in cur:
                            cur[folder_key] = {}
                        if part not in cur:
                            cur[part] = ["", "", ""]
                        cur = cur[folder_key]

                # special-case root entry when path == '.'
                if not parts:
                    pre = ', '.join(
                        [p['name'] for p in a.get('pre_processors', [])])
                    proc = ', '.join(
                        [p['name'] for p in a.get('processors', [])])
                    post = ', '.join(
                        [p['name'] for p in a.get('post_processors', [])])
                    root_dict['.'] = [pre, proc, post]

            return root_dict

        tree_data = build_tree(actions)

        # Create tabs: Tree view and Execution order view
        tabs = QTabWidget()

        # --- Tree tab ---
        tree_tab = QWidget()
        tree_layout = QVBoxLayout()
        try:
            file_widget = FileStructureWidget(
                tree_data, column_names=['Pre', 'Processors', 'Post'])
            tree_layout.addWidget(file_widget)
        except Exception:
            txt = QTextEdit()
            txt.setReadOnly(True)
            try:
                pretty = json.dumps(actions, ensure_ascii=False, indent=2)
            except Exception:
                pretty = str(actions)
            txt.setPlainText(pretty)
            tree_layout.addWidget(txt)
        tree_tab.setLayout(tree_layout)
        tabs.addTab(tree_tab, "Tree View")

        # --- Execution order tab ---
        exec_tab = QWidget()
        exec_layout = QVBoxLayout()
        is_pipeline = is_pipeline_config(getattr(self, 'config', {}))
        # Folding controls: allow collapsing rows deeper than selected level
        fold_layout = QHBoxLayout()
        lbl_fold = QLabel("只显示层级 ≤")
        spin_fold = QSpinBox()
        spin_fold.setMinimum(0)
        spin_fold.setMaximum(50)
        spin_fold.setValue(0)  # 0 表示不折叠（显示所有）
        chk_fold = QCheckBox("启用折叠")

        fold_layout.addWidget(lbl_fold)
        fold_layout.addWidget(spin_fold)
        fold_layout.addWidget(chk_fold)

        # Stage filter dropdown (pipeline mode only)
        stage_filter_combo = None
        if is_pipeline:
            fold_layout.addSpacing(16)
            lbl_stage = QLabel("Stage:")
            stage_filter_combo = QComboBox()
            stage_filter_combo.addItem("All stages")
            fold_layout.addWidget(lbl_stage)
            fold_layout.addWidget(stage_filter_combo)

        fold_layout.addStretch()
        exec_layout.addLayout(fold_layout)
        exec_table = QTableWidget()
        n_cols = 10 if is_pipeline else 9
        exec_table.setColumnCount(n_cols)
        base_headers = ['Step', 'Phase', 'Level', 'Path', 'IsDir', 'Processor', 'Status', 'Error', 'Config']
        exec_table.setHorizontalHeaderLabels(base_headers + (['Stage'] if is_pipeline else []))
        # Make table read-only (no in-place edits) and selectable by row
        exec_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        exec_table.setSelectionBehavior(QTableWidget.SelectRows)
        # Configure column resize modes: make Path column stretch, keep others interactive
        header = exec_table.horizontalHeader()
        # Allow user to freely resize any column
        for col in range(exec_table.columnCount()):
            header.setSectionResizeMode(col, QHeaderView.Interactive)
        # Allow columns to be reordered by dragging the headers
        try:
            header.setSectionsMovable(True)
        except Exception:
            pass
        # Keep sensible initial widths but allow changes
        exec_table.setColumnWidth(0, 64)  # Step (initial)
        exec_table.setColumnWidth(2, 56)  # Level (initial)
        exec_table.setColumnWidth(6, 84)  # Status (initial)
        if is_pipeline:
            exec_table.setColumnWidth(9, 110)  # Stage (initial)
        exec_layout.addWidget(exec_table)

        # connect fold + stage filter controls to hide/show rows
        def apply_fold():
            try:
                enabled = chk_fold.isChecked()
                max_level = int(spin_fold.value())
                stage_sel = stage_filter_combo.currentText() if stage_filter_combo is not None else 'All stages'
                for r in range(exec_table.rowCount()):
                    lvl_item = exec_table.item(r, 2)
                    try:
                        lvl = int(lvl_item.text()) if lvl_item is not None else 0
                    except Exception:
                        lvl = 0
                    hide = enabled and (lvl > max_level)
                    if not hide and stage_sel and stage_sel != 'All stages' and stage_filter_combo is not None:
                        stage_item = exec_table.item(r, 9)
                        row_stage = stage_item.text() if stage_item is not None else ''
                        hide = (row_stage != stage_sel)
                    exec_table.setRowHidden(r, hide)
            except Exception:
                pass

        chk_fold.stateChanged.connect(lambda _: apply_fold())
        spin_fold.valueChanged.connect(lambda _: apply_fold())
        if stage_filter_combo is not None:
            stage_filter_combo.currentIndexChanged.connect(lambda _: apply_fold())
        exec_tab.setLayout(exec_layout)
        tabs.addTab(exec_tab, "Execution order")

        layout.addWidget(tabs)

        # Populate execution order table
        try:
            seq = self.processor.simulate(root, sequence=True)
            steps = seq.get('steps', []) if isinstance(seq, dict) else []
            exec_table.setRowCount(len(steps))
            # store mapping step -> row for live updates from worker
            self._preview_step_map = {}
            self._preview_exec_table = exec_table
            self._preview_running_step = None
            # maintain persistent last-known statuses across preview openings per-root
            if not hasattr(self, '_last_preview_status'):
                self._last_preview_status = {}
                self._last_preview_errors = {}
                self._last_preview_root = root_norm
            elif getattr(self, '_last_preview_root', None) != root_norm:
                # different root: reset stored statuses
                self._last_preview_status = {}
                self._last_preview_errors = {}
                self._last_preview_root = root_norm
            for i, s in enumerate(steps):
                # Step and phase
                exec_table.setItem(i, 0, QTableWidgetItem(str(s.get('step'))))
                exec_table.setItem(i, 1, QTableWidgetItem(s.get('phase', '')))

                # Compute level (depth) from path string
                path_raw = s.get('path', '') or ''
                if path_raw in ('.', ''):
                    level = 0
                    display_name = '.'
                else:
                    parts = [p for p in re.split(r"[\\/]+", path_raw) if p]
                    level = max(0, len(parts) - 1)
                    display_name = parts[-1] if parts else path_raw

                # Level column (numeric)
                lvl_item = QTableWidgetItem(str(level))
                lvl_item.setTextAlignment(Qt.AlignCenter)
                exec_table.setItem(i, 2, lvl_item)

                # Path column: show tree-style prefix and emoji icon, full path in tooltip
                # Build tree prefix: use '│   ' for intermediate levels and '└─ ' for the final branch
                if level <= 0:
                    prefix = ''
                else:
                    parts_prefix = []
                    for d in range(level):
                        if d < level - 1:
                            parts_prefix.append('│   ')
                        else:
                            parts_prefix.append('└─ ')
                    prefix = ''.join(parts_prefix)

                # Emoji icon for folder/file
                is_dir = bool(s.get('is_dir'))
                icon = '📁 ' if is_dir else '📄 '

                path_item = QTableWidgetItem(f"{prefix}{icon}{display_name}")
                path_item.setToolTip(path_raw)
                exec_table.setItem(i, 3, path_item)

                # IsDir and processor
                exec_table.setItem(i, 4,
                                   QTableWidgetItem(str(s.get('is_dir'))))
                exec_table.setItem(i, 5,
                                   QTableWidgetItem(s.get('proc_name', '')))

                # Status column: prefer persisted status if available
                step_idx = None
                try:
                    step_idx = int(s.get('step'))
                except Exception:
                    step_idx = None

                last_status = 'Planned'
                if step_idx is not None:
                    last_status = self._last_preview_status.get(
                        step_idx, 'Planned')

                status_item = QTableWidgetItem(last_status)
                # apply color for known statuses
                if last_status == 'Running':
                    status_item.setBackground(QBrush(QColor(255, 250, 200)))
                elif last_status == 'Success':
                    status_item.setBackground(QBrush(QColor(200, 255, 200)))
                elif last_status == 'Failed':
                    status_item.setBackground(QBrush(QColor(255, 200, 200)))

                exec_table.setItem(i, 6, status_item)

                # Error column: prefer persisted error message if any
                err_text = ''
                try:
                    if hasattr(
                            self,
                            '_last_preview_errors') and step_idx is not None:
                        err_text = self._last_preview_errors.get(step_idx, '')
                except Exception:
                    err_text = ''
                err_item = QTableWidgetItem(err_text)
                if err_text:
                    err_item.setToolTip(err_text)
                exec_table.setItem(i, 7, err_item)

                try:
                    cfg_text = json.dumps(s.get('config', {}),
                                          ensure_ascii=False)
                except Exception:
                    cfg_text = str(s.get('config', ''))
                exec_table.setItem(i, 8, QTableWidgetItem(cfg_text))

                # Stage column (pipeline mode only)
                if is_pipeline:
                    stage_val = s.get('stage', '')
                    exec_table.setItem(i, 9, QTableWidgetItem(str(stage_val)))

                # record mapping from step -> row for live updates
                try:
                    if step_idx is None:
                        step_idx = int(s.get('step'))
                    if step_idx is not None:
                        self._preview_step_map[step_idx] = i
                except Exception:
                    pass

                # Row shading by depth to enhance hierarchy perception
                if level % 2 == 1:
                    shade = QBrush(QColor(250, 250, 250))
                else:
                    shade = None
                if shade is not None:
                    for col in range(exec_table.columnCount()):
                        item = exec_table.item(i, col)
                        if item is not None:
                            item.setBackground(shade)
        except Exception:
            pass

        # Populate stage filter combobox with unique stage names (pipeline mode)
        if is_pipeline and stage_filter_combo is not None:
            try:
                stages_seen = []
                for r in range(exec_table.rowCount()):
                    item = exec_table.item(r, 9)
                    if item is not None:
                        s_val = item.text()
                        if s_val and s_val not in stages_seen:
                            stages_seen.append(s_val)
                            stage_filter_combo.addItem(s_val)
            except Exception:
                pass

        # Clear history button (clears persisted preview statuses/errors for this root)
        btn_clear = QPushButton("清空预览历史")

        def _clear_preview_history():
            try:
                # reset stored statuses/errors for current root
                if hasattr(self, '_last_preview_status'):
                    self._last_preview_status = {}
                if hasattr(self, '_last_preview_errors'):
                    self._last_preview_errors = {}
                # refresh table UI
                try:
                    for r in range(exec_table.rowCount()):
                        # reset status cell
                        status_item = exec_table.item(r, 6)
                        if status_item is None:
                            status_item = QTableWidgetItem('Planned')
                            exec_table.setItem(r, 6, status_item)
                        else:
                            status_item.setText('Planned')
                            status_item.setBackground(QBrush())
                            status_item.setToolTip('')
                        # reset error cell
                        err_item = exec_table.item(r, 7)
                        if err_item is None:
                            err_item = QTableWidgetItem('')
                            exec_table.setItem(r, 7, err_item)
                        else:
                            err_item.setText('')
                            err_item.setToolTip('')
                        # ensure row is visible
                        exec_table.setRowHidden(r, False)
                except Exception:
                    pass
            except Exception:
                pass

        btn_clear.clicked.connect(_clear_preview_history)
        layout.addWidget(btn_clear)

        btn_close = QPushButton("关闭")
        btn_close.clicked.connect(dialog.close)
        layout.addWidget(btn_close)

        # Keep a reference so the dialog isn't garbage-collected and make it non-modal
        try:
            dialog.setModal(False)
            dialog.setWindowModality(Qt.NonModal)
        except Exception:
            pass
        self._preview_dialog = dialog

        # When preview dialog closes, clean up preview mappings to avoid stale refs
        def _cleanup_preview():
            try:
                if hasattr(self, '_preview_step_map'):
                    del self._preview_step_map
                if hasattr(self, '_preview_exec_table'):
                    del self._preview_exec_table
                if hasattr(self, '_preview_running_step'):
                    del self._preview_running_step
            except Exception:
                pass

        try:
            dialog.finished.connect(_cleanup_preview)
        except Exception:
            pass

        # If requested, hide rows that are still Planned (i.e. only show executed steps)
        if show_only_executed:
            try:
                for r in range(exec_table.rowCount()):
                    item = exec_table.item(r, 6)
                    status = item.text() if item is not None else 'Planned'
                    if status == 'Planned':
                        exec_table.setRowHidden(r, True)
            except Exception:
                pass

        dialog.show()

    def _show_execution_status(self):
        """Open the merged execution-order view but show only executed steps."""
        try:
            self._show_preview(show_only_executed=True)
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法显示执行情况: {e}")

    def _gen_template(self):
        path, _ = QFileDialog.getSaveFileName(self, "保存模板", "config.yaml",
                                              "YAML (*.yaml);;JSON (*.json)")
        if path:
            generate_template(path)
            self._log(f"✅ 模板已生成: {path}")

    ##日志设置
    def _setup_logging(self):
        """设置日志区域"""
        #    self.log = QTextBrowser()
        self.log.setOpenExternalLinks(True)  # 可选：支持链接
        self.log.setReadOnly(True)
        self.log.setStyleSheet("""
            QTextEdit {
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 11pt;
                background: #f9f9f9;
                border: 1px solid #ddd;
                padding: 8px;
            }
        """)
        self.log.clear()
        self._log("系统已启动", level=LogLevel.INFO)

    def _log(self, text: str, level: LogLevel = LogLevel.INFO):
        """
        增强日志输出：支持级别、颜色、时间戳、自动换行
        """
        from html import escape

        timestamp = datetime.now().strftime("%H:%M:%S")
        style = LOG_STYLES[level]
        icon = style["icon"]
        color = style["color"]
        label = style["label"]

        # 转义并处理多行文本
        #    safe_text = escape(str(text)).strip()
        #    lines = safe_text.split('\n')
        lines = text.split('\n')
        html_lines = []
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            line = line.replace(" ", "&nbsp;")  # 保留空格格式
            if i == 0:
                # 第一行带完整信息
                formatted = (
                    f"<span style='color: #888; font-family: monospace;'>[{timestamp}]</span>&nbsp;"
                    f"<b style='color: white;'>{icon} {label}</b>&nbsp;"
                    f"<span style='color: {color};'>{line}</span>")
            else:
                # 后续行缩进
                formatted = f"&nbsp;&nbsp;&nbsp;&nbsp;{line}"
            html_lines.append(formatted)

        full_html = "<br>".join(html_lines)

        # 使用 insertHtml 避免 append 自动滚动问题
        cursor = self.log.textCursor()
        cursor.movePosition(cursor.End)
        cursor.insertHtml(full_html + "<br>")

        # 限制总行数
        document = self.log.document()
        while document.blockCount() > MAX_LOG_LINES:
            cursor = self.log.textCursor()
            cursor.movePosition(cursor.Start)
            cursor.select(cursor.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()  # 删除换行符

        # 自动滚动到底部
        self.log.ensureCursorVisible()
        # 只在 auto_scroll 开启时才滚到底
#        if self.auto_scroll:
#            self.log.ensureCursorVisible()
#            self.log.moveCursor(cursor.End)

#        # 转义 HTML 特殊字符，防止注入
#        safe_text = escape(str(text))
#
#        # 处理多行文本
#        lines = safe_text.split('\n')
#        for i, line in enumerate(lines):
#            if not line.strip():
#                continue
#            if i == 0:
#                # 第一行带图标和标签
#                formatted_line = f"<b>[{timestamp}] {icon} {label}</b> <span style='color:{color};'>{line}</span>"
#            else:
#                # 后续行缩进
#                formatted_line = f"&nbsp; &nbsp; &nbsp; &nbsp; {line}"
#            self.log.append(formatted_line)
#
#        # 自动滚动到底部
#        self.log.ensureCursorVisible()
#        self.log.moveCursor(self.log.textCursor().End)

# 方法

    def _clear_log(self):
        self.log.clear()
        self._log("日志已清空", level=LogLevel.INFO)


##打开配置文件

    def _open_config_editor(self):
        """打开图形化配置编辑器"""
        from qtpy.QtWidgets import QDialog, QFormLayout, QLineEdit, QDialogButtonBox

        dialog = QDialog(self)
        dialog.setWindowTitle("编辑配置")
        dialog.resize(400, 300)

        layout = QFormLayout()

        config = getattr(self, 'config', {})

        self.config_fields = {}

        for key in ['pre_process', 'post_process', 'processor']:
            line = QLineEdit(config.get(key, ""))
            self.config_fields[key] = line
            layout.addRow(key, line)

        # 包含 filters 等复杂字段可扩展为子对话框

        buttons = QDialogButtonBox(QDialogButtonBox.Ok
                                   | QDialogButtonBox.Cancel)
        buttons.accepted.connect(lambda: self._save_config_from_editor(dialog))
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        dialog.setLayout(layout)
        dialog.exec_()

    def _save_config_from_editor(self, dialog):
        new_config = {}
        for key, line in self.config_fields.items():
            value = line.text().strip()
            if value:
                new_config[key] = value
            else:
                new_config[key] = None  # 或跳过

        self.config = new_config
        self.config_textedit.setPlainText(str(new_config))
        self._log("✅ 配置已更新")
        dialog.accept()

    def _show_results(self, results: list):
        """将结果列表转为表格展示到已有的 QTableWidget 上"""
        if not results:
            self.results_table.setRowCount(0)
            return

        try:
            df = pd.DataFrame(results)
            df = df.fillna("")
        except Exception as e:
            QMessageBox.warning(self, "数据错误", f"无法解析结果数据: {e}")
            return

        # ✅ 只更新已有表格的内容，不再创建新对象
        self.results_table.clear()  # 清除旧内容（包括表头）
        self.results_table.setRowCount(len(df))
        self.results_table.setColumnCount(len(df.columns))
        self.results_table.setHorizontalHeaderLabels(df.columns)
        self.results_table.verticalHeader().setVisible(False)
        self.results_table.setSortingEnabled(True)
        #     self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        # ✅ 关键：允许用户拖动调节列宽
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)  # 可拖动
        header.setSectionsClickable(True)
        header.setSectionsMovable(True)  # 可选：允许拖动列顺序

        # 初始列宽自适应内容
        self.results_table.resizeColumnsToContents()

        # 填充数据
        for i, row in df.iterrows():
            for j, val in enumerate(row):
                item = QTableWidgetItem(str(val))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)  # 禁止编辑
                self.results_table.setItem(i, j, item)

    def _show_dataframe(self, df: pd.DataFrame):
        dialog = QDialog(self)
        dialog.setWindowTitle("DataFrame 预览")
        dialog.resize(900, 560)
        layout = QVBoxLayout(dialog)

        table = QTableWidget()
        table.setRowCount(len(df))
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels([str(c) for c in df.columns])
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        for i, (_, row) in enumerate(df.iterrows()):
            for j, val in enumerate(row):
                table.setItem(i, j, QTableWidgetItem(str(val)))
        table.resizeColumnsToContents()
        layout.addWidget(table)

        btn_close = QPushButton("关闭")
        btn_close.clicked.connect(dialog.close)
        layout.addWidget(btn_close)
        dialog.exec_()

    def _get_enabled_processors_from_table(self):
        """
        从 plugin_table 的“启用”列读取用户勾选状态，
        返回 (pre_dict, main_dict, post_dict)
        """
        pre_enabled = {}
        main_enabled = {}
        post_enabled = {}

        all_processors = {**PRE_PROCESSORS, **PROCESSORS, **POST_PROCESSORS}

        for row in range(self.plugin_table.rowCount()):
            cb_item = self.plugin_table.item(row, 1)  # 启用列

            if not cb_item:
                continue
            func_name = cb_item.data(Qt.UserRole)
            if not func_name or func_name not in all_processors:
                continue
            if cb_item.checkState(
            ) == Qt.Checked and func_name in all_processors:
                func = all_processors[func_name]
                ptype = getattr(func, 'processor_kind', 'file')
                if ptype == 'pre':
                    pre_enabled[func_name] = func
                elif ptype == 'post':
                    post_enabled[func_name] = func
                else:
                    main_enabled[func_name] = func

        return pre_enabled, main_enabled, post_enabled

    ##刷新可用的处理函数表格
    def _refresh_plugin_table(self):

        # 👇 保存当前勾选状态
        current_state = {}
        for row in range(self.plugin_table.rowCount()):
            cb_item = self.plugin_table.item(row, 1)
            name = cb_item.data(Qt.UserRole)
            current_state[name] = (cb_item.checkState() == Qt.Checked)

        # 清空表格
        self.plugin_table.setRowCount(0)
        self.plugin_table.clearContents()
        all_processors = PRE_PROCESSORS | PROCESSORS | POST_PROCESSORS
        # 添加到表格
        try:
            # keep deterministic order (kind, priority, name) for stable UI
            items = sorted(all_processors.items(),
                           key=lambda kv:
                           (getattr(kv[1], 'processor_kind', ''), -getattr(
                               kv[1], 'processor_priority', 0), kv[0]))
        except Exception:
            items = list(all_processors.items())

        for name, func in items:
            row = self.plugin_table.rowCount()
            self.plugin_table.insertRow(row)
            # 文件名
            self.plugin_table.setItem(
                row, 0, QTableWidgetItem(str(func.processor_source)))

            # metadata must be read up-front (was previously used before declaration)
            meta = getattr(func, 'metadata', {})
            # 👇 恢复勾选状态，若无则默认 False
            cb = QTableWidgetItem()
            cb.setFlags(cb.flags() | Qt.ItemIsUserCheckable)  # 必须设置才可勾选！
            default_enabled = meta.get("enabled_by_default", True)  # 默认启用
            current_check = current_state.get(name, default_enabled)
            cb.setCheckState(Qt.Checked if current_check else Qt.Unchecked)
            cb.setData(Qt.UserRole, func.processor_name)
            self.plugin_table.setItem(row, 1, cb)

            # 启用复选框
            #            cb = QTableWidgetItem()
            #            cb.setCheckState(Qt.Checked)
            #            cb.setData(Qt.UserRole, func.processor_name)  # 存名字
            #            self.plugin_table.setItem(row, 1, cb)
            # 处理器名
            self.plugin_table.setItem(row, 2,
                                      QTableWidgetItem(func.processor_name))
            # 🔔 类型（新增）
            ptype = getattr(func, 'processor_kind', 'file')
            type_item = QTableWidgetItem(ptype.upper())
            if ptype == "pre":
                type_item.setForeground(Qt.blue)
            elif ptype == "post":
                type_item.setForeground(Qt.magenta)
            else:
                type_item.setForeground(Qt.darkGreen)
            self.plugin_table.setItem(row, 3, type_item)
            # 在 _load_plugins() 中，插入表格的循环里
            priority = getattr(func, 'processor_priority', 50)  # 默认优先级 50
            priority_item = QTableWidgetItem(str(priority))
            priority_item.setTextAlignment(Qt.AlignCenter)
            self.plugin_table.setItem(row, 4, priority_item)  # 注意：列索引变了！
            # 元数据
            meta = getattr(func, 'metadata', {})
            self.plugin_table.setItem(
                row, 5, QTableWidgetItem(meta.get("author", "未知")))
            self.plugin_table.setItem(
                row, 6, QTableWidgetItem(meta.get("version", "-")))

        # 重新应用当前排序（如果用户之前点击过列头）
        try:
            header = self.plugin_table.horizontalHeader()
            current_col = header.sortIndicatorSection()
            current_order = header.sortIndicatorOrder()
            self.plugin_table.sortItems(current_col, current_order)
        except Exception:
            pass

    def _on_plugin_header_clicked(self, logicalIndex: int):
        """点击插件表头时切换升/降序并按列排序。"""
        header = self.plugin_table.horizontalHeader()
        # 切换排序方向
        current_order = header.sortIndicatorOrder()
        new_order = Qt.DescendingOrder if current_order == Qt.AscendingOrder else Qt.AscendingOrder
        header.setSortIndicator(logicalIndex, new_order)
        self.plugin_table.sortItems(logicalIndex, new_order)

    def _on_plugin_selected(self, row, col):
        cb_item = self.plugin_table.item(row, 1)
        #       if not hasattr(cb_item, 'plugin_func'):
        #           return

        func_name = cb_item.data(Qt.UserRole)
        all_processsors = PRE_PROCESSORS | PROCESSORS | POST_PROCESSORS
        func = all_processsors.get(func_name)
        if not func:
            return
        meta = getattr(func, 'metadata', {})

        def safe_str(value, default=""):
            return html.escape(str(value)) if value is not None else default

        name = safe_str(meta.get('name'), func.processor_name)
        processor_name = safe_str(func.processor_name)
        author = safe_str(meta.get('author'), "未知")
        version = safe_str(meta.get('version'), "N/A")
        description = safe_str(meta.get('description'), "无")
        supported_types = ", ".join(meta.get('supported_types', [])) or "无"
        tags = ", ".join(meta.get('tags', [])) or "无"
        priority = getattr(func, 'processor_priority', 50)
        ptype = getattr(func, 'processor_kind', 'file').upper()

        # 🔔 加入类型
        doc = (
            f"<b>名称:</b> {name}<br>\n"
            f"<b>处理器:</b> {processor_name}<br>\n"
            f"<b>类型:</b> <span style='color: {'blue' if ptype=='PRE' else 'magenta' if ptype=='POST' else 'green'};'>{ptype}</span><br>\n"
            f"<b>优先级:</b> <b>{priority}</b><br>\n"  # 👈 新增
            f"<b>作者:</b> {author}<br>\n"
            f"<b>版本:</b> {version}<br>\n"
            f"<b>描述:</b> {description}<br>\n"
            f"<b>支持类型:</b> {safe_str(supported_types)}<br>\n"
            f"<b>标签:</b> {safe_str(tags)}")

        self.plugin_info.setHtml(doc)

    def _load_plugins(self):
        from pathlib import Path
        from importlib import reload
        from importlib.util import spec_from_file_location, module_from_spec
        import sys

        # 清空所有已注册的外部插件（保留内置？）
        # for name in list(PRE_PROCESSORS.keys()):
        #     if name not in BUILTIN_PRE:  # 需定义内置列表
        #         _unregister_pre(name)

        plugin_path = self.plugins_line.text().strip()
        if not plugin_path:
            self._log("⚠️ 请先选择插件目录")
            return

        plugin_dir = Path(plugin_path)
        if not plugin_dir.exists():
            self._log(f"❌ 插件目录不存在: {plugin_dir}")
            return
        if not plugin_dir.is_dir():
            self._log(f"❌ 不是有效目录: {plugin_dir}")
            return

        self._log(f"🔍 扫描插件目录: <b>{plugin_dir.resolve()}</b>")

        # 清空旧表
        #    self.plugin_table.setRowCount(0)
        #    self.plugin_table.clearContents()

        loaded = 0
        failed = 0

        # 存储插件信息（可选）
        self.loaded_plugins = {}

        for pyfile in plugin_dir.glob("*.py"):
            if pyfile.name == "__init__.py":
                continue
            try:
                module_name = f"plugin_ext_{pyfile.stem}"
                #   spec = spec_from_file_location(module_name, pyfile)
                # 1. 如果已存在，从 sys.modules 中移除
                if module_name in sys.modules:
                    print(f"🗑️ 移除旧模块: {module_name}")
                    del sys.modules[module_name]

                # 2. 正常导入流程
                spec = spec_from_file_location(module_name, pyfile)
                if spec is None:
                    raise ImportError(f"无法加载模块: {pyfile}")

                module = module_from_spec(spec)
                sys.modules[module_name] = module
                print(f"✅ 重新导入模块: {module_name}")
                spec.loader.exec_module(module)

                # 扫描模块中所有带 .processor_name 的函数
                plugin_funcs = []
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if callable(attr) and hasattr(attr, 'processor_name'):
                        handler_name = attr.processor_name  ##函数名
                        if attr.reload_info:
                            self._log(f"🔄 {attr.reload_info}")
                        plugin_funcs.append(attr)
                if not plugin_funcs:
                    self._log(f"🟡 {pyfile.name}：未发现处理器")
                    continue

                # 记录已加载插件（用于 UI 管理）
                self.loaded_plugins[pyfile.name] = {
                    'module': module,
                    'functions': plugin_funcs
                }

                self._log(
                    f"✅ 成功加载插件: <b>{pyfile.name}</b> ({len(plugin_funcs)} 个处理器)"
                )
                loaded += 1

            except Exception as e:
                self._log(
                    f"❌ 加载失败 {pyfile.name}: <span style='color:red;'>{e}</span>"
                )
                failed += 1

        # ✅ 最后打印 PROCESSORS 内容用于调试
        self._log(f"📊 插件加载完成: <b>{loaded}</b> 成功, <b>{failed}</b> 失败")
        self._log(f"📊 插件加载完成: <b>{loaded}</b> 成功, <b>{failed}</b> 失败")
        self._log(f"🔄 可用处理器: {list(PROCESSORS.keys())}")
        self._log(f"🔄 预处理器: {list(PRE_PROCESSORS.keys())}")
        self._log(f"🔄 后处理器: {list(POST_PROCESSORS.keys())}")

        self._refresh_plugin_table()

        # 🔍 调试：打印类型
        print("\n🔍 PROCESSORS 调试:")
        for k, v in PROCESSORS.items():
            print(f"  {k} -> type={type(v).__name__}, callable={callable(v)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BatchProcessorGUI()  #BatchProcessorGUI()
    window.show()
    sys.exit(app.exec())
