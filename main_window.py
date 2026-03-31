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
from widgets.batch_thread import BatchWorker
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

        # 右侧：配置编辑区
        config_widget = QWidget()
        config_layout = QVBoxLayout()

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

        # 按钮
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
        config_layout.addWidget(config_group)
        config_widget.setLayout(config_layout)
        upper_splitter.addWidget(config_widget)

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

            self._log(f"✅ 配置已保存: {self.config_path}")
            # 可选：重新加载以刷新 UI（如果 save_config 没有副作用）
            # self._load_config()  # 如果你需要刷新显示

        except Exception as e:
            self._log(f"❌ 保存失败: {e}")

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
        is_pipeline = getattr(self, '_is_pipeline_mode', False)
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
        self.table = QTableWidget()
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns)

        for i, row in df.iterrows():
            for j, val in enumerate(row):
                self.table.setItem(i, j, QTableWidgetItem(str(val)))

        self.layout.addWidget(self.table)

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
