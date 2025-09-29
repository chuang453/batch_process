# gui.py
from qtpy.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,QGroupBox,QProgressBar,QSplitter,
    QPushButton, QLineEdit, QLabel, QFileDialog, QTextEdit,QTableWidget, QTableWidgetItem,
    QTabWidget,QHeaderView,QMessageBox, QTextBrowser, QDialog
)
from qtpy.QtGui import QFont, QColor
from qtpy.QtCore import QThread
import html
import pandas as pd
from qtpy.QtCore import Qt
import sys
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
from config.loader import load_config, generate_template   #AVAILABLE_PROCESSORS,
from decorators.processor import ProcessingContext,PROCESSORS,PRE_PROCESSORS,POST_PROCESSORS,get_all_processors,_unregister_processor,_unregister_pre,_unregister_post
from processors import *       ##导入内置处理函数
from qtpy.QtGui import QTextCharFormat, QSyntaxHighlighter

from widgets.widgets import FileStructureWidget
from widgets.console import PythonConsoleWidget
from widgets.batch_thread import BatchWorker
from datetime import datetime
from enum import Enum

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
            self.setFormat(comment_start, len(text) - comment_start, self.formats["comment"])

        # 匹配键（以冒号结尾）
        import re
        for match in re.finditer(r"^\s*([a-zA-Z0-9_\-]+)(\s*:)", text):
            self.setFormat(match.start(1), len(match.group(1)), self.formats["key"])
            # 冒号后的内容作为值
            if match.end(2) < len(text):
                self.setFormat(match.end(2), len(text) - match.end(2), self.formats["value"])

        # 布尔值/数字
        for match in re.finditer(r"\b(true|false|null|[\d\.]+)\b", text):
            self.setFormat(match.start(), len(match.group()), self.formats["value"])


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
        
        self.processor = BatchProcessor()    ##批处理器
        self.context = ProcessingContext()   ##背景数据库

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
        self.btn_cancel.setEnabled(False)   ##初始禁用
                
        btn_metadata = QPushButton("ℹ️ 显示执行情况(metadata)")
        btn_metadata.setStyleSheet("font-weight: bold")
        btn_metadata.clicked.connect(self._show_metadata_info)
        
        
        for btn in [ self.btn_run, self.btn_cancel, btn_metadata]:  #btn_load btn_refresh_plugin, btn_plugins,
            btn_layout.addWidget(btn)
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
        self.plugin_info.setStyleSheet("QTextEdit { background: #f4f8f4; border: none; }")
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
        locals_dict = {'batch_processor': self.processor,
                       'context': self.context,
                       'config_path': self.config_path,
                       'root_path': self.root_path，
                       'pre_processors': PRE_PROCESSORS,
                       'processors': PROCESSORS,
                       'post_processors': POST_PROCESSORS}
        self.console = PythonConsoleWidget( parent=self, locals_dict = locals_dict) 
        tab_widget.addTab(self.console,  '💻 控制台')

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
                subline-offset: -6px;
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

            # 格式化为 YAML 字符串显示
            yaml_str = format_config_yaml(self.config)
            self.config_textedit.setPlainText(yaml_str)

            self._log(f"✅ 配置加载成功: {list(self.config.keys())}")

        except Exception as e:
            self._log(f"❌ 加载失败: {e}")

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
        config_ss = pprint.pformat(self.config, indent = 2, width = 40)
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
        if not hasattr(self, 'config'):
            self._load_config()
        if not hasattr(self, 'config'):
            return
        if not self.root_path:
            self.root_path = self.root_line.text().strip()
        if not self.root_path:
            self._log("请指定目标目录")
            return

        # ✅ 禁用按钮
        self.btn_run.setEnabled(False)
        self.btn_cancel.setEnabled(True)  # 如果有取消按钮

        self.processor.set_config(self.config)
        self._log(f"✅ 批处理器构建完毕!")
        processor = self.processor 

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
    
    def _on_worker_finished(self, context):
        self._log("✅ 批处理完成！")
        self.progress_bar.setFormat("完成")
        self._show_results(context.results)
        self.btn_run.setEnabled(True)
        self.btn_cancel.setEnabled(False)



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

#        metadata_ss = pprint.pformat(metadata, indent = 2, width = 40)
#        metadata_ss = 'metadata:\n' + metadata_ss
#        self._log(metadata_ss, level=LogLevel.INFO)

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
                    f"<span style='color: {color};'>{line}</span>"
                )
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


##    def _show_results(self, results: list):
##        """将结果列表转为表格展示"""
##        if not results:
##            if hasattr(self, 'results_table') and self.results_table is not None:
##                self.results_table.setRowCount(0)
##            return
##
##        # 转换为 DataFrame
##        try:
##            df = pd.DataFrame(results)
##            df = df.fillna("")
##        except Exception as e:
##            QMessageBox.warning(self, "数据错误", f"无法解析结果数据: {e}")
##            return
##
##        # 确保有主布局
##        if not hasattr(self, 'main_layout'):
##            return
##
##        # 移除旧表格
##        if hasattr(self, 'results_table') and self.results_table is not None:
##            self.main_layout.removeWidget(self.results_table)
##            self.results_table.deleteLater()
##            self.results_table = None
##
##        # 创建新表格
##        self.results_table = QTableWidget()
##        self.results_table.setRowCount(len(df))
##        self.results_table.setColumnCount(len(df.columns))
##        self.results_table.setHorizontalHeaderLabels(df.columns)
##        self.results_table.verticalHeader().setVisible(False)
##        self.results_table.setSortingEnabled(True)
##        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
##
##        # 填充数据
##        for i, row in df.iterrows():
##            for j, val in enumerate(row):
##                item = QTableWidgetItem(str(val))
##                item.setFlags(item.flags() & ~Qt.ItemIsEditable)  # 只读
##                self.results_table.setItem(i, j, item)
##
##        # 添加标签（只添加一次）
##        if not hasattr(self, 'result_label'):
##            self.result_label = QLabel("📊 处理结果:")
##            self.main_layout.addWidget(self.result_label)
##        self.result_label.setVisible(True)
##
##        self.main_layout.addWidget(self.results_table)
#        """将结果列表转为表格展示"""
#        if not results:
#            return
#
#        df = pd.DataFrame(results)
#        df = df.fillna("")
#
#        # 清除旧表格（可选）
#        if hasattr(self, 'results_table'):
#            self.layout().removeWidget(self.results_table)
#            self.results_table.deleteLater()
#
#        # 创建新表格
#        self.results_table = QTableWidget()
#        self.results_table.setRowCount(len(df))
#        self.results_table.setColumnCount(len(df.columns))
#        self.results_table.setHorizontalHeaderLabels(df.columns)
#        self.results_table.verticalHeader().setVisible(False)
#
#        for i, row in df.iterrows():
#            for j, val in enumerate(row):
#                self.results_table.setItem(i, j, QTableWidgetItem(str(val)))
#
#        # 添加到布局
#        self.layout().addWidget(QLabel("📊 处理结果:"))
#        self.layout().addWidget(self.results_table)

    def _show_dataframe(self, df: pd.DataFrame):
        self.table = QTableWidget()
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns)

        for i, row in df.iterrows():
            for j, val in enumerate(row):
                self.table.setItem(i, j, QTableWidgetItem(str(val)))

        self.layout.addWidget(self.table)

    ##刷新可用的处理函数表格
    def _refresh_plugin_table(self):
        self.plugin_table.setRowCount(0)
        self.plugin_table.clearContents()
        all_processors = PRE_PROCESSORS | PROCESSORS | POST_PROCESSORS
        # 添加到表格
        for name, func in all_processors.items():
            row = self.plugin_table.rowCount()
            self.plugin_table.insertRow(row)
            # 文件名
            self.plugin_table.setItem(
                row, 0, QTableWidgetItem(str(func.processor_source)))
            # 启用复选框
            cb = QTableWidgetItem()
            cb.setCheckState(Qt.Checked)
            cb.setData(Qt.UserRole, func.processor_name)  # 存名字
            self.plugin_table.setItem(row, 1, cb)            
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
    window = BatchProcessorGUI()   #BatchProcessorGUI()
    window.show()
    sys.exit(app.exec())
