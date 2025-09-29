import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QTextEdit, QVBoxLayout, QWidget

class WriteStream:
    def __init__(self, text_edit):
        self.text_edit = text_edit

    def write(self, text):
        if text.rstrip():  # 避免空行或纯空白刷屏
            self.text_edit.append(text.rstrip())
            self.text_edit.ensureCursorVisible()  # 自动滚动到底部

    def flush(self):
        pass

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Print 重定向到 QTextEdit")
        self.resize(600, 400)

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)

        layout = QVBoxLayout()
        layout.addWidget(self.text_edit)
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        # 重定向 stdout
        self.write_stream = WriteStream(self.text_edit)
        sys.stdout = self.write_stream

        # 测试输出
        print("✅ 程序已启动")
        print("💡 这是通过 print 输出的信息")
        print("📍 所有 print 都会出现在这里")

# 运行应用
app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec_())