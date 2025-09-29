#提供pyhon控制台
'''
1 默认情况下,此控制台自动继承主程序启动conda环境中的python包。
  还可以自定义python的conda环境,通过conda_env_path指定 Conda 环境的 Python 路径
  故若想安装其它python环境包，最好创建专门的conda环境。
2 这个python控制台自带app的一些变量。

'''



from qtpy import QtCore
import sys
import traceback
from qtpy.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QPushButton, QSplitter, QPlainTextEdit)
from qtpy.QtCore import Qt, QObject, Signal, QThread
from qtconsole.rich_jupyter_widget import RichJupyterWidget
from qtconsole.inprocess import QtInProcessKernelManager
from traitlets.config.loader import Config
from IPython.core.interactiveshell import InteractiveShell



#class SafeConsoleKernelManager(QtInProcessKernelManager):
#    """修复closed()问题的内核管理器"""
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#        self._iopub_channel = self._create_dummy_channel()
#        self._shell_channel = self._create_dummy_channel()
#        self._stdin_channel = self._create_dummy_channel()
#        
#    def _create_dummy_channel(self):
#        """创建虚拟通道对象"""
#        class DummyChannel:
#            def is_alive(self):
#                return True
#            def close(self):
#                pass
#        return DummyChannel()

from qtconsole.inprocess import QtInProcessKernelManager
from qtconsole.inprocess import QtInProcessKernelClient

class PatchedInProcessKernelClient(QtInProcessKernelClient):
    """修复 closed() 问题的客户端：打补丁到通道"""
    
    def _make_inprocess_client(self, channel):
        """包装原始通道，添加 missing 方法"""
        # 使用组合而非继承
        original = super()._make_inprocess_client(channel)
        
        # 动态添加 missing 方法
        def closed():
            return False
        
        def is_closed():
            return False
        
        # 检查并打补丁
        if not hasattr(original, 'closed'):
            original.closed = closed
        if not hasattr(original, 'is_closed'):
            original.is_closed = is_closed
        
        return original

class SafeConsoleKernelManager(QtInProcessKernelManager):
    """安全的内核管理器：返回打过补丁的客户端"""
    
    def client(self, *args, **kwargs):
        """返回修复后的客户端"""
        client = super().client(*args, **kwargs)
        
        # 手动对所有通道打补丁（关键！）
        for channel_name in ('shell_channel', 'iopub_channel', 'stdin_channel', 'hb_channel'):
            channel = getattr(client, channel_name, None)
            if channel is None:
                continue

            # 动态添加 missing 方法
            if not hasattr(channel, 'closed'):
                channel.closed = lambda: False
            if not hasattr(channel, 'is_closed'):
                channel.is_closed = lambda: False

        return client



class PythonConsoleWidget(RichJupyterWidget):
    """安全的嵌入式Python控制台"""
    def __init__(self, parent=None, locals_dict=None, conda_env_path=None):
        super().__init__(parent)

        ##指定conda环境的python路径
        if conda_env_path:
            sys.executable = conda_env_path  # 例如: "/home/user/anaconda3/envs/your_env/bin/python"        


        self._locals = locals_dict or {}
        
        # 配置控制台
        c = Config()
        c.InteractiveShell.autocall = 2
        c.InteractiveShell.colors = 'Linux'
        c.ConsoleWidget.include_other_output = True
        self.config = c
        
        # 初始化内核
        self._init_kernel()
        
        # 设置样式
        self.setStyleSheet("""
            QPlainTextEdit {
                font-family: Consolas, Courier New, monospace;
                font-size: 12px;
                background-color: #272822;
                color: #f8f8f2;
            }
        """)
    
    def _init_kernel(self):
        """初始化IPython内核"""
        # 使用修复后的内核管理器
        self.kernel_manager = SafeConsoleKernelManager()
        self.kernel_manager.start_kernel()
        self.kernel = self.kernel_manager.kernel
        
        # 设置命名空间
        self.kernel.shell.push(self._locals)
        
        # 创建客户端
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.namespace = self._locals
        
        # 启动通信
        self.kernel_client.start_channels()
        
        # 替换默认的异常处理
        self.kernel.shell.set_custom_exc((Exception,), self._handle_exception)
    
    def _handle_exception(self, etype, evalue, tb, tb_offset=None):
        """自定义异常处理"""
        tb_list = traceback.format_exception(etype, evalue, tb)
        tb_str = ''.join(tb_list)
        self._append_error(tb_str)
        return tb_str
    
    def _append_error(self, error_msg):
        """安全地追加错误信息"""
        try:
            self._append_custom_output("[Error] " + error_msg, error=True)
        except Exception as e:
            print(f"Failed to append error: {str(e)}")
    
    def _append_custom_output(self, text, error=False):
        """追加自定义输出"""
        if error:
            self.append_stream(f"\n\033[91m{text}\033[0m\n")  # 红色错误信息
        else:
            self.append_stream(f"\n{text}\n")
    
    def add_variable(self, name, value):
        """向控制台命名空间添加变量"""
        self._locals[name] = value
        if hasattr(self, 'kernel_manager') and self.kernel_manager.kernel:
            self.kernel_manager.kernel.shell.push({name: value})
    
    def clear_namespace(self):
        """清空命名空间"""
        self._locals.clear()
        if hasattr(self, 'kernel_manager') and self.kernel_manager.kernel:
            self.kernel_manager.kernel.shell.reset()
            self.kernel_manager.kernel.shell.push(self._locals)