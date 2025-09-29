# processors/__init__.py
"""
导入所有处理器模块，确保 @processor 装饰器注册所有函数
"""

from . import file_ops
from . import custom

# 从装饰器中导入注册表
#from decorators.processor import PROCESSORS

#__all__ = ['PROCESSORS',  'log_parser', 'file_ops', 'custom']