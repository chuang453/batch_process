# plugins/__init__.py
import importlib
import pkgutil
import os
from pathlib import Path

def load_plugins():
    """动态加载 plugins/ 目录下的所有模块"""
    plugin_dir = Path(__file__).parent
    print(f"🔍 扫描插件目录: {plugin_dir}")

    # 方法1：遍历所有 .py 文件（除了 __init__.py）
    for finder, name, ispkg in pkgutil.iter_modules([str(plugin_dir)]):
        if name == "__init__":
            continue
        try:
            module = importlib.import_module(f"plugins.{name}")
            print(f"✅ 加载插件: {name}")
        except Exception as e:
            print(f"❌ 加载插件失败 {name}: {e}")

    # 方法2：支持从环境变量加载外部插件
    extra_plugins = os.getenv("EXTRA_PLUGINS")
    if extra_plugins:
        for path in extra_plugins.split(os.pathsep):
            if Path(path).exists():
                importlib.machinery.SourceFileLoader(
                    f"external_{Path(path).stem}", path
                ).load_module()

# 启动时自动执行
load_plugins()