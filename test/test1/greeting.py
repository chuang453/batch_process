# D:/my_plugins/greeting.py
from decorators.processor import processor

@processor("say_hello")
def say_hello(path, context):
    return {
        "file": str(path),
        "action": "greet",
        "message": f"👋 你好！正在处理文件: {path.name}",
        "status": "success"
    }

# 👇 插件元数据（文档）
say_hello.metadata = {
    "name": "Say Hello",
    "author": "你",
    "version": "1.0",
    "description": "对每个文件打印一条问候语",
    "tags": ["demo", "greeting"],
    "supported_types": ["*.txt", "*.log"]
}