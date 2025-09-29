# processors.py 或直接写在 core.py 里

from pathlib import Path
import shutil
from typing import Any
from decorators.processor import processor, pre_processor, post_processor
SCRIPT_DIR = Path(__file__).parent.resolve()   ##此脚本的路径

##前处理
@pre_processor(name="setup_env", source = SCRIPT_DIR)
def setup_env(context, **kwargs):
    path = kwargs.get("path", "./backup")

    print("🚀 开始环境准备...")
    
#    context.set_data("root", ".")  # 可以记录根目录
#    Path(path).mkdir(exist_ok=True)
    return "env ready"

##后处理
@post_processor(name="generate_report", source = SCRIPT_DIR)
def generate_report(context, **kwargs):
    print(f"📊 处理完成，共 {len(context.results)} 项")
    return "report generated"


##处理函数
@processor(name="add_prefix", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "添加前缀",
    "author": "guancc",
    "version": "1.0",
    "description": "给文件名添加前缀",
    "supported_types": [""],
    "tags": [""]
})
def add_prefix(file_path: Path, context, **kwargs):  #prefix: str = "【文件】"
    """
    给文本文件名添加前缀
    ✅ 支持 context 和 config 参数
    """
    prefix = kwargs.get("prefix", "【文件】")


    if not file_path.is_file():
        return {"skipped": "not a file"}

    new_name = f"{prefix}{file_path.name}"
    new_path = file_path.parent / new_name

    try:
        file_path.rename(new_path)
        print(f"  ✅ 重命名: {file_path.name} → {new_name}")
        return {
            "action": "rename",
            "from": str(file_path),
            "to": str(new_path)
        }
    except Exception as e:
        print(f"  ❌ 重命名失败 {file_path}: {e}")
        return {
            "error": str(e),
            "path": str(file_path)
        }

