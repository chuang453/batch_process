# 文件
from pathlib import Path
from core.engine import ProcessingContext
from decorators.processor import processor,pre_processor,post_processor
# plugins/example.py

import os
from pathlib import Path
from core import pre_processor, post_processor, ProcessingContext

@pre_processor("setup_env")
def setup_env(context: ProcessingContext):
    """初始化：创建输出目录"""
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    context.shared["output_dir"] = str(output_dir)
    context.update_metadata(init_time="2025-09-17")

    print(f"📁 初始化完成: {output_dir}")
    return {
        "status": "success",
        "action": "setup_dirs",
        "output_dir": str(output_dir),
        "continue": True  # 对 pre 没意义，但保持结构一致
    }

@post_processor("generate_report")
def generate_report(context: ProcessingContext):
    """生成报告"""
    count = len([r for r in context.results if isinstance(r, dict) and r.get("file")])
    report = f"""
=== 批处理报告 ===
处理文件数: {count}
输出目录: {context.shared.get('output_dir', 'N/A')}
开始时间: {context.metadata.get('init_time', 'N/A')}
"""
    Path("output/report.txt").write_text(report.strip(), encoding='utf-8')
    print("📄 报告已生成")
    return {"report": "saved"}
    
    
@processor("convert_to_upper")
def convert_to_upper(path: Path, context):
    """转为大写"""
    content = path.read_text(encoding='utf-8')
    path.write_text(content.upper(), encoding='utf-8')
    return {"action": "upper", "status": "success"}

@processor("convert_to_lower")
def convert_to_lower(path: Path, context):
    """转为小写"""
    content = path.read_text(encoding='utf-8')
    path.write_text(content.lower(), encoding='utf-8')
    return {"action": "lower", "status": "success"}

@processor("remove_blank_lines")
def remove_blank_lines(path: Path, context):
    """删除空行"""
    lines = path.read_text(encoding='utf-8').splitlines()
    non_empty = [line for line in lines if line.strip()]
    path.write_text('\n'.join(non_empty), encoding='utf-8')
    return {"action": "remove_blank", "status": "success"}

# 可选：为每个函数添加元数据
convert_to_upper.metadata = {
    "name": "转为大写",
    "author": "你",
    "version": "1.0",
    "description": "将文本全部转为大写字母"
}

convert_to_lower.metadata = {
    "name": "转为小写",
    "author": "你",
    "version": "1.0",
    "description": "将文本全部转为小写字母"
}

remove_blank_lines.metadata = {
    "name": "删除空行",
    "author": "你",
    "version": "1.0",
    "description": "删除文件中的空行"
}