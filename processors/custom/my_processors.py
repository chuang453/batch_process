# processors/custom/my_processors.py
from pathlib import Path
from core.engine import ProcessingContext

def count_code_lines(file: Path, context: ProcessingContext) -> Dict[str, Any]:
    """统计代码文件的有效行数（忽略空行和注释）"""
    try:
        lines = file.read_text().splitlines()
        code_lines = 0
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith('#') and not stripped.startswith('//'):
                code_lines += 1
        
        return {
            "file": str(file),
            "code_lines": code_lines,
            "processor": "count_code_lines",
            "status": "success"
        }
    except Exception as e:
        return {
            "file": str(file),
            "processor": "count_code_lines",
            "status": "error",
            "error": str(e)
        }