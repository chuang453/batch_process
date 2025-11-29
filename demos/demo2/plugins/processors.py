# processors/my_processors.py
from decorators.processor import processor
from pathlib import Path

@processor(name="mark_enter", priority=80, metadata={"description": "进入路径前做标记"})
def mark_enter(path: Path, context, **cfg):
    context.setdefault_data(["trace", str(path)], []).append("ENTER")
    return {"path": str(path), "event": "enter"}

@processor(name="scan_file", priority=60, metadata={"description": "统计行数"})
def scan_file(path: Path, context, **cfg):
    if path.is_file():
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
            return {"file": str(path), "lines": len(lines)}
        except Exception as e:
            return {"file": str(path), "error": str(e)}
    return {"path": str(path), "skipped": True}

@processor(name="mark_exit", priority=50, metadata={"description": "离开路径后做标记"})
def mark_exit(path: Path, context, **cfg):
    context.setdefault_data(["trace", str(path)], []).append("EXIT")
    return {"path": str(path), "event": "exit"}

@processor(name="summarize_dir", priority=40, metadata={"description": "目录后统计该目录下成功扫描的文件数"})
def summarize_dir(path: Path, context, **cfg):
    if not path.is_dir():
        return {"path": str(path), "skipped": True}
    count = 0
    for r in context.results:
        if r.get("processor") == "scan_file" and r.get("phase") in ("pre","post"):
            # 统计在此目录子树下的文件
            if r.get("path", "").startswith(str(path)):
                count += 1
    return {"dir": str(path), "scanned_files_in_subtree": count}