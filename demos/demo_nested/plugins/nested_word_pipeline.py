from pathlib import Path
from typing import List, Dict, Any
from decorators.processor import processor
from utils.pipeline import get_bucket, append_numbers, record_result, set_output, get_output
from utils.adapters.docx_helpers import get_or_create_doc
from utils.adapters.plot_helpers import save_plot_png_values
from utils.io_helpers import safe_read_text, safe_read_json, csv_values

ALLOWED_SUFFIXES = {".csv", ".txt", ".json"}

@processor(name="enter_folder_word", priority=80, type_hint="dir", metadata={"desc": "进入目录时登记标题"})
def enter_folder_word(path: Path, context, doc_name: str = "nested_report.docx"):
    if not path.is_dir():
        return
    # 创建或获取文档路径（根目录统一）
    root = context.root_path or path
    doc_path = get_output(context, "doc_path")
    if not doc_path:
        doc_path = str(root / doc_name)
        set_output(context, "doc_path", doc_path)
    doc, resolved = get_or_create_doc(Path(doc_path))
    level = len(path.relative_to(root).parts) + 1 if path != root else 1
    doc.add_heading(f"Folder: {path.name}", level=level if level <= 4 else 4)
    doc.save(str(resolved))
    # 初始化每目录的数据桶
    folder_rows = get_bucket(context, "folder_rows")
    folder_values = get_bucket(context, "folder_values")
    folder_key = str(path)
    folder_rows.setdefault(folder_key, [])
    folder_values.setdefault(folder_key, [])
    record_result(context, "ok", "enter folder", folder=folder_key)
    return {"folder": folder_key, "action": "entered"}


def _extract_values(path: Path) -> List[float]:
    if path.suffix.lower() == ".csv":
        return csv_values(path)
    elif path.suffix.lower() == ".json":
        data = safe_read_json(path)
        if isinstance(data, dict):
            vals = data.get("values", [])
            return [float(v) for v in vals if isinstance(v, (int, float))]
        return []
    else:
        text = safe_read_text(path)
        nums = []
        for token in text.split():
            try:
                nums.append(float(token))
            except ValueError:
                continue
        return nums

@processor(name="process_data_file", priority=60, type_hint="file", metadata={"desc": "提取数据并登记"})
def process_data_file(path: Path, context):
    if not path.is_file() or path.suffix.lower() not in ALLOWED_SUFFIXES:
        return
    # 判断是否叶子目录（可选：若父目录还有子目录则跳过）
    parent = path.parent
    if any(p.is_dir() for p in parent.iterdir()):
        # 若想处理非叶目录，可移除此判断
        pass
    values = _extract_values(path)
    stats = {
        "file": str(path),
        "count": len(values),
        "sum": sum(values) if values else 0.0,
        "mean": (sum(values) / len(values)) if values else 0.0
    }
    # 全局记录
    records = get_bucket(context, "global_records")
    records.setdefault("rows", []).append(stats)
    # 目录级记录
    folder_rows = get_bucket(context, "folder_rows")
    folder_values = get_bucket(context, "folder_values")
    folder_key = str(parent)
    folder_rows.setdefault(folder_key, []).append(stats)
    folder_values.setdefault(folder_key, []).extend(values)
    record_result(context, "ok", "file processed", file=str(path), count=len(values))
    return stats

@processor(name="folder_summary", priority=40, type_hint="dir", metadata={"desc": "目录完成后写入表格与图"})
def folder_summary(path: Path, context, plot_width_inches: float = 4.5, dpi: int = 130):
    if not path.is_dir():
        return
    folder_key = str(path)
    folder_rows = get_bucket(context, "folder_rows")
    folder_values = get_bucket(context, "folder_values")
    rows = folder_rows.get(folder_key, [])
    values = folder_values.get(folder_key, [])
    if not rows:
        record_result(context, "skip", "no data files", folder=folder_key)
        return {"folder": folder_key, "rows": 0}

    doc_path = get_output(context, "doc_path")
    if not doc_path:
        record_result(context, "error", "doc path missing", folder=folder_key)
        return
    doc, resolved = get_or_create_doc(Path(doc_path))

    # 写表格
    table = doc.add_table(rows=len(rows) + 1, cols=4)
    hdr = table.rows[0].cells
    hdr[0].text = "File"
    hdr[1].text = "Count"
    hdr[2].text = "Sum"
    hdr[3].text = "Mean"
    for i, stat in enumerate(rows, start=1):
        c = table.rows[i].cells
        c[0].text = Path(stat["file"]).name
        c[1].text = str(stat["count"])
        c[2].text = f"{stat['sum']:.2f}"
        c[3].text = f"{stat['mean']:.2f}"

    # 生成并插入图
    img_path = Path(doc_path).parent / f"plot_{Path(folder_key).name}.png"
    try:
        save_plot_png_values(values, img_path, dpi=dpi, width_inches=plot_width_inches)
        doc.add_picture(str(img_path))
    except Exception as e:
        record_result(context, "error", f"plot failed: {e}", folder=folder_key)

    # 总结段落
    doc.add_paragraph(f"Summary for {Path(folder_key).name}: files={len(rows)}, total_values={len(values)}, sum={sum(values):.2f}")
    doc.save(str(resolved))
    record_result(context, "ok", "folder summarized", folder=folder_key, files=len(rows))
    return {"folder": folder_key, "files": len(rows), "values": len(values)}
