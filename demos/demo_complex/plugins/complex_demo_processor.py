"""Complex demo plugin: handles folder labels, 3-type data reading, plotting, and Word document generation.

This plugin demonstrates:
1. Writing folder labels when entering directories
2. Reading files with 3 types of data (Type1: main data occupies 2 cells, Type2 & Type3: 1 cell each)
3. Writing data to Word tables
4. Creating individual plots for each file
5. Writing summary and comprehensive plot when exiting directories

Dependencies: matplotlib, python-docx, Pillow
"""
from pathlib import Path
from typing import List, Dict, Any, Tuple
from decorators.processor import processor
from utils.pipeline import ensure_dir, get_bucket, set_output
from utils.adapters.docx_helpers import (
    get_or_create_doc,
    save_doc,
    docx_table_with_caption_and_merges,
    docx_insert_picture,
    docx_write_text,
)
from utils.adapters.plot_helpers import save_plot_png_values
from processors.file_ops import set_path_name_dict
import os
import time
import re

# Force non-interactive backend before any matplotlib import
os.environ.setdefault("MPLBACKEND", "Agg")


def _ensure_packages():
    """Ensure required packages are available."""
    try:
        import matplotlib  # noqa: F401
        from docx import Document  # noqa: F401
        from matplotlib.figure import Figure  # noqa: F401
        from matplotlib.backends.backend_agg import FigureCanvasAgg  # noqa: F401
        from PIL import Image  # noqa: F401
        return True, None
    except Exception as e:
        return False, e


# Folder labels mapping
FOLDER_LABELS = {
    "folder_A": "实验组A - 温度控制实验",
    "folder_B": "实验组B - 电力测试实验",
    "folder_C": "实验组C - 化学分析实验",
}


def parse_three_type_data(
        file_path: Path) -> Tuple[List[float], List[float], List[float]]:
    """Parse file with 3 types of data.
    
    Returns:
        Tuple of (type1_data, type2_data, type3_data)
    """
    type1_data = []
    type2_data = []
    type3_data = []

    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("TYPE1:"):
                # Extract numbers after TYPE1:
                nums_str = line[6:].strip()
                for num in re.split(r',\s*', nums_str):
                    try:
                        type1_data.append(float(num))
                    except:
                        pass
            elif line.startswith("TYPE2:"):
                nums_str = line[6:].strip()
                for num in re.split(r',\s*', nums_str):
                    try:
                        type2_data.append(float(num))
                    except:
                        pass
            elif line.startswith("TYPE3:"):
                nums_str = line[6:].strip()
                for num in re.split(r',\s*', nums_str):
                    try:
                        type3_data.append(float(num))
                    except:
                        pass
    except Exception as e:
        print(f"Error parsing file {file_path}: {e}")

    return type1_data, type2_data, type3_data


@processor(name="enter_folder_label",
           priority=90,
           source=__file__,
           metadata={
               "name":
               "Enter Folder - Write Label",
               "author":
               "complex_demo",
               "version":
               "1.0",
               "description":
               "When entering a folder, write its label to Word document",
           })
def enter_folder_label(path: Path, context, **cfg):
    """Write folder label when entering a directory."""
    if not path.is_dir():
        return {"skipped": True}

    ok, err = _ensure_packages()
    if not ok:
        return {"error": f"missing packages: {err}"}
    # populate per-file labels using built-in processor if a _dict file exists
    try:
        set_path_name_dict(path,
                           context,
                           _dict_file=cfg.get('_dict_file', '_dict.txt'),
                           force=bool(cfg.get('force', False)))
    except Exception:
        # non-fatal
        pass

    # Document handling via adapter
    out_doc_path = cfg.get("doc_path", "./demo_complex_output.docx")
    doc, resolved = get_or_create_doc(Path(out_doc_path))

    # Folder label prefers mapping, falls back to folder name
    folder_name = path.name
    label = FOLDER_LABELS.get(folder_name, folder_name)

    # Add heading and path info
    doc.add_heading(f"📁 {label}", level=2)
    docx_write_text(doc, f"路径: {path.relative_to(context.root_path)}")
    docx_write_text(doc, "")
    save_doc(doc, resolved)

    # Store doc path in shared context for downstream processors
    context.set_shared(["complex_demo", "doc_path"], str(resolved))

    return {
        "doc": str(resolved),
        "folder": folder_name,
        "label": label,
        "action": "enter_folder_label"
    }


@processor(
    name="read_three_type_data",
    priority=70,
    source=__file__,
    metadata={
        "name":
        "Read 3-Type Data",
        "author":
        "complex_demo",
        "version":
        "1.0",
        "description":
        "Read files with 3 types of data, write to Word table, and create plot",
    })
def read_three_type_data(path: Path, context, **cfg):
    """Read file with 3 types of data, write to Word, and create individual plot."""
    if not path.is_file():
        return {"skipped": True}

    ok, err = _ensure_packages()
    if not ok:
        return {"error": f"missing packages: {err}"}

    # Parse data
    type1, type2, type3 = parse_three_type_data(path)

    if not (type1 or type2 or type3):
        return {"skipped": True, "reason": "no_data"}

    # Document handling via adapter
    doc_path = context.get_shared(["complex_demo", "doc_path"],
                                  cfg.get("doc_path",
                                          "./demo_complex_output.docx"))
    doc, resolved = get_or_create_doc(Path(doc_path))

    # Add file name as subheading
    doc.add_heading(f"文件: {path.name}", level=3)

    # Compose table using adapter and merge groups (0-based indices)
    type1_str = ", ".join(f"{v:.2f}" for v in type1[:10])
    if len(type1) > 10:
        type1_str += f" ... (共{len(type1)}个)"
    type2_str = ", ".join(f"{v:.2f}" for v in type2[:5])
    if len(type2) > 5:
        type2_str += f" ... (共{len(type2)}个)"
    type3_str = ", ".join(f"{v:.2f}" for v in type3[:5])
    if len(type3) > 5:
        type3_str += f" ... (共{len(type3)}个)"

    header = ["Type1 主要数据", "", "Type2 辅助数据1", "Type3 辅助数据2"]
    data_row = [type1_str, "", type2_str, type3_str]
    merge_groups = [[(0, 0), (0, 1)], [(1, 0), (1, 1)]]
    # docx_table_with_caption_and_merges expects merge_groups as row/col tuples (0-based)
    docx_table_with_caption_and_merges(doc,
                                       data=[data_row],
                                       header=header,
                                       caption=f"{path.name} 数据",
                                       merge_groups=merge_groups)

    # Per-file label (from set_path_name_dict) if available
    label_list = context.get_data(['labels', str(path)], []) or []
    label_text = ", ".join(label_list) if label_list else path.name
    docx_write_text(doc, f"标签: {label_text}")

    # Create individual combined plot (multiple series) using matplotlib inline
    img_dir = Path(cfg.get("img_dir", "./demo_complex_images"))
    img_dir.mkdir(parents=True, exist_ok=True)
    img_path = img_dir / f"plot_{path.stem}.png"
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)
        if type1:
            ax.plot(type1,
                    marker='o',
                    label='Type1 主要数据',
                    linewidth=2,
                    markersize=4)
        if type2:
            ax.plot(type2,
                    marker='s',
                    label='Type2 辅助数据1',
                    linewidth=1.5,
                    markersize=4)
        if type3:
            ax.plot(type3,
                    marker='^',
                    label='Type3 辅助数据2',
                    linewidth=1.5,
                    markersize=4)
        ax.set_title(f"数据曲线 - {path.stem}")
        ax.set_xlabel("索引")
        ax.set_ylabel("数值")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        canvas = FigureCanvas(fig)
        canvas.draw()
        fig.savefig(img_path)
        docx_insert_picture(doc,
                            img_path,
                            width_inches=5.5,
                            caption=f"{path.name} 可视化")
    except Exception:
        docx_write_text(doc, "绘图失败: 无法生成图片")

    save_doc(doc, resolved)

    # Store data for folder summary
    folder_key = str(path.parent)
    folder_bucket = context.setdefault_data(
        ["complex_demo", "folder_data", folder_key], {
            "type1": [],
            "type2": [],
            "type3": [],
            "files": []
        })
    folder_bucket["type1"].extend(type1)
    folder_bucket["type2"].extend(type2)
    folder_bucket["type3"].extend(type3)
    folder_bucket["files"].append(path.name)

    return {
        "file": str(path),
        "type1_count": len(type1),
        "type2_count": len(type2),
        "type3_count": len(type3),
        "plot": str(img_path),
        "action": "read_three_type_data"
    }


@processor(name="exit_folder_summary",
           priority=60,
           source=__file__,
           metadata={
               "name":
               "Exit Folder - Summary & Plot",
               "author":
               "complex_demo",
               "version":
               "1.0",
               "description":
               "On folder exit: write summary and create comprehensive plot",
           })
def exit_folder_summary(path: Path, context, **cfg):
    """Write summary and comprehensive plot when exiting a folder."""
    if not path.is_dir():
        return {"skipped": True}

    ok, err = _ensure_packages()
    if not ok:
        return {"error": f"missing packages: {err}"}

    # Get folder data
    folder_key = str(path)
    folder_data = context.get_data(["complex_demo", "folder_data", folder_key],
                                   {})
    type1 = folder_data.get("type1", [])
    type2 = folder_data.get("type2", [])
    type3 = folder_data.get("type3", [])
    files = folder_data.get("files", [])
    if not (type1 or type2 or type3):
        return {"skipped": True, "reason": "no_data"}

    # Document via adapter
    doc_path = context.get_shared(["complex_demo", "doc_path"],
                                  cfg.get("doc_path",
                                          "./demo_complex_output.docx"))
    doc, resolved = get_or_create_doc(Path(doc_path))

    # Heading and summary
    folder_name = path.name
    label = FOLDER_LABELS.get(folder_name, folder_name)
    doc.add_heading(f"📊 {label} - 总结", level=3)

    type1_avg = sum(type1) / len(type1) if type1 else 0
    type2_avg = sum(type2) / len(type2) if type2 else 0
    type3_avg = sum(type3) / len(type3) if type3 else 0
    summary_text = (
        f"本目录共处理 {len(files)} 个文件。\n"
        f"- Type1 主要数据: {len(type1)} 个数据点\n"
        f"- Type2 辅助数据1: {len(type2)} 个数据点\n"
        f"- Type3 辅助数据2: {len(type3)} 个数据点\n\n"
        f"数据统计:\n- Type1 平均值: {type1_avg:.2f}\n- Type2 平均值: {type2_avg:.2f}\n- Type3 平均值: {type3_avg:.2f}"
    )
    docx_write_text(doc, summary_text)

    # Use adapter to create comprehensive plot (we'll combine series inline)
    img_dir = Path(cfg.get("img_dir", "./demo_complex_images"))
    img_dir.mkdir(parents=True, exist_ok=True)
    img_path = img_dir / f"summary_{folder_name}.png"
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
        fig = Figure(figsize=(7, 5), dpi=120)
        ax = fig.add_subplot(111)
        if type1:
            ax.plot(type1,
                    marker='o',
                    label=f'Type1 主要数据 (n={len(type1)})',
                    linewidth=2.5,
                    markersize=5,
                    alpha=0.8)
        if type2:
            ax.plot(type2,
                    marker='s',
                    label=f'Type2 辅助数据1 (n={len(type2)})',
                    linewidth=2,
                    markersize=5,
                    alpha=0.8)
        if type3:
            ax.plot(type3,
                    marker='^',
                    label=f'Type3 辅助数据2 (n={len(type3)})',
                    linewidth=2,
                    markersize=5,
                    alpha=0.8)
        ax.set_title(f"综合数据曲线 - {label}", fontsize=14, fontweight='bold')
        ax.set_xlabel("数据索引", fontsize=11)
        ax.set_ylabel("数值", fontsize=11)
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3, linestyle='--')
        fig.tight_layout()
        canvas = FigureCanvas(fig)
        canvas.draw()
        fig.savefig(img_path)
        docx_insert_picture(doc,
                            img_path,
                            width_inches=6.0,
                            caption=f"{label} 综合可视化")
    except Exception as e:
        docx_write_text(doc, f"综合绘图失败: {e}")

    docx_write_text(doc, "".join(["="] * 60))
    save_doc(doc, resolved)

    return {
        "folder": folder_name,
        "label": label,
        "files_processed": len(files),
        "type1_count": len(type1),
        "type2_count": len(type2),
        "type3_count": len(type3),
        "summary_plot": str(img_path),
        "action": "exit_folder_summary"
    }
