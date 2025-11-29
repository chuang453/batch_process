from pathlib import Path
from typing import Any, List, Optional
import csv, json

def safe_read_text(path: Path, encoding: str = "utf-8") -> str:
    try:
        return path.read_text(encoding=encoding, errors="ignore")
    except Exception:
        return ""

def safe_read_json(path: Path, encoding: str = "utf-8") -> Any:
    try:
        txt = safe_read_text(path, encoding)
        return json.loads(txt) if txt else None
    except Exception:
        return None

def safe_read_csv_values(path: Path, encoding: str = "utf-8") -> List[float]:
    """旧接口：读取所有可转换为 float 的单元格（跳过首行作为潜在表头）。"""
    vals: List[float] = []
    try:
        txt = safe_read_text(path, encoding)
        if not txt:
            return vals
        reader = csv.reader(txt.splitlines())
        next(reader, None)  # header if present
        for row in reader:
            for v in row:
                try:
                    vals.append(float(v))
                except Exception:
                    pass
    except Exception:
        return vals
    return vals

def csv_values(path: Path, column: Optional[str] = None, encoding: str = "utf-8") -> List[float]:
    """新接口：提取 CSV 中某列的数值。

    - column=None 时：优先取第一列。
    - column 提供列名：使用 DictReader 按表头查找；若失败回退第一列。
    - 自动跳过无法转换为 float 的值。
    """
    vals: List[float] = []
    try:
        txt = safe_read_text(path, encoding)
        if not txt:
            return vals
        lines = txt.splitlines()
        if not lines:
            return vals
        if column:
            try:
                reader = csv.DictReader(lines)
                for row in reader:
                    if column in row:
                        try:
                            vals.append(float(row[column]))
                        except Exception:
                            pass
                if vals:
                    return vals
                # 如果指定列未取到值，回退第一列
            except Exception:
                pass
        reader = csv.reader(lines)
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            target = row[0]
            try:
                vals.append(float(target))
            except Exception:
                pass
    except Exception:
        return vals
    return vals
