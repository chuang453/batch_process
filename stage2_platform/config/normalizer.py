from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def _infer_source_type(input_item: Dict[str, Any]) -> str:
    explicit = input_item.get("source_type")
    if explicit:
        return str(explicit)

    params = input_item.get("source_params") or {}
    raw_path = params.get("path")
    if raw_path:
        suffix = Path(raw_path).suffix.lower()
        if suffix in (".csv", ".parquet", ".xlsx", ".xls"):
            return "file"
    if "artifact_path" in params:
        return "stage1_artifact"
    if "connection_string" in params:
        return "sql"
    if "url" in params:
        return "api"
    if "df" in params:
        return "memory"
    return "file"


def normalize_project(raw: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(raw or {})
    cfg.setdefault("name", "stage2_project")

    inputs = []
    for idx, item in enumerate(cfg.get("inputs", []) or [], start=1):
        cur = dict(item or {})
        cur.setdefault("name", f"input_{idx}")
        cur.setdefault("source_params", {})
        cur["source_type"] = _infer_source_type(cur)
        inputs.append(cur)
    cfg["inputs"] = inputs

    stages = []
    for stage in cfg.get("stages", []) or []:
        st = dict(stage or {})
        st.setdefault("name", "data")
        st.setdefault("type", "data")
        st.setdefault("source", "df")

        if not isinstance(st.get("series"), list):
            st["series"] = [{
                "name": "default",
                "input_key": st.get("source", "df"),
                "output_key": st.get("output_key", ""),
                "steps": list(st.get("steps", []) or []),
                "enabled": True,
                "continue_on_error": bool(st.get("continue_on_error", True)),
                "output_policy": st.get("output_policy", "overwrite"),
            }]

        normalized_series = []
        for sidx, item in enumerate(st.get("series", []) or [], start=1):
            ser = dict(item or {})
            ser.setdefault("name", f"series_{sidx}")
            ser.setdefault("input_key", st.get("source", "df"))
            ser.setdefault("output_key", "")
            ser.setdefault("steps", [])
            ser.setdefault("enabled", True)
            ser.setdefault("continue_on_error", True)
            ser.setdefault("output_policy", "overwrite")
            normalized_series.append(ser)
        st["series"] = normalized_series
        stages.append(st)

    cfg["stages"] = stages
    return cfg
