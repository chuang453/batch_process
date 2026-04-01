from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml


SOURCE_PARAM_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "file": {"path": "", "encoding": "utf-8"},
    "memory": {"df": None},
    "stage1_artifact": {"artifact_path": "", "dataset": "df"},
    "sql": {"connection_string": "", "query": ""},
    "api": {"url": "", "timeout": 30, "data_key": ""},
}


def clone_project(project: Dict[str, Any] | None) -> Dict[str, Any]:
    return copy.deepcopy(project or {"name": "stage2_project", "inputs": [], "stages": []})


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


def get_source_param_template(source_type: str) -> Dict[str, Any]:
    return copy.deepcopy(SOURCE_PARAM_TEMPLATES.get(str(source_type or "").strip(), {}))


def parse_structured_text(text: str, label: str) -> Any:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return yaml.safe_load(raw)
        except Exception as exc:
            raise ValueError(f"{label} parse failed: {exc}") from exc


def parse_mapping_text(text: str, label: str) -> Dict[str, Any]:
    value = parse_structured_text(text, label)
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping/object")
    return value


def parse_list_text(text: str, label: str) -> List[Any]:
    value = parse_structured_text(text, label)
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list/array")
    return value


def serialize_project_for_disk(project: Dict[str, Any]) -> Dict[str, Any]:
    data = clone_project(project)
    for item in data.get("inputs", []) or []:
        params = item.get("source_params") or {}
        if item.get("source_type") == "memory" and isinstance(params.get("df"), pd.DataFrame):
            name = item.get("name", "input")
            raise ValueError(
                f"input '{name}' uses an in-memory DataFrame and cannot be saved directly; export it as artifact/file first"
            )
    return _serialize_value(data)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_value(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.DataFrame):
        raise ValueError("DataFrame values cannot be serialized into project files")
    return value