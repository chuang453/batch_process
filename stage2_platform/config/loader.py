from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from ruamel.yaml import YAML

from .normalizer import normalize_project

_yaml = YAML(typ="safe")


def load_project(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"project file not found: {p}")

    suffix = p.suffix.lower()
    if suffix in (".yaml", ".yml"):
        data = _yaml.load(p.read_text(encoding="utf-8")) or {}
    elif suffix == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"unsupported project file extension: {suffix}")

    if not isinstance(data, dict):
        raise ValueError("project config must be a dict")
    return normalize_project(data)
