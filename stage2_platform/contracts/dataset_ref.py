from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class DatasetRef:
    name: str
    source_type: str
    source_params: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
