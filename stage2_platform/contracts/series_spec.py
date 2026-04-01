from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SeriesSpec:
    name: str
    label: str = ""
    input_key: str = "df"
    output_key: str = ""
    steps: List[Dict[str, Any]] = field(default_factory=list)
    enabled: bool = True
    continue_on_error: bool = True
    output_policy: str = "overwrite"
