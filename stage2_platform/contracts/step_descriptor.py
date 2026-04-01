from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, NamedTuple


@dataclass
class StepDescriptor:
    step_id: str
    op_type: str
    op_name: str
    params: Any = None
    detail: Dict[str, Any] = field(default_factory=dict)
    sub_steps: List[Dict[str, Any]] = field(default_factory=list)
    level: int = 0


@dataclass
class StepResult:
    df: Any
    success: bool
    error: str = ""
    step_id: str = ""
    op_name: str = ""


class StepEvent(NamedTuple):
    step_id: str
    op_name: str
    kind: str
    error: str = ""
