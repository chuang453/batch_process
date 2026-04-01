from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SeriesManifest:
    series_name: str
    input_key: str
    output_key: str
    status: str
    started_at: str = ""
    finished_at: str = ""
    duration_s: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    error: str = ""


@dataclass
class RunManifest:
    run_id: str
    project_name: str
    status: str
    series_records: List[SeriesManifest] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
