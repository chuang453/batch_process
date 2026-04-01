from __future__ import annotations

from datetime import datetime
from typing import Dict, List


class LineageTracker:

    def __init__(self):
        self._rows: List[Dict] = []

    def add(self, input_key: str, series_name: str, output_key: str) -> None:
        self._rows.append({
            "input_key": input_key,
            "series_name": series_name,
            "output_key": output_key,
            "timestamp": datetime.now().isoformat(sep=" ", timespec="seconds"),
        })

    def get_by_output(self, output_key: str) -> List[Dict]:
        return [row for row in self._rows if row.get("output_key") == output_key]

    def all(self) -> List[Dict]:
        return list(self._rows)
