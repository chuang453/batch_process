from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from .base import OutputWriter


class MemoryWriter(OutputWriter):

    def __init__(self, target: Dict[str, pd.DataFrame]):
        self.target = target

    def write(self, name: str, df: pd.DataFrame, params: Dict[str, Any]) -> str:
        policy = str((params or {}).get("policy", "overwrite"))
        key = name
        if policy == "versioned":
            suffix = 1
            while key in self.target:
                suffix += 1
                key = f"{name}_{suffix}"
        elif policy == "skip" and key in self.target:
            return key

        self.target[key] = df.copy()
        return key
