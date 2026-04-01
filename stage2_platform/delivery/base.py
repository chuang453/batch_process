from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import pandas as pd


class OutputWriter(ABC):

    @abstractmethod
    def write(self, name: str, df: pd.DataFrame, params: Dict[str, Any]) -> Any:
        raise NotImplementedError
