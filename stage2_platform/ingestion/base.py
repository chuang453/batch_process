from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


class InputAdapter(ABC):

    @abstractmethod
    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        raise NotImplementedError
