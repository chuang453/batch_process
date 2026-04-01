from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from .base import InputAdapter


class MemoryAdapter(InputAdapter):

    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not isinstance(params, dict):
            errors.append("params must be a dict")
            return errors
        if "df" not in params:
            errors.append("missing required param: df")
            return errors
        if not isinstance(params.get("df"), pd.DataFrame):
            errors.append("param 'df' must be a pandas DataFrame")
        return errors

    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        errors = self.validate_params(params)
        if errors:
            raise ValueError("; ".join(errors))
        return params["df"].copy()
