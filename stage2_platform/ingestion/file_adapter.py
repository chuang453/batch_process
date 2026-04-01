from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .base import InputAdapter


class FileAdapter(InputAdapter):

    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not isinstance(params, dict):
            errors.append("params must be a dict")
            return errors

        raw_path = params.get("path")
        if not raw_path:
            errors.append("missing required param: path")
            return errors

        path = Path(raw_path)
        if not path.exists():
            errors.append(f"file not found: {path}")
            return errors

        suffix = path.suffix.lower()
        if suffix not in (".csv", ".parquet", ".xlsx", ".xls"):
            errors.append(f"unsupported file extension: {suffix}")
        return errors

    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        errors = self.validate_params(params)
        if errors:
            raise ValueError("; ".join(errors))

        path = Path(params["path"])
        suffix = path.suffix.lower()

        if suffix == ".csv":
            return pd.read_csv(path, encoding=params.get("encoding", "utf-8"))
        if suffix == ".parquet":
            return pd.read_parquet(path)

        sheet_name = params.get("sheet_name", 0)
        return pd.read_excel(path, sheet_name=sheet_name)
