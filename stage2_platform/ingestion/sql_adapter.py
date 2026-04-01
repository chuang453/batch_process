from __future__ import annotations

import importlib
from typing import Any, Dict, List

import pandas as pd

from .base import InputAdapter


class SqlAdapter(InputAdapter):

    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not isinstance(params, dict):
            errors.append("params must be a dict")
            return errors
        if not params.get("connection_string"):
            errors.append("missing required param: connection_string")
        if not params.get("query"):
            errors.append("missing required param: query")
        return errors

    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        errors = self.validate_params(params)
        if errors:
            raise ValueError("; ".join(errors))

        try:
            sqlalchemy = importlib.import_module("sqlalchemy")
            create_engine = sqlalchemy.create_engine
        except Exception as exc:
            raise RuntimeError("sqlalchemy is required for SqlAdapter") from exc

        engine = create_engine(params["connection_string"])
        with engine.connect() as conn:
            return pd.read_sql(params["query"], conn)
