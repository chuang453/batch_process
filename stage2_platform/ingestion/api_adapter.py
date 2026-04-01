from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from .base import InputAdapter


class ApiAdapter(InputAdapter):

    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not isinstance(params, dict):
            errors.append("params must be a dict")
            return errors
        if not params.get("url"):
            errors.append("missing required param: url")
        return errors

    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        errors = self.validate_params(params)
        if errors:
            raise ValueError("; ".join(errors))

        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for ApiAdapter") from exc

        headers = params.get("headers") or {}
        response = requests.get(params["url"], headers=headers, timeout=int(params.get("timeout", 30)))
        response.raise_for_status()
        data = response.json()

        records = data
        if isinstance(records, dict):
            key = params.get("data_key")
            if key and key in records:
                records = records[key]
            else:
                records = [records]

        if not isinstance(records, list):
            records = [records]
        return pd.DataFrame(records)
