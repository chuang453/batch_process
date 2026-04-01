from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .base import InputAdapter


class Stage1ArtifactAdapter(InputAdapter):

    def validate_params(self, params: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not isinstance(params, dict):
            errors.append("params must be a dict")
            return errors

        artifact_path = params.get("artifact_path")
        if not artifact_path:
            errors.append("missing required param: artifact_path")
            return errors

        root = Path(artifact_path)
        if not root.exists() or not root.is_dir():
            errors.append(f"artifact_path does not exist or is not a directory: {root}")
            return errors

        if not (root / "metadata.json").exists():
            errors.append("metadata.json not found in artifact_path")
        return errors

    def load(self, params: Dict[str, Any]) -> pd.DataFrame:
        errors = self.validate_params(params)
        if errors:
            raise ValueError("; ".join(errors))

        root = Path(params["artifact_path"])
        metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))

        dataset_name = params.get("dataset")
        datasets = metadata.get("datasets", {})
        if dataset_name is None:
            if len(datasets) == 1:
                dataset_name = next(iter(datasets.keys()))
            elif "df" in datasets:
                dataset_name = "df"
            else:
                raise ValueError("multiple datasets in artifact; pass params['dataset']")

        if dataset_name not in datasets:
            raise ValueError(f"dataset not found in artifact metadata: {dataset_name}")

        rel = datasets[dataset_name].get("file")
        fmt = datasets[dataset_name].get("format", "parquet")
        file_path = root / rel
        if not file_path.exists():
            raise ValueError(f"dataset file not found: {file_path}")

        if fmt == "parquet":
            return pd.read_parquet(file_path)
        if fmt == "csv":
            return pd.read_csv(file_path)
        raise ValueError(f"unsupported dataset format in artifact: {fmt}")
