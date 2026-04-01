from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .base import OutputWriter


class FileWriter(OutputWriter):

    def write(self, name: str, df: pd.DataFrame, params: Dict[str, Any]) -> str:
        params = params or {}
        output_dir = Path(params.get("output_dir", "."))
        output_dir.mkdir(parents=True, exist_ok=True)

        fmt = str(params.get("format", "parquet")).lower()
        if fmt not in ("parquet", "csv", "xlsx"):
            raise ValueError(f"unsupported output format: {fmt}")

        path = output_dir / f"{name}.{fmt}"
        if fmt == "parquet":
            df.to_parquet(path, index=False)
        elif fmt == "csv":
            df.to_csv(path, index=False)
        else:
            df.to_excel(path, index=False)
        return str(path)
