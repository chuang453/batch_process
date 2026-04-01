from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .base import OutputWriter


class BundleWriter(OutputWriter):

    def write(self, name: str, df: pd.DataFrame, params: Dict[str, Any]) -> str:
        params = params or {}
        output_dir = Path(params.get("output_dir", "."))
        output_dir.mkdir(parents=True, exist_ok=True)

        bundle_dir = output_dir / f"{name}_bundle"
        bundle_dir.mkdir(parents=True, exist_ok=True)

        fmt = "parquet"
        data_file = bundle_dir / f"{name}.parquet"
        try:
            df.to_parquet(data_file, index=False)
        except Exception:
            fmt = "csv"
            data_file = bundle_dir / f"{name}.csv"
            df.to_csv(data_file, index=False)

        metadata = {
            "datasets": {
                name: {
                    "file": data_file.name,
                    "format": fmt,
                }
            }
        }
        (bundle_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        zip_path = output_dir / f"{name}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(data_file, arcname=data_file.name)
            zf.write(bundle_dir / "metadata.json", arcname="metadata.json")
        return str(zip_path)
