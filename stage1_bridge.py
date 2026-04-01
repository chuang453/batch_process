from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd

from decorators.processor import ProcessingContext


def export_artifact(context: ProcessingContext, output_dir: str | Path) -> Path:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    datasets: Dict[str, Dict[str, str]] = {}
    for key, value in (getattr(context, 'main', {}) or {}).items():
        if not isinstance(value, pd.DataFrame):
            continue

        fmt = 'parquet'
        file_path = root / f'{key}.parquet'
        try:
            value.to_parquet(file_path, index=False)
        except Exception:
            fmt = 'csv'
            file_path = root / f'{key}.csv'
            value.to_csv(file_path, index=False)

        datasets[str(key)] = {
            'file': file_path.name,
            'format': fmt,
        }

    metadata = {
        'datasets': datasets,
    }
    (root / 'metadata.json').write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return root
