from __future__ import annotations

from typing import Dict, Type

import pandas as pd

from stage2_platform.contracts import DatasetRef

from .api_adapter import ApiAdapter
from .base import InputAdapter
from .file_adapter import FileAdapter
from .memory_adapter import MemoryAdapter
from .sql_adapter import SqlAdapter
from .stage1_adapter import Stage1ArtifactAdapter

ADAPTER_REGISTRY: Dict[str, Type[InputAdapter]] = {
    "file": FileAdapter,
    "memory": MemoryAdapter,
    "in_memory": MemoryAdapter,
    "stage1_artifact": Stage1ArtifactAdapter,
    "sql": SqlAdapter,
    "api": ApiAdapter,
}


def load_dataset(ref: DatasetRef) -> pd.DataFrame:
    adapter_cls = ADAPTER_REGISTRY.get(ref.source_type)
    if adapter_cls is None:
        raise ValueError(f"unsupported source_type: {ref.source_type}")
    adapter = adapter_cls()
    errors = adapter.validate_params(ref.source_params)
    if errors:
        raise ValueError("; ".join(errors))
    return adapter.load(ref.source_params)
