from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from stage2_platform.contracts import RunManifest


class DatasetCatalog:

    def __init__(self):
        self._store: Dict[str, Any] = {}

    def get(self, key: str, default=None):
        return self._store.get(key, default)

    def put(self, key: str, value: Any, policy: str = "overwrite") -> str:
        policy = str(policy or "overwrite")
        target_key = str(key)

        if policy == "skip" and target_key in self._store:
            return target_key

        if policy == "versioned":
            suffix = 1
            while target_key in self._store:
                suffix += 1
                target_key = f"{key}_{suffix}"

        stored = value.copy() if hasattr(value, "copy") else value
        self._store[target_key] = stored
        return target_key

    def keys(self) -> List[str]:
        return list(self._store.keys())

    def has(self, key: str) -> bool:
        return key in self._store

    def items(self):
        return self._store.items()


@dataclass
class Stage2Context:
    catalog: DatasetCatalog = field(default_factory=DatasetCatalog)
    manifest: RunManifest | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    logs: List[str] = field(default_factory=list)

    def log(self, message: str) -> None:
        self.logs.append(str(message))
