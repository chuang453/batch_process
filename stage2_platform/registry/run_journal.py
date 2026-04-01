from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

from stage2_platform.contracts import RunManifest


class RunJournal:

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, manifest: RunManifest) -> None:
        row = asdict(manifest)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def read_all(self) -> List[Dict]:
        if not self.path.exists():
            return []
        out: List[Dict] = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out
