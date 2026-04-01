from __future__ import annotations

from pathlib import Path
from typing import Dict

import json
from ruamel.yaml import YAML

TEMPLATE: Dict = {
    "name": "stage2_demo",
    "inputs": [
        {
            "name": "raw",
            "source_type": "file",
            "source_params": {"path": "./data/raw.csv"},
        }
    ],
    "stages": [
        {
            "name": "clean",
            "type": "data",
            "source": "raw",
            "series": [
                {
                    "name": "default",
                    "input_key": "raw",
                    "output_key": "raw_clean",
                    "steps": [{"dropna": {}}, {"head": 1000}],
                }
            ],
        }
    ],
}


def generate_stage2_template(path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    suffix = p.suffix.lower()

    if suffix in (".yaml", ".yml"):
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.allow_unicode = True
        with p.open("w", encoding="utf-8") as f:
            yaml.dump(TEMPLATE, f)
        return

    if suffix == ".json":
        p.write_text(json.dumps(TEMPLATE, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    raise ValueError("template output file must be .yaml/.yml/.json")
