from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path

import pandas as pd

from stage2_platform.api import Stage2Service


def _jsonable(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _export_outputs(service: Stage2Service, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for key, value in service.context.catalog.items():
        if isinstance(value, pd.DataFrame):
            value.to_csv(out_dir / f"{key}.csv", index=False)


def main() -> int:
    project_path = Path(__file__).with_name("project.yaml")
    output_dir = Path(__file__).with_name("output")

    service = Stage2Service()
    service.load_project(str(project_path))

    errors = service.validate_project()
    if errors:
        print("Validation failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    simulate = service.simulate()
    print("Simulation summary:")
    print(json.dumps(simulate, ensure_ascii=False, indent=2))

    manifest = service.run_project()
    print("\nRun manifest:")
    print(json.dumps(_jsonable(manifest), ensure_ascii=False, indent=2))

    _export_outputs(service, output_dir)
    print(f"\nCSV outputs exported to: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
