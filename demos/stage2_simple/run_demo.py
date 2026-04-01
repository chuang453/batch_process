from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path

from stage2_platform.api import Stage2Service


def _jsonable(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def main() -> int:
    project_path = Path(__file__).with_name("project.yaml")
    service = Stage2Service()
    service.load_project(str(project_path))

    errors = service.validate_project()
    if errors:
        print("Validation failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    manifest = service.run_project()
    print(json.dumps(_jsonable(manifest), ensure_ascii=False, indent=2))

    out = service.context.catalog.get("orders_prepared")
    if out is not None:
        print("\norders_prepared preview:")
        print(out.head(5).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
