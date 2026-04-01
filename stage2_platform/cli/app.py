from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from typing import Any

from stage2_platform.api import Stage2Service


def _to_jsonable(value: Any):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stage2", description="Stage 2 data platform CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run a Stage 2 project")
    run_parser.add_argument("project")

    validate_parser = sub.add_parser("validate", help="Validate a Stage 2 project")
    validate_parser.add_argument("project")

    simulate_parser = sub.add_parser("simulate", help="Simulate a Stage 2 project")
    simulate_parser.add_argument("project")

    sub.add_parser("list-ops", help="List builtin ops and registered transforms")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    service = Stage2Service()

    if args.command == "list-ops":
        print(json.dumps(service.list_ops(), ensure_ascii=False, indent=2))
        return 0

    service.load_project(args.project)

    if args.command == "validate":
        errors = service.validate_project()
        print(json.dumps({"ok": not errors, "errors": errors}, ensure_ascii=False, indent=2))
        return 0 if not errors else 1

    if args.command == "simulate":
        print(json.dumps(_to_jsonable(service.simulate()), ensure_ascii=False, indent=2))
        return 0

    if args.command == "run":
        manifest = service.run_project()
        print(json.dumps(_to_jsonable(manifest), ensure_ascii=False, indent=2))
        return 0 if getattr(manifest, "status", "") in ("done", "partial") else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
