from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Set

IMPACT_MAP = {
    "core/engine.py": [
        "test/test_validate.py",
        "test/test_file_ops.py",
        "test/test_main_routing.py",
    ],
    "core/data_stage.py": [
        "test/test_data_stage.py",
        "test/test_transform.py",
    ],
    "core/pipeline.py": [
        "test/test_pipeline.py",
        "test/test_pipeline_helpers.py",
        "test/test_pipeline_simulate_compat.py",
    ],
    "decorators/processor.py": [
        "test/test_builtin_recorders.py",
        "test/test_processor_metadata.py",
        "test/test_validate.py",
    ],
    "config/loader.py": [
        "test/test_config_loader_pipeline.py",
        "test/test_validate.py",
    ],
    "processors/": [
        "test/test_file_ops.py",
        "test/test_builtin_recorders.py",
    ],
    "widgets/batch_thread.py": [
        "test/test_pipeline_worker_signals.py",
    ],
}


def _normalize(path: str) -> str:
    p = path.strip().replace("\\", "/")
    return p.lstrip("./")


def select_tests_for_paths(changed_paths: Iterable[str]) -> List[str]:
    selected: Set[str] = set()
    normalized_paths = [_normalize(p) for p in changed_paths if p.strip()]

    for path in normalized_paths:
        if path in IMPACT_MAP:
            selected.update(IMPACT_MAP[path])

        for key, tests in IMPACT_MAP.items():
            if key.endswith("/") and path.startswith(key):
                selected.update(tests)

    return sorted(selected)


def _read_changed_paths(use_git_diff: bool) -> List[str]:
    if use_git_diff:
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(result.stderr.strip() or "git diff failed", file=sys.stderr)
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    return [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]


def _existing_tests(paths: Iterable[str]) -> List[str]:
    return [p for p in paths if Path(p).exists()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Select impacted pytest files from changed paths.")
    parser.add_argument(
        "--git-diff",
        action="store_true",
        help="Read changed files from `git diff --name-only HEAD` instead of stdin.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run `pytest -q <selected tests>` directly.",
    )
    parser.add_argument(
        "--all-on-empty",
        action="store_true",
        help="When no impacted tests are found, run `pytest -q` if --run is set.",
    )
    args = parser.parse_args()

    changed = _read_changed_paths(use_git_diff=args.git_diff)
    selected = _existing_tests(select_tests_for_paths(changed))

    if not args.run:
        if selected:
            print(" ".join(selected))
        return 0

    if selected:
        cmd = ["pytest", "-q", *selected]
        return subprocess.run(cmd, check=False).returncode

    if args.all_on_empty:
        return subprocess.run(["pytest", "-q"], check=False).returncode

    print("No impacted tests selected; skipping pytest run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
