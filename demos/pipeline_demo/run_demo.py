"""Minimal pipeline demo.

Usage (from repo root):
    python demos/pipeline_demo/run_demo.py

Creates a small temporary directory tree with .txt files, runs the pipeline
defined in config_pipeline.yaml, and prints the final context results.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

# Allow imports from repo root when run directly
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import yaml

from config.loader import load_config, load_plugins
from core.pipeline import Pipeline
from decorators.processor import ProcessingContext

# Load built-in processors (registers file_ops, builtin_recorders, etc.)
import processors  # noqa: F401 — side-effect: registers all built-in processors
# Load plugin processors (plugins/ directory)
load_plugins()

# ── build a tiny demo tree ────────────────────────────────────────────────────
with tempfile.TemporaryDirectory(prefix="pipeline_demo_") as tmpdir:
    root = Path(tmpdir)
    (root / "alpha").mkdir()
    (root / "alpha" / "a.txt").write_text("hello")
    (root / "alpha" / "b.txt").write_text("world")
    (root / "beta").mkdir()
    (root / "beta" / "c.txt").write_text("foo")

    # ── load config ───────────────────────────────────────────────────────────
    cfg_path = Path(__file__).parent / "config_pipeline.yaml"
    config = load_config(str(cfg_path))

    # patch stage root to our temp dir
    for stage in config.get("pipeline", []):
        if stage.get("type") == "walk":
            stage["root"] = str(root)

    # ── run pipeline ──────────────────────────────────────────────────────────
    ctx = ProcessingContext()
    pipeline = Pipeline(stages=config["pipeline"], context=ctx)

    def _progress(current, total, msg=""):
        bar = "#" * int(20 * current / max(total, 1))
        print(f"\r  [{bar:<20}] {current}/{total}  {msg}", end="", flush=True)

    pipeline.set_progress_callback(_progress)
    pipeline.run(root_path=root, context=ctx)
    print()  # newline after progress bar

    # ── print results ─────────────────────────────────────────────────────────
    print(f"\nProcessed {len(ctx.results)} result(s):")
    for r in ctx.results:
        print(f"  {r}")

    executed = ctx.get_shared(["executed"], {})
    all_entries = []
    def _collect(node):
        if isinstance(node, list):
            all_entries.extend(node)
        elif isinstance(node, dict):
            for v in node.values():
                _collect(v)
    _collect(executed)
    print(f"\nShared 'executed' entries: {len(all_entries)}")
    for entry in all_entries:
        print(f"  [{entry.get('time', '?')}] {entry.get('type', '?')} {entry.get('path', '?')}")
