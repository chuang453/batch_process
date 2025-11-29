"""Standalone validation demo — runnable without pytest.

This script creates a small directory with a text file, configures the engine to
run the built-in `backup_file` processor, and verifies the backup is created.

Run with:
    python test/run_validate_demo.py
"""
import sys
import tempfile
from pathlib import Path

import processors  # register built-in processors
import sys

# make stdout utf-8 where possible (prevents UnicodeEncodeError on some Windows shells)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Make wcmatch flags available if the installed wcmatch uses underscore-prefixed names
try:
    import wcmatch.glob as _wglob
    if not hasattr(_wglob, 'PATHNAME'):
        setattr(_wglob, 'PATHNAME', getattr(_wglob, '_PATHNAME', 0))
    if not hasattr(_wglob, 'GLOBSTAR'):
        setattr(_wglob, 'GLOBSTAR', getattr(_wglob, '_GLOBSTAR', 0))
except Exception:
    pass
from core.engine import BatchProcessor


def main():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "src"
        root.mkdir()
        src_file = root / "example.txt"
        src_file.write_text("hello world\n", encoding="utf-8")

        backup_root = Path(td) / "backup_out"

        config = {
            "**/*.txt": {
                "processors": ["backup_file"],
                "config": {"backup_dir": str(backup_root)}
            }
        }

        bp = BatchProcessor()
        bp.set_config(config)

        print("Running processor on:", root)
        context = bp.run(root)

        expected = backup_root / "example.txt"
        if not expected.exists():
            print("FAILED: expected backup not found:", expected)
            print("Context results:")
            print(context.results)
            return 2

        print("OK — backup created:", expected)
        return 0


if __name__ == "__main__":
    sys.exit(main())
