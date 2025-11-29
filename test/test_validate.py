import os
from pathlib import Path

import processors  # ensure builtin processors are registered (processors/__init__.py imports modules)
import sys

# Ensure prints in the engine won't trigger Windows GBK encoding errors in CI/terminals
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Some WCMATCH versions expose constants with a leading underscore; ensure compatibility
try:
    import wcmatch.glob as _wglob
    if not hasattr(_wglob, 'PATHNAME'):
        setattr(_wglob, 'PATHNAME', getattr(_wglob, '_PATHNAME', 0))
    if not hasattr(_wglob, 'GLOBSTAR'):
        setattr(_wglob, 'GLOBSTAR', getattr(_wglob, '_GLOBSTAR', 0))
except Exception:
    # If wcmatch is missing or different, engine may still handle simple patterns
    pass
from core.engine import BatchProcessor


def test_backup_file_processor(tmp_path):
    """End-to-end validation: run engine on a small tree and verify backup_file runs.

    - Create a folder with a text file
    - Configure engine to run `backup_file` for **/*.txt with a custom backup_dir
    - Assert the backup file exists and a result entry was recorded
    """

    # Arrange
    root = tmp_path / "src"
    root.mkdir()
    src_file = root / "example.txt"
    src_file.write_text("hello world\n", encoding="utf-8")

    backup_root = tmp_path / "backup_out"

    config = {
        "**/*.txt": {
            "processors": ["backup_file"],
            "config": {"backup_dir": str(backup_root)}
        }
    }

    bp = BatchProcessor()
    bp.set_config(config)

    # Act
    context = bp.run(root)

    # Assert: backup file created and results recorded
    assert (backup_root / "example.txt").exists(), "backup file should exist"

    # engine stores results in context.results; find a backup result
    backup_results = [r for r in context.results if r.get("processor") == "backup_file"]
    assert backup_results, f"expected at least one backup_file result, got: {context.results}"
    res = backup_results[0]
    assert res.get("result") and "to" in res["result"], "result record should include 'to' path"
