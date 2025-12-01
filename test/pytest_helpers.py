"""Small pytest helper utilities for recorder tests.

Provide thin wrappers used by unit tests to read the persisted history DB.
"""
from typing import List, Dict, Optional
from processors import builtin_recorders


def read_db_rows(log_dir: str, limit: Optional[int] = None) -> List[Dict]:
    """Return rows from the per-directory `processed_history.db` as dicts.

    Uses the `read_history_rows` helper from `processors.builtin_recorders`.
    """
    return builtin_recorders.read_history_rows(log_dir, limit=limit)
