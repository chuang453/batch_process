import json
from pathlib import Path

import processors  # ensure builtin processors (and builtin_recorders) are registered

from decorators.processor import ProcessingContext


def test_persist_history_sqlite_async(tmp_path):
    # Arrange
    logs_dir = tmp_path / "logs"
    p = tmp_path / "file.txt"
    p.write_text("hello")

    ctx = ProcessingContext()
    # ensure there is a result for persist_history_sqlite to pick up
    ctx.results.append({"processor": "test_proc", "value": 123, "path": str(p)})

    # Act: call the new processor
    br = processors.builtin_recorders
    res = br.persist_history_sqlite(p, ctx, log_dir=str(logs_dir))
    assert res.get("enqueued") is True

    # Wait for writer to flush
    flushed = br.flush_history_queue(log_dir=str(logs_dir), timeout=3.0)
    assert flushed, "persist queue did not flush in time"

    # Assert: DB exists and contains the record
    dbpath = logs_dir / 'processed_history.db'
    assert dbpath.exists(), f"expected DB at {dbpath}"
    rows = br.read_history_rows(str(logs_dir))
    assert rows, "expected at least one DB row"
    assert rows[-1]['raw'].get('processor') == 'test_proc'

    # cleanup writer thread
    try:
        br.shutdown_writer(log_dir=str(logs_dir))
    except Exception:
        pass


def test_persist_history_sqlite_batch_writes(tmp_path):
    # Arrange
    logs_dir = tmp_path / "logs2"
    br = processors.builtin_recorders

    # Enqueue many records to trigger batch behavior
    total = 120
    for i in range(total):
        rec = {"processor": f"p_{i}", "index": i, "time": "now"}
        br.enqueue_persist(rec, log_dir=str(logs_dir))

    # Act: wait for flush
    flushed = br.flush_history_queue(log_dir=str(logs_dir), timeout=10.0)
    assert flushed, "persist queue did not flush in time"

    # Assert: DB exists and contains expected number of rows
    dbpath = logs_dir / 'processed_history.db'
    assert dbpath.exists(), "expected history DB"
    rows = br.read_history_rows(str(logs_dir))
    assert len(rows) == total, f"expected {total} rows, got {len(rows)}"

    # cleanup
    try:
        br.shutdown_writer(log_dir=str(logs_dir))
    except Exception:
        pass
