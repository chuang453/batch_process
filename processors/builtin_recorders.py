from datetime import datetime
import json
from pathlib import Path
from typing import Any, Optional
import threading
import queue
import time

from decorators.processor import processor, ProcessingContext
import sqlite3


def _make_parts_key(path: Path, context: ProcessingContext):
    try:
        rel_path = path.relative_to(
            context.root_path) if context.root_path else path
        parts = list(rel_path.parts) if rel_path != Path('.') else ['.']
    except Exception:
        parts = [str(path)]
    parts_key = [p + '/' for p in parts[:-1]] + [parts[-1]]
    return parts_key


@processor(name="record_to_shared", priority=10)
def record_to_shared(path: Path, context: ProcessingContext, **kwargs) -> Any:
    """Append a short execution record into `context.shared['executed'][...parts_key...]`.

    Intended as an optional inline/post processor you can enable per-path.
    """
    try:
        ts = datetime.now().isoformat(sep=' ', timespec='seconds')
        parts_key = ['executed'] + _make_parts_key(path, context)
        lst = context.setdefault_shared(parts_key, [])
        entry = {
            'time':
            ts,
            'processor':
            getattr(record_to_shared, 'processor_name', 'record_to_shared'),
            'path':
            str(path),
            'type':
            'dir' if path.is_dir() else 'file',
            'note':
            kwargs.get('note'),
        }
        lst.append(entry)
        return {'recorded': True}
    except Exception as e:
        return {'recorded': False, 'error': str(e)}


# ---- SQLite-backed background writer ----
_writers: dict = {}


class SQLiteBatchWriter:

    def __init__(self,
                 db_path: str,
                 batch_size: int = 200,
                 flush_interval: float = 0.5):
        self.db_path = str(db_path)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._queue: 'queue.Queue[dict]' = queue.Queue()
        self._shutdown = False
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"sqlite_writer_{Path(db_path).name}")
        self._ensure_db()
        self._thread.start()

    def _ensure_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        schema = """
        CREATE TABLE IF NOT EXISTS processed_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          path TEXT,
          processor TEXT,
          phase TEXT,
          status TEXT,
          cfg TEXT,
          result TEXT,
          error TEXT,
          raw TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_proc_time ON processed_history(processor, ts);
        CREATE INDEX IF NOT EXISTS idx_path ON processed_history(path);
        """
        try:
            conn.executescript(schema)
            conn.commit()
        finally:
            conn.close()

    def enqueue(self, record: dict):
        self._queue.put(record)

    def _flush(self, records: list):
        if not records:
            return
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            sql = (
                "INSERT INTO processed_history (ts, path, processor, phase, status, cfg, result, error, raw) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            params = []
            for r in records:
                ts = r.get('time') or r.get('ts') or ''
                path = r.get('path')
                processor = r.get('processor')
                phase = r.get('phase')
                status = r.get('status')
                cfg = json.dumps(r.get('config', {}), ensure_ascii=False)
                result = json.dumps(r.get('result', None), ensure_ascii=False)
                error = r.get('error')
                raw = json.dumps(r, ensure_ascii=False)
                params.append((ts, path, processor, phase, status, cfg, result,
                               error, raw))
            cur.executemany(sql, params)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()

    def _loop(self):
        buffer = []
        last_flush = time.time()
        while True:
            try:
                item = self._queue.get(timeout=self.flush_interval)
                buffer.append(item)
                self._queue.task_done()
            except queue.Empty:
                pass

            now = time.time()
            should_flush = False
            if len(buffer) >= self.batch_size:
                should_flush = True
            elif buffer and (now - last_flush) >= self.flush_interval:
                should_flush = True
            elif self._shutdown and buffer:
                should_flush = True

            if should_flush:
                try:
                    self._flush(buffer)
                except Exception:
                    pass
                buffer.clear()
                last_flush = now

            if self._shutdown and self._queue.empty() and not buffer:
                break

    def flush(self, timeout: float = 5.0) -> bool:
        start = time.time()
        while not self._queue.empty():
            if time.time() - start > timeout:
                return False
            time.sleep(0.02)
        # small pause to let internal loop flush buffer
        time.sleep(self.flush_interval + 0.05)
        return True

    def shutdown(self, timeout: float = 2.0) -> bool:
        self._shutdown = True
        start = time.time()
        while self._thread.is_alive() and time.time() - start < timeout:
            time.sleep(0.02)
        if self._thread.is_alive():
            self._thread.join(timeout=0.5)
        return not self._thread.is_alive()


def _get_writer_for_dir(log_dir: str) -> SQLiteBatchWriter:
    key = str(Path(log_dir).resolve())
    if key in _writers:
        return _writers[key]
    db_path = Path(key) / 'processed_history.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    w = SQLiteBatchWriter(str(db_path))
    _writers[key] = w
    return w


def enqueue_persist(record: dict, log_dir: str = 'debug_logs'):
    """Enqueue a record for async persistence to SQLite.

    `log_dir` is the directory where `processed_history.db` will be stored.
    """
    w = _get_writer_for_dir(log_dir)
    w.enqueue(record)


def flush_history_queue(log_dir: Optional[str] = None,
                        timeout: float = 5.0) -> bool:
    """Flush writer(s). If `log_dir` is None flushes all writers."""
    start = time.time()
    if log_dir:
        w = _writers.get(str(Path(log_dir).resolve()))
        if not w:
            return True
        return w.flush(timeout=timeout)

    for w in list(_writers.values()):
        remaining = max(0.0, timeout - (time.time() - start))
        if not w.flush(timeout=remaining):
            return False
    return True


def shutdown_writer(log_dir: Optional[str] = None, timeout: float = 2.0):
    """Shutdown writer(s). If `log_dir` is None shutdowns all writers."""
    if log_dir:
        w = _writers.get(str(Path(log_dir).resolve()))
        if not w:
            return True
        return w.shutdown(timeout=timeout)

    ok = True
    for w in list(_writers.values()):
        ok = ok and w.shutdown(timeout=timeout)
    return ok


@processor(name="persist_history_jsonl", priority=5)
def persist_history_jsonl(path: Path,
                          context: ProcessingContext,
                          log_dir: str = "debug_logs",
                          **kwargs) -> Any:
    """Persist the most recent `context.results` entry (if any) to a DB.

    This kept the original processor name for backward compatibility; it
    enqueues a structured record into the per-directory SQLite writer.
    """
    # Build the record (fallback if context.results missing)
    last = context.results[-1] if context.results else None
    if last and isinstance(last, dict) and 'processor' in last:
        record = last.copy()
    else:
        record = {
            'time': datetime.now().isoformat(sep=' ', timespec='seconds'),
            'path': str(path),
            'note': 'no_result_available'
        }

    enqueue_persist(record, log_dir)
    return {'enqueued': True}


def read_history_rows(log_dir: str, limit: Optional[int] = None):
    """Read rows from the per-directory history DB and return list of dicts.

    The `raw` column is parsed as JSON and returned under key `raw` as a dict.
    """
    db = Path(log_dir) / 'processed_history.db'
    if not db.exists():
        return []
    conn = sqlite3.connect(str(db))
    try:
        cur = conn.cursor()
        sql = "SELECT id, ts, path, processor, phase, status, cfg, result, error, raw FROM processed_history ORDER BY id"
        if limit:
            sql = sql + f" LIMIT {int(limit)}"
        cur.execute(sql)
        rows = cur.fetchall()
        cols = ['id', 'ts', 'path', 'processor', 'phase', 'status', 'cfg', 'result', 'error', 'raw']
        out = []
        for r in rows:
            d = dict(zip(cols, r))
            try:
                d['raw'] = json.loads(d.get('raw') or '{}')
            except Exception:
                d['raw'] = d.get('raw')
            out.append(d)
        return out
    finally:
        conn.close()


@processor(name="persist_history_sqlite", priority=5)
def persist_history_sqlite(path: Path, context: ProcessingContext, log_dir: str = "debug_logs", **kwargs) -> Any:
    """Persist the most recent `context.results` entry (if any) to SQLite.

    This is the preferred name for the built-in persistence processor.
    The old `persist_history_jsonl` name remains as an alias for compatibility.
    """
    last = context.results[-1] if context.results else None
    if last and isinstance(last, dict) and 'processor' in last:
        record = last.copy()
    else:
        record = {
            'time': datetime.now().isoformat(sep=' ', timespec='seconds'),
            'path': str(path),
            'note': 'no_result_available'
        }
    enqueue_persist(record, log_dir)
    return {'enqueued': True}
