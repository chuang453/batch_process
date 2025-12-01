from datetime import datetime
import json
from pathlib import Path
from typing import Any

from decorators.processor import processor, ProcessingContext


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


@processor(name="persist_history_jsonl", priority=5)
def persist_history_jsonl(path: Path,
                          context: ProcessingContext,
                          log_dir: str = "debug_logs",
                          **kwargs) -> Any:
    """Persist the most recent `context.results` entry (if any) to a JSONL file.

    Use as a post-processor to write structured history for external tools.
    """
    try:
        last = context.results[-1] if context.results else None
        if last and isinstance(last, dict) and 'processor' in last:
            record = last.copy()
        else:
            # fallback minimal record
            record = {
                'time': datetime.now().isoformat(sep=' ', timespec='seconds'),
                'path': str(path),
                'note': 'no_result_available'
            }

        d = Path(log_dir)
        d.mkdir(parents=True, exist_ok=True)
        fpath = d / 'processed_history.jsonl'
        with open(fpath, 'a', encoding='utf-8') as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + '\n')

        return {'persisted': True, 'file': str(fpath)}
    except Exception as e:
        return {'persisted': False, 'error': str(e)}
