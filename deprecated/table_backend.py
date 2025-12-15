"""Simple pluggable table backend implementations.

This module exposes a minimal in-memory backend implementing the
`write_table` and `get_tables` APIs expected by `utils.pipeline`.

It is intentionally lightweight and designed for tests and demos. The
storage layout serializes `table_keys` to a JSON prefix so callers can
select tables by the same dict shape used when writing.
"""
from typing import Any, Dict, Optional, Tuple
import json
import fnmatch
import re
from threading import Lock


class InMemoryTableBackend:

    def __init__(self):
        # storage: dataname -> prefix -> list of records
        # each record: { 'data': ..., 'metadata': ..., 'table_keys': ..., 'path': ... }
        self._store: Dict[str, Dict[str, list]] = {}
        self._lock = Lock()

    def _serialize_keys(self, table_keys: Optional[dict]) -> str:
        if table_keys is None:
            return "default"
        return json.dumps(table_keys, sort_keys=True, separators=(",", ":"))

    def write_table(self,
                    dataname: str,
                    table_ref_or_keys,
                    table,
                    metadata=None) -> Dict[str, Any]:
        """Write a table into the in-memory store.

		`table_ref_or_keys` may be either a unified `table_ref` (dict possibly
		including `"__path__"`) or a plain `table_keys` dict/None. The function
		stores the table under a JSON-serialized prefix derived from the
		`table_keys` portion.
		"""
        if metadata is None:
            metadata = {}

        # normalize input
        # Accept either a unified `table_ref` (dict) or a plain `table_keys` dict/None.
        # The backend treats all keys uniformly (including any key named
        # '__path__') and serializes the provided keys dict as the prefix used
        # for lookup and grouping. We preserve the original `table_ref` in
        # storage for generality but do not special-case any particular key.
        if isinstance(table_ref_or_keys, dict):
            full_ref = dict(table_ref_or_keys)  # preserve everything
            table_keys = dict(full_ref) if full_ref else None
        else:
            full_ref = None
            table_keys = table_ref_or_keys

        prefix = self._serialize_keys(table_keys)

        with self._lock:
            db = self._store.setdefault(dataname, {})
            bucket = db.setdefault(prefix, [])
            bucket.append({
                'data': table,
                'metadata': metadata,
                'table_keys': table_keys,
                'table_ref': full_ref,
            })

        # return metadata as a convenience (pipeline expects this behavior)
        return metadata

    def _match_value(self, value, selector) -> bool:
        # exact match
        if selector is None:
            return True
        # callable predicate
        if callable(selector):
            try:
                return bool(selector(value))
            except Exception:
                return False
        # list/tuple: membership
        if isinstance(selector, (list, tuple)):
            # If both stored value and selector are sequences, treat as
            # sequence equality (useful for path-like lists). Otherwise
            # treat selector as a membership list.
            if isinstance(value, (list, tuple)):
                return list(value) == list(selector)
            return value in selector
        # string semantics: regex, substring, glob, or exact
        if isinstance(selector, str):
            if selector.startswith('re:'):
                pattern = selector[3:]
                try:
                    return re.search(pattern, str(value)) is not None
                except re.error:
                    return False
            if selector.startswith('in:'):
                return selector[3:] in str(value)
            # glob-like
            if any(ch in selector for ch in ['*', '?', '[']):
                return fnmatch.fnmatch(str(value), selector)
            # fallback exact string match
            return str(value) == selector

        # fallback: equality
        return value == selector

    def get_tables(self,
                   dataname: str,
                   table_ref_or_keys,
                   sep: str = "_") -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Retrieve flattened data_map and metadata_map for matching tables.

		Returns two dicts (data_map, metadata_map) where keys are flattened
		strings starting with the serialized table_keys prefix, followed by
		separators and suffixes that identify stored values. This shape is
		intentionally simple to let `pipeline.get_data_from` discover
		prefixes and inject path columns.
		"""
        # normalize selector
        # If a unified table_ref (dict) is provided, treat it as the set of
        # keys to match against. No key (including '__path__') is treated
        # specially by this backend; callers are free to include any keys
        # they wish in the selector.
        if isinstance(table_ref_or_keys, dict):
            req_keys = dict(table_ref_or_keys) if table_ref_or_keys else None
            req_full_ref = dict(table_ref_or_keys)
        else:
            req_keys = table_ref_or_keys
            req_full_ref = None

        out_data = {}
        out_meta = {}

        db = self._store.get(dataname, {})
        # iterate prefixes
        for prefix, records in db.items():
            # reconstruct stored table_keys by parsing prefix (if not 'default')
            try:
                stored_keys = None if prefix == 'default' else json.loads(
                    prefix)
            except Exception:
                stored_keys = None

            # selection: if req_keys provided, every key in req_keys must match stored
            if req_keys is not None:
                match_all = True
                for k, sel in req_keys.items():
                    stored_val = None
                    if stored_keys and k in stored_keys:
                        stored_val = stored_keys[k]
                    else:
                        match_all = False
                        break
                    if not self._match_value(stored_val, sel):
                        match_all = False
                        break
                if not match_all:
                    continue

            # prefix matches; flatten each record
            for idx, rec in enumerate(records):
                # data key
                data_key = f"{prefix}{sep}data{sep}{idx}"
                out_data[data_key] = rec['data']

                # metadata entries: flatten per-metadata-key
                meta = rec.get('metadata') or {}
                for mk, mv in meta.items():
                    meta_key = f"{prefix}{sep}metadata{sep}{mk}"
                    # if multiple records under same prefix provide same meta key,
                    # prefer the first (tests don't require per-record disambiguation)
                    if meta_key not in out_meta:
                        out_meta[meta_key] = mv

                # preserved full table_ref is available to callers via the
                # flattened metadata map (under the prefix-derived metadata
                # keys). We do not expose or synthesize any special 'path'
                # entry here; callers that want path-like data should include
                # it explicitly in their keys/metadata when writing.

        return out_data, out_meta


# single exported default backend instance
DEFAULT_BACKEND = InMemoryTableBackend()
