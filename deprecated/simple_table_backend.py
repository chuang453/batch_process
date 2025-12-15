"""A minimal, path-agnostic pure-Python in-memory table backend.

This backend is intentionally simple and does NOT treat any key (including
`__path__`) specially. Table references (table_ref) are treated as an opaque
dict of keys -> values; selectors may match any of those keys, or metadata
keys, or data column values.

API:
- write_table(dataname, table_ref, table, extra_dict=None) -> metadata
- get_tables(dataname, table_ref, sep="_") -> (data_map, metadata_map)
- delete_table(dataname, table_ref) -> bool

Storage model (simple list):
  self._records[dataname] = [ record1, record2, ... ]
where record is:
  {
    'table_ref': {...},            # original dict (may be empty)
    'table_id': <stable-string>,   # serialized table_keys (table_ref minus reserved keys)
    'columns': [...],              # list of column names or []
    'rows': [ [..], [..] ],        # list-of-lists
    'metadata': {...}              # stored metadata
  }

This implementation avoids any special handling for `__path__` — if callers
include `__path__` in `table_ref` it will be treated like any other key.
"""
from typing import Any, Dict, List, Tuple, Optional
import json
import fnmatch
import re

from utils import nested_dicts as nd


class SimpleTableBackend:

    def __init__(self):
        # dataname -> list of records
        self._records: Dict[str, List[Dict[str, Any]]] = {}

    # -- helpers ----------------------------------------------------------------
    def _normalize_table_ref(self, table_ref) -> Dict[str, Any]:
        if table_ref is None:
            return {}
        if isinstance(table_ref, dict):
            return dict(table_ref)
        if isinstance(table_ref, (list, tuple)):
            # compatibility behavior: normalize non-dict refs into a dict with
            # a '__path__' key so callers can continue to pass path-like
            # values. Note: the backend treats this key like any other key.
            return {"__path__": [str(x) for x in table_ref]}
        return {"__path__": [str(table_ref)]}

    def _serialize_table_keys(self, table_keys: Optional[Dict]) -> str:
        if table_keys is None:
            return "default"
        if not isinstance(table_keys, dict):
            raise TypeError("table_keys must be a dict or None")
        try:
            return json.dumps(table_keys,
                              sort_keys=True,
                              separators=(",", ":"))
        except Exception:
            return str(sorted([(k, str(v)) for k, v in table_keys.items()]))

    def _table_to_rows_and_columns(self,
                                   table) -> Tuple[List[List[Any]], List[str]]:
        # Lightweight normalization: dict-of-seqs, list-of-dicts, list-of-lists, scalar
        rows: List[List[Any]] = []
        columns: List[str] = []

        # dict-of-seqs
        if isinstance(table, dict) and all(
                isinstance(v, (list, tuple)) for v in table.values()):
            columns = list(table.keys())
            cols = [table[k] for k in columns]
            rows = [list(r) for r in zip(*cols)] if cols else []
            return rows, columns

        # list-of-dicts
        if isinstance(table, list) and table and isinstance(table[0], dict):
            cols: List[str] = []
            for r in table:
                for k in r.keys():
                    if k not in cols:
                        cols.append(k)
            columns = cols
            for r in table:
                rows.append([r.get(c) for c in columns])
            return rows, columns

        # list-of-lists
        if isinstance(table, list) and (not table
                                        or isinstance(table[0],
                                                      (list, tuple))):
            rows = [list(r) for r in table]
            return rows, columns

        # scalar
        return [[table]], []

    def _val_matches(self, expected, actual) -> bool:
        if callable(expected):
            try:
                return bool(expected(actual))
            except Exception:
                return False
        if isinstance(expected, str):
            if expected.startswith("re:") or expected.startswith("regex:"):
                pat = expected.split(':', 1)[1]
                try:
                    return bool(re.search(pat, str(actual or "")))
                except re.error:
                    return False
            if expected.startswith("in:"):
                sub = expected.split(':', 1)[1]
                return sub in str(actual or "")
            if any(ch in expected for ch in "*?"):
                return fnmatch.fnmatch(str(actual or ""), expected)
            return str(actual) == expected
        if isinstance(expected, (list, tuple, set)):
            return actual in expected
        return expected == actual

    # -- public API -------------------------------------------------------------
    def write_table(self,
                    dataname: str,
                    table_ref,
                    table,
                    extra_dict=None) -> Dict[str, Any]:
        ref = self._normalize_table_ref(table_ref)
        # table_keys are everything except reserved keys; we treat none specially
        tk = dict(ref) if isinstance(ref, dict) else {}
        # do not specially pop '__path__' — keep it as a normal key for serialization
        tk_for_id = dict(tk)
        table_id = self._serialize_table_keys(tk_for_id or None)

        rows, columns = self._table_to_rows_and_columns(table)
        rowcount = len(rows)

        metadata: Dict[str, Any] = {}
        # incorporate extra_dict: per-row sequences -> appended to rows as columns;
        # scalars -> metadata. This mirrors pipeline expectations but keeps backend generic.
        if extra_dict:
            append_cols: List[Tuple[str, List[Any]]] = []
            for k, v in extra_dict.items():
                if k == 'columns':
                    continue
                if isinstance(
                        v,
                    (list, tuple)) and len(v) == rowcount and rowcount > 0:
                    append_cols.append((k, list(v)))
                else:
                    metadata[k] = v
            if append_cols:
                # ensure columns list accounts for appended cols
                for cname, _ in append_cols:
                    if cname not in columns:
                        columns.append(cname)
                # append values to each row
                for idx, r in enumerate(rows):
                    for cname, vals in append_cols:
                        r.append(vals[idx])

        if columns:
            metadata.setdefault('columns', columns)

        rec = {
            'table_ref': ref,
            'table_id': table_id,
            'columns': columns,
            'rows': rows,
            'metadata': metadata,
        }

        self._records.setdefault(dataname, []).append(rec)
        return metadata or {}

    def get_tables(self,
                   dataname: str,
                   table_ref,
                   sep: str = "_") -> Tuple[Dict[str, Any], Dict[str, Any]]:
        selector = table_ref
        data_map: Dict[str, Any] = {}
        metadata_map: Dict[str, Any] = {}

        recs = self._records.get(dataname, [])

        for rec in recs:
            rec_ref = rec.get('table_ref', {})
            rec_meta = rec.get('metadata', {})
            rec_data = [list(r) for r in rec.get('rows', [])]
            cols = rec.get('columns') or []
            table_id = rec.get('table_id')

            match_ok = True

            if selector is None:
                match_ok = True
            elif isinstance(selector, dict):
                for sel_k, sel_v in selector.items():
                    # do not treat '__path__' specially — it is just another key
                    if sel_k in rec_ref:
                        if not self._val_matches(sel_v, rec_ref.get(sel_k)):
                            match_ok = False
                            break
                    elif sel_k in rec_meta:
                        if not self._val_matches(sel_v, rec_meta.get(sel_k)):
                            match_ok = False
                            break
                    elif sel_k in cols:
                        idx = cols.index(sel_k)
                        filtered_rows = []
                        for row in rec_data:
                            try:
                                val = row[idx]
                            except Exception:
                                val = None
                            if self._val_matches(sel_v, val):
                                filtered_rows.append(row)
                        if not filtered_rows:
                            match_ok = False
                            break
                        rec_data = filtered_rows
                    else:
                        # unknown selector key -> non-match
                        match_ok = False
                        break
            else:
                # scalar selector: match exact table_id
                if str(selector) != str(table_id):
                    match_ok = False

            if not match_ok:
                continue

            # if rows filtered out completely, skip
            if not rec_data:
                continue

            table_key = str(table_id)
            flat_data = nd.flatten_dict({'data': rec_data},
                                        parent=(table_key, ),
                                        serializer='sep',
                                        sep=sep)
            flat_meta = nd.flatten_dict({'metadata': rec_meta},
                                        parent=(table_key, ),
                                        serializer='sep',
                                        sep=sep)

            for k, v in flat_data.items():
                data_map[str(k)] = v
            for k, v in flat_meta.items():
                metadata_map[str(k)] = v

        return data_map, metadata_map

    def delete_table(self, dataname: str, table_ref) -> bool:
        ref = self._normalize_table_ref(table_ref)
        tk = dict(ref) if isinstance(ref, dict) else {}
        table_id = self._serialize_table_keys(tk or None)

        recs = self._records.get(dataname, [])
        for i, rec in enumerate(list(recs)):
            if rec.get('table_id') == table_id and rec.get('table_ref') == ref:
                recs.pop(i)
                return True
        return False


# default instance
DEFAULT_SIMPLE_BACKEND = SimpleTableBackend()
