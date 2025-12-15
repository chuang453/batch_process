# utils module — dataset helpers

This document explains two helpers provided in `utils/pipeline.py`: `write_data_to_dataset` and `get_data_from`.

These helpers provide a simple in-memory dataset registry (stored under `context.shared['dataset']`) and flexible retrieval/filtering primitives. They are intended for processors that need to record per-file or per-path tabular output and later aggregate or query those tables.

## write_data_to_dataset

Signature

```
write_data_to_dataset(dataname: str,
                      path,
                      table_keys,
                      context,
                      data: List[List[Any]],
                      metadata: Dict[str, Any] = None) -> Dict[str, Any]
```

Description

- Stores a table (2D list `data`) and its associated `metadata` under `context.shared['dataset'][dataname]`.
- Storage layout:

```
shared['dataset'][dataname][<label part 0>][<label part 1>]...[<table_key>] = {
    'data': [[...], ...],
    'metadata': {...}
}
```

Parameters

- `dataname` (str): top-level dataset name.
- `path`: may be one of:
  - a list/tuple of label parts (used directly),
  - a string path that is registered in `context.data['labels']` (it will be resolved to the recorded label list),
  - a simple label string (the function will attempt to resolve it to a full label list if recorded; otherwise it is treated as a single-part label).
- `table_keys`: a `dict` representing the table identifier, or `None` to select the
    default table. When a `dict` is supplied it will be serialized to a stable JSON
    string (the internal `table_id`) for storage and the original dict will be kept
    under the private node `_table_key_dict` for that table. Note: this API is
    intentionally strict — `table_keys` must be `dict` or `None` (strings/lists are
    no longer accepted). See the migration note below.
- `context`: the ProcessingContext providing `setdefault_shared` used to store the nested dicts.
- `data`: 2D list (rows) representing the table.
- `metadata`: dictionary describing the table (e.g. `{'columns': [...], 'units': [...]}`)

Returns

- The function returns the `metadata` dict that was stored (convenience return).

Notes

- This function currently writes to the in-memory nested dict under `context.shared`. It does not persist to disk unless you add a persistence adapter.
- `path` label resolution uses `_resolve_path_to_labels(context, path)` found in `utils/pipeline.py`.

Example

```python
write_data_to_dataset('mydata', ['/exp','run1'], 't1', context, [[1,2],[3,4]], {'columns':['a','b']})
```

---

## get_data_from

Signature

```
get_data_from(dataname: str,
              path,
              table_keys=None,
              sep: str = "_",
              context=None) -> Tuple[Dict[str, Any], Dict[str, Any]]
```

Description

- Retrieves tables under `shared['dataset'][dataname]` for the resolved `path` and filters them by `table_keys`.
- Returns a tuple `(data_map, metadata_map)` where both are flattened dicts. Keys are strings joined by `sep` and are prefixed by the table key.

Parameters

- `dataname` (str): top-level dataset name.
- `path`: same resolution rules as `write_data_to_dataset` (list/tuple, registered path, or label name).
- `table_keys`: controls which tables to include. Supported forms (strict):
    - `None` (default): include all tables under the resolved path.
    - `dict`: a structured selector. The selector's keys are matched against the
        stored `_table_key_dict` or against fields in the table `metadata`. Selector
        values may be exact values, glob patterns (`*`/`?`), regex (`re:` / `regex:`),
        substring (`in:`), lists/tuples (membership) or callables (predicate on the
        actual metadata value). Example selectors: `{'name': 't1'}`,
        `{'name': 're:^t[0-9]+$'}`, `{'sample': 'in:A'}`, `{'name': lambda v: v.endswith('1')}`.
- `sep`: separator used when flattening keys (default `_`).
 - `sep`: separator used when flattening keys (default `_`).

Notes on dict-style `table_keys`:

- When writing, passing a `dict` will cause the dict to be serialized to a stable
    JSON `table_id` (used as the dict key under `shared['dataset'][dataname][...path...]`).
- The original dict is stored under `_table_key_dict` inside the table node and is
    available for exact dict-based selection when calling `get_data_from`.
- `get_data_from` accepts a dict selector; it will match tables whose `_table_key_dict`
    equals the provided dict (or whose serialized `table_id` equals the serialized selector).
- `context`: ProcessingContext providing `get_shared`.

Return value

- `data_map`: flattened mapping of table data. Keys are strings like `<table_key><sep>...` as produced by `utils.nested_dicts.flatten_dict` with `serializer='sep'`.
- `metadata_map`: flattened mapping of metadata for matched tables (same key prefix behavior).

Examples

```python
# get all tables under /data/run1
data_map, meta_map = get_data_from('mydata', '/data/run1', table_keys=None, context=context)

# exact single table (selector matched against stored _table_key_dict or metadata)
data_map, meta_map = get_data_from('mydata', ['/exp','run1'], table_keys={'name': 't1'}, context=context)

# glob pattern on selector value
data_map, meta_map = get_data_from('mydata', 'run1', table_keys={'name': 't*'}, context=context)

# regex selector value
data_map, meta_map = get_data_from('mydata', 'run1', table_keys={'name': 're:^t[0-9]+$'}, context=context)

# callable selector value
pred = lambda v: v.endswith('summary')
data_map, meta_map = get_data_from('mydata', 'run1', table_keys={'name': pred}, context=context)
```

Converting `data_map` to a DataFrame

`data_map` is a flattened dict. If you want a tabular `pandas.DataFrame`, you can convert per-table data manually. Example (simple):

```python
import pandas as pd

data_map, meta_map = get_data_from('mydata', ['exp','run1'], table_keys='t*', context=context)

# Group keys by table prefix
from collections import defaultdict
groups = defaultdict(dict)
for flat_key, value in data_map.items():
    tbl, rest = flat_key.split(sep, 1) if sep in flat_key else (flat_key, '')
    groups[tbl][rest] = value

# Convert one table to rows when stored as data_{row}_{col}
def table_from_flat(flat):
    # naive extraction assuming keys like 'data_0_1' -> row 0 col 1
    rows = {}
    for fk, v in flat.items():
        if fk.startswith('data' + sep):
            parts = fk.split(sep)
            # parts = ['data','row','col',...]
            if len(parts) >= 3:
                r = int(parts[1]); c = int(parts[2])
                rows.setdefault(r, {})[c] = v
    # build ordered rows
    maxr = max(rows.keys()) if rows else -1
    df_rows = []
    for r in range(maxr + 1):
        cols = rows.get(r, {})
        # order columns by numeric key
        row = [cols.get(c) for c in sorted(cols.keys())]
        df_rows.append(row)
    return pd.DataFrame(df_rows)

# build DataFrames for matched tables
dfs = {tbl: table_from_flat(flat) for tbl, flat in groups.items()}
```

Notes and suggestions

- The `utils` helpers purposely store a generic nested-dict to keep dependencies minimal. For larger datasets consider a persistence backend (SQLite or Parquet) and storing a single canonical DataFrame per `dataname` (with `path` / `table_key` as columns). See the project discussion notes for options.
- `table_keys` filtering supports flexible patterns; prefer exact or glob for predictable results. Regex and substring are available for advanced cases.

Migration note

- This module now requires `table_keys` to be a `dict` (or `None`). If you
    previously called `write_data_to_dataset(..., table_keys='t1', ...)`, migrate by
    switching to a small dict selector and optionally adding the same value to the
    table `metadata` so it can be matched later. Example:

```python
# before
write_data_to_dataset('mydata', path, 't1', context, data, {'columns':[... ]})

# after
write_data_to_dataset('mydata', path, {'name':'t1'}, context, data, {'columns':[...], 'name':'t1'})
```

If you prefer backward-compatible behavior (accepting strings/lists), the code can
be adjusted to auto-wrap non-dict `table_keys` into a dict (for example
`{'name': '<value>'}`) — tell me if you'd like that compatibility layer added.

If you want, I can add a convenience `read_dataset_as_dataframe(...)` helper that returns a `pandas.DataFrame` directly (optionally persisting to SQLite). Ask and I will implement it.
