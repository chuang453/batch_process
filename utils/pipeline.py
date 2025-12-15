"""Pipeline helper functions and adapters.

This module provides the pipeline-facing helpers that build a unified
`table_ref` and adapt calls to the pluggable backend (see
`utils.table_backend.DEFAULT_BACKEND`). The pipeline is responsible for
resolving path parts, mapping them to category columns, and preserving the
original `metadata` returned by `write_data_to_dataset`.

Backends are independent components; they accept either a unified
`table_ref` or a plain `table_keys` dict/None. `extra_dict` passed to backends
carries metadata and any sequence-valued per-row columns (backend normalizes
these into table rows and metadata).
"""

from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple
import fnmatch
import re
import json
from abc import ABC, abstractmethod
import warnings

from utils import nested_dicts as nd
from deprecated.table_backend import DEFAULT_BACKEND as _DEFAULT_BACKEND

# --- Internal helpers (generalized operations that work on an in-memory root dict)


# ---------------- Backend adapter (pipeline -> backend) ----------------
def write_to_backend(dataname: str,
                     extra_col,
                     table,
                     matadata=None) -> Dict[str, Any]:
    """Adapter that writes to the configured backend.

    `table_ref` is the unified reference (see pipeline functions which build it).
    This adapter exists so the pipeline functions call a single, testable
    entrypoint rather than depending on backend internals.
    """
    # Forward the provided selector directly to the backend. Backends are
    # expected to handle either a unified `table_ref` dict or a plain
    # `table_keys` dict/None; doing this keeps the adapter thin and avoids
    # mutating caller data. The adapter does not treat any key specially.
    return _DEFAULT_BACKEND.write_table(dataname, extra_col, table, matadata)


def read_from_backend(dataname: str,
                      table_ref,
                      sep: str = "_") -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Adapter that reads from the configured backend.

    Accepts either a unified `table_ref` (dict) or a plain `table_keys` dict/None.
    This adapter forwards the provided selector directly to the backend
    `get_tables` implementation and returns the resulting `(data_map,
    metadata_map)` pair. The adapter does not synthesize or strip any keys.
    """
    # Simply forward the selector to the backend; do not mutate or pop keys.
    data_map, metadata_map = _DEFAULT_BACKEND.get_tables(dataname,
                                                         table_ref,
                                                         sep=sep)
    return data_map, metadata_map


# NOTE: concrete backend implementations (TableBackend, InMemoryTableBackend)
# were moved to `utils.table_backend` to provide a standalone, reusable
# backend component. Use the imported `_DEFAULT_BACKEND` from that module.


def ensure_dir(path: Path) -> Path:
    """Create directory if missing and return the Path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_bucket(context, key: Any, default: Optional[Any] = None):
    """Flexible bucket accessor.

    Supports two styles:
    - Nested data path (list): get/set in context.data via setdefault.
    - Simple string name: creates/returns a dict under shared ['pipeline','buckets', name].
    """
    if isinstance(key, list):
        return context.setdefault_data(key,
                                       default if default is not None else {})
    # simple named bucket under shared
    buckets = context.setdefault_shared(["pipeline", "buckets"], {})
    return buckets.setdefault(str(key),
                              default if isinstance(default, dict) else {})


def append_numbers(context, key_parts: List[str], values: List[float]) -> int:
    """Append numeric values to a list stored at nested key_parts in context.data."""
    bucket: List[float] = context.setdefault_data(key_parts, [])
    bucket.extend(values)
    return len(values)


def set_output(context, *args, **kwargs):
    """Record an output artifact.

    Backward compatible signatures:
    - set_output(context, component, key, value, version='v1')
    - set_output(context, key, value, version='v1')  (simpler form)
    """
    version = kwargs.get("version", "v1")
    if len(args) == 4:  # component, key, value, version? (legacy explicit)
        component, key, value, version = args
        context.set_shared(["pipeline", component, version, "outputs", key],
                           value)
    elif len(args) == 3:
        component, key, value = args
        context.set_shared(["pipeline", component, version, "outputs", key],
                           value)
    elif len(args) == 2:
        key, value = args
        context.set_shared(["pipeline", "outputs", version, key], value)
    else:
        raise TypeError(
            "set_output expects (component,key,value) or (key,value)")


def get_output(context, *args, **kwargs):
    """Retrieve recorded output.

    Backward compatible signatures:
    - get_output(context, component, key, default=None, version='v1')
    - get_output(context, key, default=None, version='v1')
    """
    version = kwargs.get("version", "v1")
    default = kwargs.get("default", None)
    if len(args) == 4:  # legacy explicit
        component, key, default, version = args
        return context.get_shared(
            ["pipeline", component, version, "outputs", key], default)
    elif len(args) == 3:
        component, key, default = args
        return context.get_shared(
            ["pipeline", component, version, "outputs", key], default)
    elif len(args) == 2:
        key, = args[:1]
        # second arg might be default
        if isinstance(args[1], (str, Path, list, dict, int, float)):
            default = args[1]
        key = args[0]
        return context.get_shared(["pipeline", "outputs", version, key],
                                  default)
    elif len(args) == 1:
        key = args[0]
        return context.get_shared(["pipeline", "outputs", version, key],
                                  default)
    else:
        raise TypeError(
            "get_output expects (component,key[,default]) or (key[,default])")


def get_or_create_doc(context, doc_path: str):
    """Record document path (actual creation handled by adapter)."""
    doc_path_p = Path(doc_path)
    context.set_shared(["pipeline", "doc", "path"], str(doc_path_p))
    return doc_path_p


def _resolve_path_to_labels(context, path) -> List[str]:
    """Resolve an input `path` to a list of label parts.

    Supported inputs:
    - list/tuple: treated as already label parts and returned as strings
    - Path or str that matches a recorded path in `context.data['labels']`:
        returns the stored labels list for that path
    - str that matches a label value stored under any recorded path: returns
        the full labels list for the first matching path (sorted)
    - otherwise: returns `[str(path)]` as a single-part list
    """
    # already a sequence of labels
    if isinstance(path, (list, tuple)):
        return [str(p) for p in path]

    pstr = str(path)
    # try to get labels recorded for this exact path
    try:
        labels = context.get_data(['labels', pstr])
    except Exception:
        labels = None
    if labels:
        return [str(x) for x in labels]

    # If no labels found in context, attempt to return the path parts
    # relative to the recorded `root` in context (preferred). Emit a
    # warning so callers know to populate `labels` if they want stable
    # mappings. Fall back to splitting the path into its parts.
    try:
        from pathlib import Path as _Path
        p = _Path(pstr)
        # attempt to get context root
        try:
            root_val = context.get_data(['root'])
        except Exception:
            try:
                root_val = context.get_shared(['root'])
            except Exception:
                root_val = None

        parts = None
        if root_val:
            try:
                root_p = _Path(root_val)
                rel = p.resolve().relative_to(root_p.resolve())
                parts = [str(x) for x in rel.parts]
            except Exception:
                # if relative_to fails, just use full parts
                parts = [str(x) for x in p.parts]
        else:
            parts = [str(x) for x in p.parts]

        warnings.warn(
            f"labels for path '{pstr}' not found in context; using path parts {parts} as labels",
            UserWarning)
        return parts
    except Exception:
        # fallback: treat as literal single-part
        return [pstr]

    # search all recorded labels for a matching label value
    # try:
    #     all_labels = context.get_data(['labels'], {}) or {}
    # except Exception:
    #     all_labels = {}

    # for path_key in sorted(all_labels.keys()):
    #     lab = all_labels.get(path_key)
    #     if not lab:
    #         continue
    #     # if pstr matches the entire labels sequence or a suffix element
    #     if isinstance(lab, (list, tuple)):
    #         if ''.join(lab) == pstr or pstr in lab or lab == [pstr]:
    #             return [str(x) for x in lab]

    # fallback: treat as literal single-part


# plotting moved to adapters/plot_helpers.py

# IO helpers moved to utils/io_helpers.py


# ---------------- Reporting helpers ----------------
def record_result(context, status: str, message: str, **extra):
    """Simplified result recorder used by helper examples.

    Adds an entry with status + message and any extra metadata.
    Does not interfere with engine's own structured result entries.
    """
    entry = {"status": status, "message": message}
    entry.update(extra)
    context.add_result(entry)


def set_config(context,
               component: str,
               key: str,
               value: Any,
               version: str = "v1"):
    context.set_shared(["pipeline", component, version, "config", key], value)


def get_config(context,
               component: str,
               key: str,
               default: Any = None,
               version: str = "v1"):
    return context.get_shared(["pipeline", component, version, "config", key],
                              default)


def write_data_to_dataset(dataname: str,
                          path,
                          table_keys,
                          context,
                          data: List[List[Any]],
                          metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """Store a 2D table and its metadata via the pipeline adapter.
    This function builds a unified set of extra columns (a dict) from the
    provided `path` and `table_keys`, and forwards the data to the configured
    backend using `write_to_backend`. Note: this function does NOT inject a
    reserved `"__path__"` key by default. Backends that accept an explicit
    `table_ref` containing `"__path__"` should be called by the caller when
    that behavior is required. Storage semantics (pipeline ↔ backend contract):
    """
    # basic validation
    if not isinstance(dataname, str):
        raise TypeError("dataname must be a string")

    if metadata is None:
        metadata = {}

    path_dict: Dict[str, Any] = {}

    # helper to safely read context data
    def _ctx_get(key_path):
        try:
            return context.get_data(key_path)
        except Exception:
            try:
                return context.get_shared(key_path)
            except Exception:
                return None

    ## 处理path参数
    if isinstance(path, dict):
        path_dict = {str(k): v for k, v in path.items()}
    else:
        # try to convert Path/str to relative parts against context root
        parts = None
        from pathlib import Path as _Path
        try:
            if isinstance(path, (_Path, )) or isinstance(path, str):
                p = _Path(path)
                root_val = _ctx_get(['root'])
                if root_val:
                    try:
                        root_p = _Path(root_val)
                        rel = p.resolve().relative_to(root_p.resolve())
                        parts = [str(x) for x in rel.parts]
                    except Exception:
                        parts = _resolve_path_to_labels(context, path)
                else:
                    parts = _resolve_path_to_labels(context, path)
            elif isinstance(path, (list, tuple)):
                parts = [str(x) for x in path]
            else:
                parts = _resolve_path_to_labels(context, path)
        except Exception:
            parts = _resolve_path_to_labels(context, path)

        # map parts to categories if available
        cats = None
        try:
            cats = _ctx_get(['categories'])
        except Exception:
            cats = None

        if parts:
            if isinstance(cats, (list, tuple)) and any(
                    isinstance(x, str) for x in cats):
                # align tail of categories to parts
                if len(parts) <= len(cats):
                    sel = cats[-len(parts):]
                else:
                    sel = cats
                path_dict = dict(
                    zip([str(x) for x in sel], [str(x) for x in parts]))
            else:
                n = len(parts)
                default_cats = [f"level{i}" for i in range(n)]
                warnings.warn(
                    f"context ['categories'] missing; using default categories {default_cats} for path {parts}",
                    UserWarning)
                path_dict = dict(zip(default_cats, [str(x) for x in parts]))
        else:
            path_dict = {}

    # validate table_keys
    if table_keys is not None and not isinstance(table_keys, dict):
        raise TypeError("table_keys must be a dict or None")

    # build unified table_ref with path and table_keys
    extra_cols: Dict[str, Any] = {}
    extra_cols.update(path_dict)
    if table_keys:
        for k, v in table_keys.items():
            # do not overwrite path-derived columns
            extra_cols.setdefault(str(k), v)

    # Normalize empty parts to None so backends treat missing paths as the
    # default storage location instead of an empty-list special case.
    if 'parts' in locals() and not parts:
        parts = None

    write_to_backend(dataname, extra_cols, data, metadata)
    return metadata


def get_data_from(dataname: str,
                  path,
                  table_keys=None,
                  sep: str = "_",
                  context=None) -> Tuple[Dict[str, Any], Dict[str, Any]]:

    if context is None:
        raise ValueError("context is required for get_data_from")

    if not isinstance(dataname, str):
        raise TypeError("dataname must be a string")

    # Handle path selectors: path may be a dict of column-matchers or a
    # path-like value (list/Path/str).
    path_selector = None
    if isinstance(path, dict):
        # use as a selector mapping column name -> matcher
        path_selector = {str(k): v for k, v in path.items()}
        path_parts = None
    else:
        path_parts = _resolve_path_to_labels(
            context,
            path) if not isinstance(path,
                                    (list, tuple)) else [str(p) for p in path]

    # Merge selectors: path_selector (if dict) + table_keys (if provided).
    combined_selector = {}
    if path_selector:
        combined_selector.update(path_selector)
    if table_keys is not None:
        if not isinstance(table_keys, dict):
            raise TypeError("table_keys must be a dict or None")
        # table_keys take precedence on collisions
        combined_selector.update(table_keys)

    final_table_keys = combined_selector if combined_selector else None

    # build selector using merged selectors (do not inject any '__path__')
    selector = final_table_keys if final_table_keys else None

    data_map, metadata_map = read_from_backend(dataname, selector, sep=sep)

    # determine the preferred path column name from context (backwards-compatible)
    path_col = "path"
    try:
        cat = context.get_data(["categories"]) if hasattr(context,
                                                          "get_data") else None
        if isinstance(cat, str):
            path_col = cat
        elif isinstance(cat, (list, tuple)) and cat:
            if all(isinstance(x, str) for x in cat):
                path_col = cat[-1]
    except Exception:
        path_col = "path"

    # If adapter provided explicit path_parts (it will for pipeline callers),
    # inject a standardized path column entry for each discovered table prefix.
    updated_meta = dict(metadata_map or {})
    if path_parts:
        path_value = "/".join([str(p) for p in path_parts])
        # discover table prefixes from existing flattened keys
        prefixes = set()
        for k in list(data_map.keys()) + list(metadata_map.keys()):
            if isinstance(k, str) and sep in k:
                prefixes.add(k.split(sep, 1)[0])
        # if no prefixes found, use a default prefix
        if not prefixes:
            prefixes = {"default"}
        for pfx in prefixes:
            updated_meta[f"{pfx}{sep}{path_col}"] = path_value

    return data_map, updated_meta
