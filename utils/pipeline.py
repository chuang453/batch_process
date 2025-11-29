from pathlib import Path
from typing import Any, List, Dict, Optional

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
        return context.setdefault_data(key, default if default is not None else {})
    # simple named bucket under shared
    buckets = context.setdefault_shared(["pipeline", "buckets"], {})
    return buckets.setdefault(str(key), default if isinstance(default, dict) else {})

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
        context.set_shared(["pipeline", component, version, "outputs", key], value)
    elif len(args) == 3:
        component, key, value = args
        context.set_shared(["pipeline", component, version, "outputs", key], value)
    elif len(args) == 2:
        key, value = args
        context.set_shared(["pipeline", "outputs", version, key], value)
    else:
        raise TypeError("set_output expects (component,key,value) or (key,value)")

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
        return context.get_shared(["pipeline", component, version, "outputs", key], default)
    elif len(args) == 3:
        component, key, default = args
        return context.get_shared(["pipeline", component, version, "outputs", key], default)
    elif len(args) == 2:
        key, = args[:1]
        # second arg might be default
        if isinstance(args[1], (str, Path, list, dict, int, float)):
            default = args[1]
        key = args[0]
        return context.get_shared(["pipeline", "outputs", version, key], default)
    elif len(args) == 1:
        key = args[0]
        return context.get_shared(["pipeline", "outputs", version, key], default)
    else:
        raise TypeError("get_output expects (component,key[,default]) or (key[,default])")

def get_or_create_doc(context, doc_path: str):
    """Record document path (actual creation handled by adapter)."""
    doc_path_p = Path(doc_path)
    context.set_shared(["pipeline", "doc", "path"], str(doc_path_p))
    return doc_path_p

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

def set_config(context, component: str, key: str, value: Any, version: str = "v1"):
    context.set_shared(["pipeline", component, version, "config", key], value)

def get_config(context, component: str, key: str, default: Any = None, version: str = "v1"):
    return context.get_shared(["pipeline", component, version, "config", key], default)

