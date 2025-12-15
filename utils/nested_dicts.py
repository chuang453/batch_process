"""Nested dict helper utilities.

Provides:
- set_dict_data
- get_dict_data
- setdefault_dict_data
- delete_dict_data
- list_dict_keys

These functions operate on nested dictionaries where a path is represented
either as a single key or a list of keys. They modify dictionaries in-place.
"""
from typing import Any, Dict, List
import json


def set_dict_data(datadict: Dict[Any, Any], keys: Any, value: Any) -> None:
    if not isinstance(keys, list):
        datadict[keys] = value
        return
    if len(keys) == 0:
        raise ValueError("Keys list cannot be empty")

    shared = datadict
    for key in keys[:-1]:
        if key not in shared:
            shared[key] = {}
        if not isinstance(shared[key], dict):
            raise TypeError(
                f"Cannot set nested key '{key}' because it is not a dict")
        shared = shared[key]
    shared[keys[-1]] = value


def get_dict_data(datadict: Dict[Any, Any],
                  keys: Any,
                  default: Any = None) -> Any:
    if not isinstance(keys, list):
        return datadict.get(keys, default)
    if len(keys) == 0:
        return default

    shared = datadict
    try:
        for key in keys:
            if not isinstance(shared, dict) or key not in shared:
                raise KeyError(key)
            shared = shared[key]
        return shared
    except (KeyError, TypeError):
        return default


def setdefault_dict_data(datadict: Dict, keys: Any, default=None):
    if not isinstance(keys, list):
        return datadict.setdefault(keys, default)
    if len(keys) == 0:
        return default

    shared = datadict
    for key in keys[:-1]:
        if key not in shared:
            shared[key] = {}
        if not isinstance(shared[key], dict):
            raise TypeError(
                f"Cannot set nested key '{key}' because it is not a dict")
        shared = shared[key]

    return shared.setdefault(keys[-1], default)


def delete_dict_data(datadict: Dict, keys: Any) -> bool:
    if not isinstance(keys, list):
        return datadict.pop(keys, None) is not None
    if len(keys) == 0:
        return False

    parent = datadict
    parents = []
    for k in keys[:-1]:
        if not isinstance(parent, dict) or k not in parent:
            return False
        parents.append((parent, k))
        parent = parent[k]

    last = keys[-1]
    if not isinstance(parent, dict) or last not in parent:
        return False

    parent.pop(last, None)
    for p, k in reversed(parents):
        if isinstance(p.get(k), dict) and not p[k]:
            p.pop(k, None)
        else:
            break
    return True


def list_dict_keys(datadict: Dict,
                   prefix: List[str] = None) -> List[List[str]]:
    ns = datadict
    if prefix:
        ns = get_dict_data(datadict, prefix, {})

    paths: List[List[str]] = []

    def walk(node: Any, base: List[str]):
        if isinstance(node, dict):
            if not node:
                paths.append(base)
            else:
                for k, v in node.items():
                    walk(v, base + [str(k)])
        else:
            paths.append(base)

    walk(ns, prefix or [])
    return paths


def flatten_dict(datadict: Dict,
                 parent: tuple = (),
                 serializer: str = "tuple",
                 sep: str = ".") -> Dict:
    """Flatten a nested dict into a mapping of path-key -> value.

    serializer: 'tuple' (default) returns tuple keys; 'json' returns JSON
    strings of the path list; 'sep' returns sep-joined strings.

    Examples (serializer='tuple'):
        {'a': {'b': 1, 'c': {'d': 2}}, 'x': {}} -> {
            ('a','b'): 1,
            ('a','c','d'): 2,
            ('x',): {}
        }
    """

    def _encode_key(path: tuple):
        if serializer == "tuple":
            return path
        if serializer == "json":
            return json.dumps(list(path), ensure_ascii=False)
        if serializer == "sep":
            return sep.join(path)
        raise ValueError(f"Unknown serializer: {serializer}")

    out: Dict = {}

    def _walk(node: Any, path: tuple):
        if isinstance(node, dict):
            if not node:
                out[_encode_key(path)] = {}
            else:
                for k, v in node.items():
                    _walk(v, path + (str(k), ))
        else:
            out[_encode_key(path)] = node

    _walk(datadict, parent)
    return out


def unflatten_dict(flatmap: Dict,
                   serializer: str = "auto",
                   sep: str = ".") -> Dict:
    """Reconstruct a nested dict from a flattened mapping.

    If serializer == 'auto', the function will try to infer key format:
      - tuple/list keys are used directly
      - string keys that decode as JSON lists will be used as lists
      - otherwise strings containing `sep` are split on `sep`
      - otherwise the whole string is used as a single key
    If serializer is specified ('tuple', 'json', 'sep'), keys are decoded
    accordingly and type errors raised on mismatches.
    """
    root: Dict = {}
    for key, value in flatmap.items():
        # decode key to path tuple
        if serializer == "auto":
            if isinstance(key, (list, tuple)):
                path = tuple(str(x) for x in key)
            elif isinstance(key, str):
                try:
                    v = json.loads(key)
                    if isinstance(v, list):
                        path = tuple(str(x) for x in v)
                    else:
                        if sep in key:
                            path = tuple(key.split(sep))
                        else:
                            path = (key, )
                except Exception:
                    if sep in key:
                        path = tuple(key.split(sep))
                    else:
                        path = (key, )
            else:
                raise TypeError(
                    "flatten map keys must be tuples/lists or strings")
        elif serializer == "tuple":
            if not isinstance(key, (list, tuple)):
                raise TypeError(
                    "Expected tuple/list keys for serializer='tuple'")
            path = tuple(str(x) for x in key)
        elif serializer == "json":
            if not isinstance(key, str):
                raise TypeError(
                    "Expected string keys (JSON) for serializer='json'")
            v = json.loads(key)
            if not isinstance(v, list):
                raise TypeError("JSON key did not decode to list")
            path = tuple(str(x) for x in v)
        elif serializer == "sep":
            if not isinstance(key, str):
                raise TypeError("Expected string keys for serializer='sep'")
            path = tuple(key.split(sep))
        else:
            raise ValueError("Unknown serializer")

        node = root
        for k in path[:-1]:
            if k not in node or not isinstance(node[k], dict):
                node[k] = {}
            node = node[k]
        node[path[-1]] = value
    return root


__all__ = [
    "set_dict_data",
    "get_dict_data",
    "setdefault_dict_data",
    "delete_dict_data",
    "list_dict_keys",
    "flatten_dict",
    "unflatten_dict",
]
