"""Utilities for resolving processor parameters from kwargs and context.shared."""

from typing import Any, Dict, Iterable

_SENTINEL = object()


def resolve_param(name: str,
                  kwargs: Dict[str, Any],
                  context,
                  namespace: str = None,
                  default: Any = None) -> Any:
    """Resolve a parameter with precedence: kwargs -> shared namespace -> default."""
    if kwargs is None:
        kwargs = {}

    val = kwargs.get(name, _SENTINEL)
    if val is not _SENTINEL:
        return val

    if namespace and context is not None:
        shared_val = context.get_shared([namespace, name], _SENTINEL)
        if shared_val is not _SENTINEL:
            return shared_val

    return default


def resolve_params(names: Iterable[str],
                   kwargs: Dict[str, Any],
                   context,
                   namespace: str = None,
                   defaults: Dict[str, Any] = None) -> Dict[str, Any]:
    """Resolve multiple parameters using resolve_param()."""
    defaults = defaults or {}
    return {
        name: resolve_param(name, kwargs, context, namespace,
                            defaults.get(name, None))
        for name in names
    }
