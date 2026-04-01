from __future__ import annotations

from typing import Any, Callable, Dict

import pandas as pd

STAGE2_TRANSFORMS: Dict[str, Callable[..., Any]] = {}


def df_transform(name: str | None = None,
                 vectorized: bool = False,
                 produces_multiple: bool = False):
    def decorator(func: Callable[..., Any]):
        reg_name = str(name or func.__name__)
        setattr(func, "vectorized", bool(vectorized))
        setattr(func, "produces_multiple", bool(produces_multiple))
        STAGE2_TRANSFORMS[reg_name] = func
        return func

    return decorator


def register_transform(name: str, func: Callable[..., Any],
                       vectorized: bool = False,
                       produces_multiple: bool = False) -> None:
    setattr(func, "vectorized", bool(vectorized))
    setattr(func, "produces_multiple", bool(produces_multiple))
    STAGE2_TRANSFORMS[str(name)] = func


def import_legacy_transforms() -> None:
    try:
        from decorators.processor import TRANSFORMS
    except Exception:
        return

    for name, func in TRANSFORMS.items():
        if name not in STAGE2_TRANSFORMS:
            STAGE2_TRANSFORMS[name] = func


def apply_transform(func_name: str,
                    df: pd.DataFrame,
                    context: Any,
                    config: Dict[str, Any] | None = None):
    func = STAGE2_TRANSFORMS.get(func_name)
    if func is None:
        raise KeyError(f"transform not found: {func_name}")

    out = func(df, context, **(config or {}))
    return out


# Import existing registry on module import for backward compatibility.
import_legacy_transforms()
