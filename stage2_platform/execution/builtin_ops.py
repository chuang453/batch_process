from __future__ import annotations

from typing import Any, Callable, Dict

import pandas as pd


def _op_rename(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.rename(columns=params)


def _op_dropna(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    if isinstance(params, dict):
        return df.dropna(**params)
    return df.dropna()


def _op_filter(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.query(params)


def _op_select(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df[list(params)]


def _op_sort(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    if isinstance(params, dict):
        return df.sort_values(**params)
    return df.sort_values(by=params)


def _op_fillna(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.fillna(params)


def _op_eval(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.eval(params)


def _op_astype(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.astype(params)


def _op_drop(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    if isinstance(params, dict):
        return df.drop(**params)
    return df.drop(columns=list(params))


def _op_head(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.head(int(params))


def _op_tail(df: pd.DataFrame, params: Any) -> pd.DataFrame:
    return df.tail(int(params))


BUILTIN_OPS: Dict[str, Callable[[pd.DataFrame, Any], pd.DataFrame]] = {
    "rename": _op_rename,
    "dropna": _op_dropna,
    "filter": _op_filter,
    "select": _op_select,
    "sort": _op_sort,
    "fillna": _op_fillna,
    "eval": _op_eval,
    "astype": _op_astype,
    "drop": _op_drop,
    "head": _op_head,
    "tail": _op_tail,
}


def register_builtin_op(name: str, func: Callable[[pd.DataFrame, Any], pd.DataFrame]) -> None:
    BUILTIN_OPS[str(name)] = func
