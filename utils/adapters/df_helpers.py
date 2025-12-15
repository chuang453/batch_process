#此文件中的函数用于辅助dataframe的相关操作

import pandas as pd
from typing import Any, Dict, List, Tuple
import numpy as np


def filter_dataframe(df: pd.DataFrame,
                     conditions: Dict[str, Any],
                     combine: str = 'and',
                     inplace: bool = False) -> pd.DataFrame:
    """Filter rows of a DataFrame by a mapping of column -> condition.

    - `conditions` is a dict where keys are column names and values are:
      - a callable accepting a Series and returning a boolean Series (per-row mask),
      - a single value (exact match),
      - an iterable (list/tuple/set) meaning membership (`isin`),
      - or `None` to match missing values.
    - `combine` is 'and' (default) or 'or' to combine per-column masks.
    - If `inplace` is True the original `df` will be filtered in-place and
      returned; otherwise a new filtered DataFrame is returned.

    Examples:
      res = filter_dataframe(df, {'A': lambda s: s > 0, 'B': 'x'})
      res = filter_dataframe(df, {'tag': ['a','b']}, combine='or')
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must be a pandas DataFrame')
    if not isinstance(conditions, dict):
        raise TypeError('conditions must be a dict')

    if combine not in ('and', 'or'):
        raise ValueError("combine must be 'and' or 'or'")

    mask = pd.Series(True, index=df.index)

    for col, cond in conditions.items():
        if col not in df.columns:
            raise KeyError(f"column '{col}' not found in DataFrame")

        series = df[col]

        if callable(cond):
            col_mask = cond(series)
            # allow callable to return scalar bool or boolean Series
            if isinstance(col_mask, (bool, )):
                col_mask = pd.Series(bool(col_mask), index=df.index)
            else:
                col_mask = pd.Series(col_mask, index=df.index)
        else:
            if cond is None:
                col_mask = series.isna()
            elif isinstance(cond, (list, tuple, set)):
                col_mask = series.isin(cond)
            else:
                col_mask = series == cond

        # ensure boolean dtype and align
        col_mask = col_mask.fillna(False).astype(bool)

        if combine == 'and':
            mask &= col_mask
        else:
            mask |= col_mask

    if inplace:
        # mutate original df
        keep_idx = df.index[mask]
        drop_idx = df.index.difference(keep_idx)
        if not drop_idx.empty:
            df.drop(index=drop_idx, inplace=True)
        return df
    else:
        return df.loc[mask].copy()


def split_dataframe_by_groups(
        df: pd.DataFrame,
        group_cols: List[str]) -> List[Tuple[Dict[str, Any], pd.DataFrame]]:
    """按给定的分组列顺序将 DataFrame 拆分为多个分组。

        返回一个列表，列表中每个元素为 `(group_key_dict, group_df)`，其中
        - `group_key_dict` 是 {分组列名: 分组值} 的字典；
        - `group_df` 是该分组对应的 DataFrame 副本，去掉了分组列。

        例子：
          res = split_dataframe_by_groups(df, ['country','year'])

        注意：如果 `group_cols` 为空，返回列表包含单个元素：空字典和原始 DataFrame 的副本。
        """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must be a pandas DataFrame')
    if not isinstance(group_cols, (list, tuple)):
        raise TypeError('group_cols must be a list or tuple of column names')

    group_cols = list(group_cols)

    for col in group_cols:
        if col not in df.columns:
            raise KeyError(f"column '{col}' not found in DataFrame")

    # 如果没有分组列，返回整个表作为单个分组
    if len(group_cols) == 0:
        return [({}, df.copy())]

    result: List[Tuple[Dict[str, Any], pd.DataFrame]] = []

    # 使用 dropna=False 保持包含 NaN 的组（pandas 版本兼容）
    grouped = df.groupby(group_cols, dropna=False)
    for key, group in grouped:
        # key 在单列分组时是标量，多个列时为 tuple
        if isinstance(key, tuple):
            key_vals = key
        else:
            key_vals = (key, )

        group_key = dict(zip(group_cols, key_vals))
        group_df = group.drop(columns=group_cols).copy()
        result.append((group_key, group_df))

    return result


def prepend_dict_columns(df: pd.DataFrame,
                         kv: Dict[str, Any],
                         inplace: bool = False) -> pd.DataFrame:
    """在 DataFrame 前部添加由字典指定的列。

    - `kv` 是要添加的列映射 {col_name: value}。
      - 如果 `value` 是标量，则对所有行重复该值；
      - 如果 `value` 是序列（list/tuple/np.ndarray/pd.Series），其长度必须等于 `len(df)`，
        将按行对应填充；
    - 返回的 DataFrame 列顺序保证字典中的键位于最前面，原有其它列按原顺序跟随；
    - `inplace=True` 会在原 `df` 上修改并返回它；否则返回一个新的 DataFrame 副本。

    示例:
      df2 = prepend_dict_columns(df, {'country':'CN', 'year':2020})
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must be a pandas DataFrame')
    if not isinstance(kv, dict):
        raise TypeError('kv must be a dict')

    n = len(df)

    # Prepare a working copy when not inplace
    target_df = df if inplace else df.copy()

    for col, val in kv.items():
        # sequence-like values (excluding strings) are expanded per-row
        if isinstance(val, (list, tuple, np.ndarray,
                            pd.Series)) and not isinstance(val, (str, bytes)):
            series = pd.Series(val)
            if len(series) != n:
                raise ValueError(
                    f"length of value for column '{col}' != number of rows ({len(series)} != {n})"
                )
            series.index = target_df.index
            target_df[col] = series
        else:
            # scalar or string: broadcast
            target_df[col] = val

    # Reorder columns so that kv.keys() are first (preserve order of existing cols afterwards)
    new_front = [c for c in kv.keys()]
    rest = [c for c in target_df.columns if c not in new_front]
    ordered = new_front + rest

    if inplace:
        # reindex columns in-place by assignment
        target_df = target_df.reindex(columns=ordered)
        return target_df
    else:
        return target_df.reindex(columns=ordered)
