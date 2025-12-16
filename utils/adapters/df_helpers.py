#此文件中的函数用于辅助dataframe的相关操作，并提供一个简单的数据库读写接口
##

import pandas as pd
from typing import Any, Dict, List, Tuple
import numpy as np


def filter_dataframe(df: pd.DataFrame,
                     conditions: Dict[str, Any],
                     combine: str = 'and',
                     inplace: bool = False) -> pd.DataFrame:
    """根据列到条件的映射筛选 DataFrame 行。
    参数说明：
    - `conditions`：字典，键为列名，值为筛选条件：
        - 可调用对象（接受 Series，返回布尔 Series，表示逐行是否匹配）；
        - 单个值（表示精确匹配）；
        - 可迭代（list/tuple/set），表示 membership（等同于 `isin`）；
        - 或者 `None` 表示匹配缺失值（NaN/None）。
    - `combine`：'and'（默认）或 'or'，用于合并各列的布尔掩码。
    - `inplace`：若为 True，在原始 `df` 上就地筛选并返回该对象；否则返回筛选结果的副本。
    示例：
        res = filter_dataframe(df, {'A': lambda s: s > 0, 'B': 'x'})
        res = filter_dataframe(df, {'tag': ['a','b']}, combine='or')
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError('df must be a pandas DataFrame')
    if not isinstance(conditions, dict):
        raise TypeError('conditions must be a dict')

    if combine not in ('and', 'or'):
        raise ValueError("combine must be 'and' or 'or'")

    # For 'and' start with all True; for 'or' start with all False
    if combine == 'and':
        mask = pd.Series(True, index=df.index)
    else:
        mask = pd.Series(False, index=df.index)

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
    """按指定列对 DataFrame 分组并返回逐组数据的列表。
    返回值为列表，每个元素为 `(group_key_dict, group_df)`：
    - `group_key_dict`：字典，键为分组列名，值为该组对应的分组值；
    - `group_df`：该组对应的 DataFrame 副本，已去除分组列。
    示例：
        res = split_dataframe_by_groups(df, ['country','year'])
    说明：当 `group_cols` 为空时，返回单个元素：空字典和原始 DataFrame 的副本。
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

        # normalize missing values (NaN / pd.NA) to Python None for stable keys
        norm_vals = tuple(None if pd.isna(v) else v for v in key_vals)
        group_key = dict(zip(group_cols, norm_vals))
        group_df = group.drop(columns=group_cols).copy()
        result.append((group_key, group_df))

    return result


def prepend_dict_columns(df: pd.DataFrame,
                         kv: Dict[str, Any],
                         inplace: bool = False) -> pd.DataFrame:
    """在 DataFrame 前部插入由字典指定的列。
    参数说明：
    - `kv`：字典，映射新列名到对应的值；
        - 若值为标量，则对所有行广播该值；
        - 若值为序列（list/tuple/ndarray/Series，且非字符串），其长度必须等于 DataFrame 行数；
    - 返回的 DataFrame 保证 `kv` 中键的顺序位于最前面，剩余列按原顺序跟随；
    - `inplace=True`：在原始 `df` 上就地修改并返回该对象；否则返回一个新的副本。
    示例：
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
        # perform true in-place reordering while preserving object identity
        new_df = target_df.reindex(columns=ordered)
        # remove all existing columns from the original df
        orig_cols = list(df.columns)
        if orig_cols:
            df.drop(columns=orig_cols, inplace=True)
        # assign columns back in the requested order
        for col in ordered:
            df[col] = new_df[col].values
        return df
    else:
        return target_df.reindex(columns=ordered)


###=============================  简易数据库接口  =============================###


def write_data_to_database(data_name: str,
                           data: List,
                           col_name: List[str],
                           extra_dict: Dict,
                           context: Dict,
                           meta_data: Dict = None) -> None:
    """将二维列表数据追加到内存数据库中的表（保存在 `context['data']`），并可同时写入表级元数据。

    参数说明：
    - `data_name`：表名，在 `context['data']` 中作为键；
    - `data`：二维列表或元组（按行），每行长度应与 `col_name` 一致；
    - `col_name`：数据列名列表；
    - `extra_dict`：额外列映射（列名 -> 值），值可为标量或与行数等长的序列；
    - `context`：字典，必须包含或将创建 `context['data']`（表名 -> DataFrame 映射）。
        - `meta_data`：可选的字典，将与表名一起存入 `context['meta']`（表名 -> meta dict）。

            支持的 `meta_data` 键（示例与语义）：
            - `table_keys`: 列名列表，用以唯一确定一张“基本表”（base table）的键列。
                - 要求：列表内元素为字符串；列名必须存在于要写入或已存在的表列集合中。
                - 语义：说明哪些列组合起来可以唯一标识一条基表记录（例如 `['id']` 或 `['project','version']`）。

            - `base_col_names`: 基本表的完整列名列表（list of column names）。
                - 要求：为字符串列表；列名必须存在于表的列集合中。
                - 语义：当当前写入为派生/增量表时，`base_col_names` 描述底层基表应包含的所有列。

            - `keys`: 基本表的复合主键（composite primary key）列名列表。
                - 要求：为字符串列表。
                - 验证：如果同时提供 `base_col_names`，`keys` 中的列必须包含于 `base_col_names`；否则 `keys` 中的列必须存在于表的列集合中。
                - 语义：指定用于唯一性约束或主键连接的列顺序（例如 `['project','version','id']`）。

            其它说明与行为：
            - 函数在写入前会进行基本类型与存在性校验（确保这些键为字符串列表且引用的列在当前或已存在表列中可见）；
            - 如果 `meta_data` 与已存在表的元数据存在冲突，新的键值会覆盖已有值（使用字典合并，新的键覆盖旧的键）；
            - `meta_data` 主要用于上层逻辑（例如构建主/从关系、合并判重、或生成索引），本函数仅负责验证与存储。
            - 示例：
                meta_data = {
                        'table_keys': ['project','file'],
                        'base_col_names': ['project','file','line','value'],
                        'keys': ['project','file']
                }

    行为：若表不存在则创建，若表已存在则对齐列并追加新行，缺失列会填充为 NA。
    函数会更新 `context['data'][data_name]`。"""
    # basic validation
    if not isinstance(data_name, str):
        raise TypeError('data_name must be a string')
    if not isinstance(data, (list, tuple)):
        raise TypeError('data must be a list or tuple of rows')
    if not isinstance(col_name, (list, tuple)):
        raise TypeError('col_name must be a list or tuple of column names')
    if not isinstance(extra_dict, dict):
        raise TypeError('extra_dict must be a dict')
    if not isinstance(context, dict):
        raise TypeError('context must be a dict-like object')
    if meta_data is not None and not isinstance(meta_data, dict):
        raise TypeError('meta_data must be a dict if provided')

    # ensure context has a 'data' dict
    db = context.setdefault('data', {})
    if not isinstance(db, dict):
        raise TypeError(
            "context['data'] must be a dict mapping table name -> DataFrame")

    # ensure context has a 'meta' dict for storing per-table metadata
    meta_db = context.setdefault('meta', {})
    if not isinstance(meta_db, dict):
        raise TypeError(
            "context['meta'] must be a dict mapping table name -> meta dict")

    # build DataFrame from provided data
    if len(data) == 0:
        # create empty frame with specified columns (and extra cols)
        new_df = pd.DataFrame(columns=list(col_name) + list(extra_dict.keys()))
    else:
        # verify each row length
        for i, row in enumerate(data):
            if not isinstance(row, (list, tuple)):
                raise TypeError(f'row {i} of data must be list/tuple')
            if len(row) != len(col_name):
                raise ValueError(
                    f'row {i} length ({len(row)}) != len(col_name) ({len(col_name)})'
                )
        new_df = pd.DataFrame(data, columns=list(col_name))

    # attach extra columns: reuse prepend_dict_columns for validation but
    # ensure the final new table keeps `col_name` columns first and extras after
    if extra_dict:
        temp = prepend_dict_columns(new_df.copy(), extra_dict, inplace=False)
        # build ordered columns: data columns first, then any extras
        ordered = list(col_name) + [
            c for c in temp.columns if c not in col_name
        ]
        new_df = temp.reindex(columns=ordered)

    # prepare combined columns (new data + existing table if any) for meta validation
    combined_cols = set(new_df.columns)
    if data_name in db and isinstance(db[data_name], pd.DataFrame):
        combined_cols |= set(db[data_name].columns)

    # validate meta_data keys if provided
    if meta_data is not None:
        # 'table_keys' : list of column names that uniquely identify a base table
        if 'table_keys' in meta_data:
            tk = meta_data['table_keys']
            if not isinstance(tk, (list, tuple)) or not all(
                    isinstance(x, str) for x in tk):
                raise TypeError(
                    "meta_data['table_keys'] must be a list of column name strings"
                )
            for col in tk:
                if col not in combined_cols:
                    raise KeyError(
                        f"meta_data 'table_keys' references unknown column '{col}'"
                    )

        # 'base_col_names' : list of all column names for the base table
        if 'base_col_names' in meta_data:
            bcn = meta_data['base_col_names']
            if not isinstance(bcn, (list, tuple)) or not all(
                    isinstance(x, str) for x in bcn):
                raise TypeError(
                    "meta_data['base_col_names'] must be a list of column name strings"
                )
            for col in bcn:
                if col not in combined_cols:
                    raise KeyError(
                        f"meta_data 'base_col_names' references unknown column '{col}'"
                    )

        # 'keys' : composite primary key list for the base table
        if 'keys' in meta_data:
            ks = meta_data['keys']
            if not isinstance(ks, (list, tuple)) or not all(
                    isinstance(x, str) for x in ks):
                raise TypeError(
                    "meta_data['keys'] must be a list of column name strings")
            # if base_col_names provided, validate against them; otherwise validate against combined_cols
            if 'base_col_names' in meta_data:
                base_set = set(meta_data['base_col_names'])
                for col in ks:
                    if col not in base_set:
                        raise KeyError(
                            f"meta_data 'keys' references column '{col}' not listed in 'base_col_names'"
                        )
            else:
                for col in ks:
                    if col not in combined_cols:
                        raise KeyError(
                            f"meta_data 'keys' references unknown column '{col}'"
                        )

    # if table exists, append; else create
    if data_name in db:
        existing = db[data_name]
        if not isinstance(existing, pd.DataFrame):
            raise TypeError(f"existing table '{data_name}' is not a DataFrame")

        # Align columns: add missing cols with NaN
        for c in existing.columns:
            if c not in new_df.columns:
                new_df[c] = pd.NA
        for c in new_df.columns:
            if c not in existing.columns:
                existing[c] = pd.NA

        # ensure same column order: keep existing order then new columns appended in new_df order
        cols = list(existing.columns)
        combined = pd.concat([existing[cols], new_df[cols]],
                             ignore_index=True,
                             sort=False)
        db[data_name] = combined
    else:
        # create new table; ensure column order: data columns then extra columns
        db[data_name] = new_df

    # update context
    context['data'] = db
    # update metadata if provided: merge into existing metadata for this table
    if meta_data is not None:
        existing_meta = meta_db.get(data_name, {})
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        # update existing with new keys (new values overwrite)
        existing_meta.update(meta_data)
        meta_db[data_name] = existing_meta
    context['meta'] = meta_db


def get_data_from_database(data_name: str,
                           filter_dict: Dict = None,
                           group_keys: List[str] = None,
                           out_col: List[str] = None,
                           context: Dict = None,
                           out_option: str = 'split') -> Any:
    """从内存数据库（`context['data']`）读取数据，可筛选并按需分组返回多种形式。
    参数说明：
    - `data_name`：要读取的表名；
    - `filter_dict`：可选的筛选字典，语义与 `filter_dataframe` 相同；
    - `group_keys`：可选的分组列名列表；若未提供，函数会尝试从 `context['meta'][data_name]['table_keys']`
        推断默认分组键（参见下文）；
    - `out_col`：可选的输出列名列表；若为 None（默认）且在 `context['meta'][data_name]` 中存在
        `base_col_names`，则使用该列表作为默认输出列；否则返回表中所有列。
    关于从元数据推断默认值的规则（当 `context` 中存在 `meta` 时）：
    - 默认 `out_col`：若 `context['meta'][data_name]['base_col_names']` 存在，`out_col` 默认为该列表；
        否则保持显式提供或返回所有列。
    - 默认 `group_keys`：若 `context['meta'][data_name]['table_keys']` 存在且 `group_keys` 未显式提供，
        则使用 `table_keys` 中去除已出现在 `filter_dict` 中键的剩余列作为分组键（即
        `group_keys = [k for k in table_keys if k not in filter_dict.keys()]`）。如果去除后无剩余键，
        则视为不进行分组（等同于 `group_keys=None`）。
    输出行为说明：
    - 当 `group_keys` 被使用时：
            - `out_option='split'`：返回 `split_dataframe_by_groups` 的列表格式，元素为 `(group_key_dict, group_df)`，
                其中 `group_df` 为该组的数据副本，不包含分组列（如果 `out_col` 被指定，`group_df` 会仅包含
                `out_col` 中除分组列之外的列）。
            - `out_option='groups'`：返回字典，键为分组键的元组（或标量），值为包含分组列的 DataFrame；
                若 `out_col` 被指定，则返回的 DataFrame 会包含 `group_keys` 与 `out_col`（不重复）。
    - 当未指定 `group_keys`（或推断为 None）时：
            - `out_option='frame'` 或 `out_option='split'`：返回筛选后的单个 DataFrame（`split` 在无分组时
                等价于 `frame`）；
    - `out_option` 必须是 `'split'`、`'groups'` 或 `'frame'`。
    - `context`：包含 `context['data']`（表数据）以及可选的 `context['meta']`（表级元数据）的字典。
    返回所请求的数据结构；若表不存在会抛出 `KeyError`。"""
    if context is None or not isinstance(context, dict):
        raise TypeError('context (dict) is required')

    db = context.get('data', {})
    if not isinstance(db, dict):
        raise TypeError(
            "context['data'] must be a dict mapping table name -> DataFrame")

    if data_name not in db:
        raise KeyError(f"table '{data_name}' not found in context['data']")

    df = db[data_name]
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"table '{data_name}' is not a DataFrame")

    # work on a copy to avoid mutating stored table
    working = df.copy()

    # apply filter if provided
    if filter_dict:
        working = filter_dataframe(working,
                                   filter_dict,
                                   combine='and',
                                   inplace=False)

    # attempt to read metadata defaults from context['meta'][data_name]
    meta_db = context.get('meta', {}) if isinstance(context, dict) else {}
    meta = meta_db.get(data_name) if isinstance(meta_db, dict) else None

    # default out_col from meta['base_col_names'] when not explicitly provided
    if out_col is None and isinstance(meta, dict) and 'base_col_names' in meta:
        out_col = list(meta['base_col_names'])

    # default group_keys from meta['table_keys'] minus any keys used in filter_dict
    if group_keys is None and isinstance(meta, dict) and 'table_keys' in meta:
        used = set(filter_dict.keys()) if isinstance(filter_dict,
                                                     dict) else set()
        remaining = [k for k in meta['table_keys'] if k not in used]
        # if no remaining keys, treat as no grouping
        group_keys = remaining if len(remaining) > 0 else None

    # Handle out_col selection while preserving group_keys for grouping
    desired_cols = None
    if out_col is not None:
        if not isinstance(out_col, (list, tuple)):
            raise TypeError('out_col must be a list or tuple of column names')
        desired_cols = list(out_col)
        # If grouping will be performed, ensure group keys are present for grouping operations
        if group_keys:
            for g in group_keys:
                if g not in working.columns:
                    raise KeyError(f"group column '{g}' not present in table")
            # temporary columns to keep for grouping: group_keys + desired_cols
            temp_cols = list(dict.fromkeys(list(group_keys) + desired_cols))
            for c in temp_cols:
                if c not in working.columns:
                    raise KeyError(
                        f"requested column '{c}' not present in table")
            working = working[temp_cols].copy()
        else:
            # no grouping, simply subset to desired columns
            for c in desired_cols:
                if c not in working.columns:
                    raise KeyError(
                        f"requested column '{c}' not present in table")
            working = working[desired_cols].copy()

    # no grouping requested -> return frame (or split treated as frame)
    if not group_keys:
        if out_option in ('frame', 'split'):
            return working
        else:
            raise ValueError(
                "out_option must be one of 'split', 'groups', or 'frame'")

    # validate group_keys
    if not isinstance(group_keys, (list, tuple)):
        raise TypeError('group_keys must be a list or tuple of column names')
    group_keys = list(group_keys)
    for col in group_keys:
        if col not in working.columns:
            raise KeyError(f"group column '{col}' not present in table")

    if out_option == 'split':
        # split_dataframe_by_groups will remove group columns from each group_df
        # If desired_cols specified, ensure split groups contain only desired columns (without group cols)
        splits = split_dataframe_by_groups(working, group_keys)
        if desired_cols is None:
            return splits
        # for split, each group_df returned by split_dataframe_by_groups has no group columns
        # so we need to subset each group_df to desired_cols minus group_keys
        sub_cols = [c for c in desired_cols if c not in group_keys]
        new_splits = []
        for key_dict, grp in splits:
            new_grp = grp.copy()
            # validate columns
            for c in sub_cols:
                if c not in new_grp.columns:
                    raise KeyError(
                        f"requested column '{c}' not present in group result")
            new_splits.append((key_dict, new_grp[sub_cols].copy()))
        return new_splits
    elif out_option == 'groups':
        grouped = {}
        gb = working.groupby(group_keys, dropna=False)
        for key, group in gb:
            # normalize key to tuple
            if isinstance(key, tuple):
                key_tuple = key
            else:
                key_tuple = (key, )
            # For groups output, include group columns and any requested out_col (if provided)
            if desired_cols is not None:
                cols_to_return = list(group_keys) + [
                    c for c in desired_cols if c not in group_keys
                ]
                grouped[key_tuple] = group[cols_to_return].copy()
            else:
                grouped[key_tuple] = group.copy()
        return grouped
    elif out_option == 'frame':
        return working
    else:
        raise ValueError(
            "out_option must be one of 'split', 'groups', or 'frame'")
