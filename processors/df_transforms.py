from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from decorators.processor import ProcessingContext, transform
from utils.param_resolver import resolve_param


@transform(name='df_select_cols', outputs=['selected columns'])
def df_select_cols(df: pd.DataFrame, context: ProcessingContext,
                   **kwargs) -> pd.DataFrame:
    out_cols = resolve_param('out_cols', kwargs, context, 'select_cols')
    if not out_cols:
        context.add_result({
            'processor': 'df_select_cols',
            'status': 'skipped',
            'reason': 'no out_cols provided',
        })
        return df
    return df[list(out_cols)]


@transform(name='df_sort_by')
def df_sort_by(df: pd.DataFrame, context: ProcessingContext,
               **kwargs) -> pd.DataFrame:
    sort_cols = resolve_param('sort_cols', kwargs, context, 'sort_by')
    ascending = resolve_param('ascending', kwargs, context, 'sort_by', True)
    if not sort_cols:
        return df
    return df.sort_values(by=sort_cols, ascending=ascending)


@transform(name='df_rename_cols')
def df_rename_cols(df: pd.DataFrame, context: ProcessingContext,
                   **kwargs) -> pd.DataFrame:
    rename_dict = resolve_param('rename_dict', kwargs, context, 'rename_cols')
    if not rename_dict:
        return df
    return df.rename(columns=rename_dict)


@transform(name='df_fillna')
def df_fillna(df: pd.DataFrame, context: ProcessingContext,
              **kwargs) -> pd.DataFrame:
    filling = resolve_param('filling', kwargs, context, 'df_fillna', '-')
    return df.fillna(filling)


@transform(name='find_min_MS', inputs=['main:df'], outputs=['filtered df'])
def find_min_MS(df: pd.DataFrame, context: ProcessingContext,
                **kwargs) -> pd.DataFrame:
    ms_col = resolve_param('MS_col', kwargs, context, 'find_min_MS', 'MS')
    n_sel = resolve_param('n_sel', kwargs, context, 'find_min_MS', 1)
    case_cols = resolve_param('case_cols', kwargs, context, 'find_min_MS', [])
    mat_cols = resolve_param('mat_cols', kwargs, context, 'find_min_MS', [])

    group_cols: List[str] = list(case_cols or []) + list(mat_cols or [])
    if ms_col not in df.columns:
        context.add_result({
            'processor': 'find_min_MS',
            'status': 'skipped',
            'reason': f'missing column: {ms_col}',
        })
        return df

    if group_cols:
        valid_group_cols = [c for c in group_cols if c in df.columns]
        if not valid_group_cols:
            return df.nsmallest(int(n_sel), ms_col)
        return df.groupby(valid_group_cols, dropna=False,
                          group_keys=False).apply(
                              lambda g: g.nsmallest(int(n_sel), ms_col))

    return df.nsmallest(int(n_sel), ms_col)


@transform(name='find_max_damage', inputs=['main:df'], outputs=['filtered df'])
def find_max_damage(df: pd.DataFrame, context: ProcessingContext,
                    **kwargs) -> pd.DataFrame:
    dmg_col = resolve_param('dmg_col', kwargs, context, 'find_max_damage',
                            'damage')
    n_sel = resolve_param('n_sel', kwargs, context, 'find_max_damage', 1)
    case_cols = resolve_param('case_cols', kwargs, context,
                              'find_max_damage', [])
    mat_cols = resolve_param('mat_cols', kwargs, context, 'find_max_damage',
                             [])

    group_cols: List[str] = list(case_cols or []) + list(mat_cols or [])
    if dmg_col not in df.columns:
        context.add_result({
            'processor': 'find_max_damage',
            'status': 'skipped',
            'reason': f'missing column: {dmg_col}',
        })
        return df

    if group_cols:
        valid_group_cols = [c for c in group_cols if c in df.columns]
        if not valid_group_cols:
            return df.nlargest(int(n_sel), dmg_col)
        return df.groupby(valid_group_cols, dropna=False,
                          group_keys=False).apply(
                              lambda g: g.nlargest(int(n_sel), dmg_col))

    return df.nlargest(int(n_sel), dmg_col)
