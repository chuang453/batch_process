import pandas as pd

from core.data_stage import DataStage
from decorators.processor import ProcessingContext, transform


@transform(name='append_row_count_for_test')
def append_row_count_for_test(df, context, **kwargs):
    out = df.copy()
    out['row_count'] = len(out)
    return out


def test_builtin_ops_chain():
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    df = pd.DataFrame({'a': [1, None, 3], 'b': [3, 2, 1]})

    out = ds.run_steps(df, [
        {'dropna': {'subset': ['a']}},
        {'sort': {'by': 'b', 'ascending': True}},
        {'select': ['a', 'b']},
    ])

    assert list(out['b']) == [1, 3]


def test_group_by_collect_and_runtime_pop():
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    df = pd.DataFrame({
        'group': ['x', 'x', 'y'],
        'value': [1, 2, 3],
    })

    out = ds.run_steps(df, [{
        'group_by': 'group',
        'collect': True,
        'steps': [
            {'run': ['append_row_count_for_test']},
        ],
    }])

    assert isinstance(out, pd.DataFrame)
    assert 'group' in out.columns
    assert 'row_count' in out.columns

    runtime_info = ctx.get_metadata(['runtime_info'], {})
    assert runtime_info.get('loop_cols', []) == []
    assert runtime_info.get('loop_vars', []) == []
