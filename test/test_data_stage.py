import pandas as pd
import pytest

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

    # runtime_info is no longer mutated by DataStage group loops.
    runtime_info = ctx.get_metadata(['runtime_info'], {})
    assert runtime_info == {}


def test_simulate_steps_flat():
    """simulate_steps returns enriched flat records for builtin and transform steps."""
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    steps = [
        {'dropna': {'subset': ['a']}},
        {'sort': {'by': 'b'}},
        {'run': ['my_func']},
    ]
    records = ds.simulate_steps(None, steps)
    assert len(records) == 3

    assert records[0]['step'] == '1'
    assert records[0]['step_type'] == 'builtin_op'
    assert records[0]['op_name'] == 'dropna'
    assert records[0]['level'] == 0

    assert records[1]['step'] == '2'
    assert records[1]['op_name'] == 'sort'

    assert records[2]['step'] == '3'
    assert records[2]['step_type'] == 'transform'
    assert records[2]['op_name'] == 'my_func'


def test_simulate_steps_nested_group_by():
    """simulate_steps recursively expands group_by sub-steps."""
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    steps = [
        {'dropna': {}},
        {
            'group_by': ['col_a', 'col_b'],
            'steps': [
                {'sort': {'by': 'x'}},
                {'run': ['fn1', 'fn2']},
            ],
        },
        {'select': ['a']},
    ]
    records = ds.simulate_steps(None, steps)

    # Total: 1 (dropna) + 1 (group_by) + 2 (sub) + 1 (select) = 5
    assert len(records) == 5

    # group_by parent
    gb = records[1]
    assert gb['step'] == '2'
    assert gb['step_type'] == 'group_by'
    assert gb['op_name'] == 'col_a, col_b'
    assert gb['level'] == 0

    # nested sub-steps
    sub1 = records[2]
    assert sub1['step'] == '2.1'
    assert sub1['step_type'] == 'builtin_op'
    assert sub1['level'] == 1

    sub2 = records[3]
    assert sub2['step'] == '2.2'
    assert sub2['step_type'] == 'transform'
    assert sub2['op_name'] == 'fn1, fn2'
    assert sub2['level'] == 1

    # step after group_by
    assert records[4]['step'] == '3'
    assert records[4]['level'] == 0


def test_simulate_steps_empty():
    """simulate_steps with empty steps list returns empty."""
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    assert ds.simulate_steps(None, []) == []
    assert ds.simulate_steps(None, None) == []


def test_run_steps_abort_policy_raises_on_failure():
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})

    with pytest.raises(Exception):
        ds.run_steps(df, [{'select': ['missing_column']}], on_error='abort')


def test_run_steps_observer_receives_events():
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    events = []

    ds.run_steps(df,
                 [{'select': ['a']}],
                 observer=lambda e: events.append((e.step_id, e.op_name, e.kind)))

    assert events[0] == ('1', 'select', 'started')
    assert events[1] == ('1', 'select', 'finished')
