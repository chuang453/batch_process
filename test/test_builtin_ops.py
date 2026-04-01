import pandas as pd

from stage2_platform.execution.builtin_ops import BUILTIN_OPS, register_builtin_op


def test_builtin_ops_registry_contains_core_ops():
    for key in ['dropna', 'select', 'sort', 'head', 'tail']:
        assert key in BUILTIN_OPS


def test_builtin_select_and_sort():
    df = pd.DataFrame({'a': [2, 1], 'b': [10, 20]})
    out = BUILTIN_OPS['sort'](df, {'by': 'a', 'ascending': True})
    out = BUILTIN_OPS['select'](out, ['a'])
    assert list(out['a']) == [1, 2]


def test_register_builtin_op():
    def _noop(df, _params):
        return df

    register_builtin_op('noop_for_test', _noop)
    assert 'noop_for_test' in BUILTIN_OPS
