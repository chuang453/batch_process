import numpy as np
from utils.adapters import df_helpers as dh


def test_filter_eq_and_missing_key():
    data = [({'a': 1, 'b': 'x'}, 'o1'), ({'a': 2}, 'o2')]
    res = dh.filter_pairs_by_dict(data, {'a': 1})
    assert len(res) == 1
    assert res[0][1] == 'o1'

    # missing key should not match
    res2 = dh.filter_pairs_by_dict(data, {'b': 'x', 'c': 1})
    assert res2 == []


def test_filter_membership():
    data = [({'t': 'a'}, 1), ({'t': 'c'}, 2), ({'t': 'b'}, 3)]
    res = dh.filter_pairs_by_dict(data, {'t': ['a', 'b']})
    assert len(res) == 2
    assert {p[1] for p in res} == {1, 3}


def test_filter_none_and_nan():
    data = [({'v': None}, 'n1'), ({'v': np.nan}, 'n2'), ({'v': 5}, 'n3')]
    res = dh.filter_pairs_by_dict(data, {'v': None})
    # both None and NaN should match
    assert {r[1] for r in res} == {'n1', 'n2'}


def test_filter_callable():
    data = [({'x': 10}, 'a'), ({'x': 3}, 'b')]
    res = dh.filter_pairs_by_dict(data, {'x': lambda v: (v or 0) > 5})
    assert len(res) == 1 and res[0][1] == 'a'
