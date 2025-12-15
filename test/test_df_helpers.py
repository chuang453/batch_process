import pandas as pd
import numpy as np
import pytest

from utils.adapters.df_helpers import filter_dataframe, split_dataframe_by_groups
from utils.adapters.df_helpers import prepend_dict_columns


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "A": [10, 20, 10, 30, np.nan],
        "B": [1, -1, 5, 6, 0],
        "C": ["x", "y", "x", "z", "y"],
        "D": [None, "val", "val", None, "val"]
    })


def test_exact_match(sample_df):
    res = filter_dataframe(sample_df, {"A": 10})
    assert list(res["A"]) == [10, 10]
    assert len(res) == 2


def test_callable_condition(sample_df):
    res = filter_dataframe(sample_df, {"B": lambda s: s > 0})
    assert (res["B"] > 0).all()
    assert len(res) == 3


def test_iterable_membership(sample_df):
    res = filter_dataframe(sample_df, {"C": ["x", "z"]})
    assert set(res["C"]) <= {"x", "z"}
    assert len(res) == 3


def test_none_match(sample_df):
    res = filter_dataframe(sample_df, {"A": None})
    assert res.shape[0] == 1
    assert pd.isna(res.iloc[0]["A"])


def test_combine_or(sample_df):
    # A == 10 or B > 5
    res = filter_dataframe(sample_df, {
        "A": 10,
        "B": lambda s: s > 5
    },
                           combine="or")
    # rows with A==10 are idx 0,2; B>5 is idx 3
    assert set(res.index) == {0, 2, 3}


def test_inplace_mutation(sample_df):
    df = sample_df.copy()
    out = filter_dataframe(df, {"A": 10}, inplace=True)
    # return value should be same object
    assert out is df
    assert list(df["A"]) == [10, 10]


def test_missing_column_raises(sample_df):
    with pytest.raises(KeyError):
        filter_dataframe(sample_df, {"NOPE": 1})


def test_invalid_conditions_type(sample_df):
    with pytest.raises(TypeError):
        filter_dataframe(sample_df, [1, 2, 3])


def test_invalid_combine(sample_df):
    with pytest.raises(ValueError):
        filter_dataframe(sample_df, {"A": 10}, combine="xor")


def test_callable_returns_scalar_true(sample_df):
    res = filter_dataframe(sample_df, {"A": lambda s: True})
    assert len(res) == len(sample_df)


def test_callable_returns_numpy_array(sample_df):

    def cond(s):
        # return numpy boolean array
        return np.array([v == 10 for v in s.values])

    res = filter_dataframe(sample_df, {"A": cond})
    assert len(res) == 2


def test_non_inplace_returns_copy(sample_df):
    df = sample_df.copy()
    res = filter_dataframe(df, {"A": 10}, inplace=False)
    # modifying result should not change original
    res.loc[res.index[0], "A"] = 999
    assert df.loc[res.index[0], "A"] != 999


def test_split_single_group(sample_df):
    groups = split_dataframe_by_groups(sample_df, ["C"])
    assert len(groups) == 3
    vals = {kd["C"] for kd, _ in groups}
    assert vals == {"x", "y", "z"}
    expected = {"x": 2, "y": 2, "z": 1}
    for kd, gdf in groups:
        assert "C" not in gdf.columns
        assert len(gdf) == expected[kd["C"]]


def test_split_multi_group(sample_df):
    groups = split_dataframe_by_groups(sample_df, ["C", "A"])
    # expected unique combinations: (x,10), (y,20), (y, nan), (z,30)
    assert len(groups) == 4
    for kd, gdf in groups:
        assert "C" not in gdf.columns and "A" not in gdf.columns


def test_split_empty_group_cols(sample_df):
    groups = split_dataframe_by_groups(sample_df, [])
    assert len(groups) == 1
    kd, gdf = groups[0]
    assert kd == {}
    assert gdf.equals(sample_df)


def test_split_missing_column_raises(sample_df):
    with pytest.raises(KeyError):
        split_dataframe_by_groups(sample_df, ["NOPE"])


def test_split_nan_groups_preserved(sample_df):
    groups = split_dataframe_by_groups(sample_df, ["D"])
    # expect two groups (None and 'val') with sizes 2 and 3
    counts = sorted([len(gdf) for _, gdf in groups])
    assert counts == [2, 3]


def test_prepend_scalar_and_order(sample_df):
    res = prepend_dict_columns(sample_df, {'X': 'x', 'Y': 1})
    # keys are first
    assert list(res.columns[:2]) == ['X', 'Y']
    assert all(res['X'] == 'x')
    assert all(res['Y'] == 1)
    # original columns preserved after
    assert set(res.columns[2:]) >= set(sample_df.columns)


def test_prepend_sequence_values(sample_df):
    vals = ['a', 'b', 'c', 'd', 'e']
    res = prepend_dict_columns(sample_df, {'T': vals})
    assert list(res['T']) == vals


def test_prepend_length_mismatch_raises(sample_df):
    with pytest.raises(ValueError):
        prepend_dict_columns(sample_df, {'T': [1, 2]})


def test_prepend_inplace_and_overwrite(sample_df):
    df = sample_df.copy()
    # add column with same name as existing to ensure overwrite
    out = prepend_dict_columns(df, {'A': 999, 'Z': 'z'}, inplace=True)
    assert out is df
    # 'A' should be overwritten and moved to front
    assert list(df.columns[:2]) == ['A', 'Z']
    assert (df['A'] == 999).all()
