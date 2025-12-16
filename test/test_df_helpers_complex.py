import pandas as pd
import numpy as np
import pytest
from pandas.testing import assert_frame_equal

from utils.adapters.df_helpers import (
    write_data_to_database,
    get_data_from_database,
    prepend_dict_columns,
    filter_dataframe,
    split_dataframe_by_groups,
)


def test_complex_integration():
    # prepare an empty context (in-memory "database")
    ctx = {}

    # 1) write initial table (two rows)
    data0 = [[1, 10], [2, 20]]
    cols = ["a", "b"]
    write_data_to_database("tbl", data0, cols, {"src": "init"}, ctx)

    tbl = ctx["data"]["tbl"]
    assert list(tbl.columns) == ["a", "b", "src"]
    assert len(tbl) == 2
    assert (tbl["src"] == "init").all()

    # 2) append rows with extra columns (one scalar, one per-row sequence)
    data1 = [[3, 30], [4, 40]]
    extra = {"src": ["app", "app2"], "tag": "X"}
    write_data_to_database("tbl", data1, cols, extra, ctx)

    df_all = get_data_from_database("tbl", context=ctx, out_option="frame")
    # expected columns include a,b,src,tag (order may vary by implementation)
    assert set(df_all.columns) >= {"a", "b", "src", "tag"}
    assert len(df_all) == 4

    # check appended values present
    # find row where a == 3
    row3 = df_all[df_all["a"] == 3]
    assert row3.shape[0] == 1
    assert row3.iloc[0]["b"] == 30
    assert row3.iloc[0]["src"] == "app"
    assert row3.iloc[0]["tag"] == "X"

    # 3) use filter_dataframe to get rows with a > 2 and src == 'app'
    filtered = filter_dataframe(df_all, {"a": lambda s: s > 2, "src": "app"})
    assert len(filtered) == 1
    assert int(filtered.iloc[0]["a"]) == 3

    # 4) test get_data_from_database grouping outputs
    split_res = get_data_from_database("tbl",
                                       None, ["src"],
                                       out_option="split",
                                       context=ctx)
    # split_res is list of (group_key_dict, group_df_without_group_cols)
    assert isinstance(split_res, list)
    # reconstruct counts per src
    counts = {kd["src"]: len(gdf) for kd, gdf in split_res}
    assert counts.get("init") == 2
    assert counts.get("app") == 1
    assert counts.get("app2") == 1

    # 5) test 'groups' out_option returns group DataFrames that include group column
    groups_map = get_data_from_database("tbl",
                                        None, ["src"],
                                        out_option="groups",
                                        context=ctx)
    assert isinstance(groups_map, dict)
    # keys are tuples of group values
    assert any(k == ("init", ) for k in groups_map.keys())
    assert groups_map[("init", )].shape[0] == 2

    # 6) test prepend_dict_columns with mixed scalar and sequence and inplace behavior
    base = pd.DataFrame({"x": [10, 20, 30]})
    res = prepend_dict_columns(base, {"meta": [1, 2, 3], "tag": "Z"})
    assert list(res.columns[:2]) == ["meta", "tag"]
    assert (res["tag"] == "Z").all()

    # ensure inplace=True mutates original object
    b2 = base.copy()
    ret = prepend_dict_columns(b2, {"m": 9}, inplace=True)
    assert ret is b2
    assert list(b2.columns)[0] == "m"

    # 7) test split_dataframe_by_groups handles NaN group keys
    df_nan = pd.DataFrame({"g": ["a", None, "a", None], "v": [1, 2, 3, 4]})
    split_nan = split_dataframe_by_groups(df_nan, ["g"])
    # expect two groups ('a' and None)
    gvals = {kd["g"] for kd, _ in split_nan}
    assert set(gvals) == {"a", None}

    # compare one group's content via assert_frame_equal (after resetting index)
    for kd, gdf in split_nan:
        if kd["g"] == "a":
            assert_frame_equal(
                gdf.reset_index(drop=True),
                pd.DataFrame({
                    "v": [1, 3]
                }).reset_index(drop=True))


if __name__ == "__main__":
    pytest.main(["-q", "test/test_df_helpers_complex.py"])
