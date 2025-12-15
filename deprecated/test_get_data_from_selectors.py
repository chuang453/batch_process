import pytest

from utils import pipeline as pl


class DummyContext:

    def __init__(self):
        self.shared = {}

    def setdefault_shared(self, path, default=None):
        node = self.shared
        for p in path:
            node = node.setdefault(p, {})
        return node

    def get_shared(self, path, default=None):
        node = self.shared
        for p in path:
            if not isinstance(node, dict) or p not in node:
                return default
            node = node[p]
        return node


def _table_names_from_meta(meta_map):
    names = set()
    for k, v in meta_map.items():
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == "metadata" and parts[2] == "name":
            names.add(v)
    return names


def _setup_dataset(ctx, dataname="ds_selectors", path=["p1"]):
    # create several tables under same path with varied metadata
    pl.write_data_to_dataset(dataname, path, {"name": "t1"}, ctx, [[1, 2]], {
        "columns": ["a", "b"],
        "name": "t1",
        "group": "g1"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "t2"}, ctx, [[10, 20]], {
        "columns": ["a", "b"],
        "name": "t2",
        "group": "g1"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "t10"}, ctx, [[100]], {
        "columns": ["a"],
        "name": "t10",
        "group": "g2"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "summary"}, ctx, [[999]], {
        "note": "summary",
        "name": "summary",
        "group": "g2"
    })


def test_get_data_from_various_selectors():
    ctx = DummyContext()
    dataname = "ds_selectors"
    _setup_dataset(ctx, dataname=dataname)

    # all tables
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys=None, context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names >= {"t1", "t2", "t10", "summary"}

    # exact table_keys
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "t1"}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1"}

    # glob
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "t*"}, context=ctx)
    assert _table_names_from_meta(meta_map) >= {"t1", "t2", "t10"}

    # regex
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "re:^t[0-9]+$"}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "t2", "t10"}

    # substring
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "in:sum"}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"summary"}

    # list-of-values
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": ["t1", "summary"]}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "summary"}

    # callable predicate
    pred = lambda v: v.endswith("1")
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": pred}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1"}

    # selector on metadata column 'group'
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"group": "g1"}, context=ctx)
    assert _table_names_from_meta(meta_map) >= {"t1", "t2"}

    # path as dict selector (should act like metadata selector)
    data_map, meta_map = pl.get_data_from(dataname, {"name": "t2"}, table_keys=None, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t2"}

    # merged selectors: path dict + table_keys, table_keys take precedence
    data_map, meta_map = pl.get_data_from(dataname, {"name": "t1"}, table_keys={"name": "t2"}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t2"}

    # list membership selector
    data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": ["t1", "t10"]}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "t10"}
import pytest

from utils import pipeline as pl


class DummyContext:

    def __init__(self):
        self.shared = {}

    def setdefault_shared(self, path, default=None):
        node = self.shared
        for p in path:
            node = node.setdefault(p, {})
        return node

    def get_shared(self, path, default=None):
        node = self.shared
        for p in path:
            if not isinstance(node, dict) or p not in node:
                return default
            node = node[p]
        return node


def _table_names_from_meta(meta_map):
    names = set()
    for k, v in meta_map.items():
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == "metadata" and parts[2] == "name":
            names.add(v)
    return names


def _setup_dataset(ctx, dataname="ds", path=["p1"]):
    # create several tables under same path with varied metadata
    pl.write_data_to_dataset(dataname, path, {"name": "t1"}, ctx, [[1, 2]], {
        "columns": ["a", "b"],
        "name": "t1",
        "group": "g1"
    })
        "columns": ["a", "b"],
        "name": "t1",
        "group": "g1"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "t2"}, ctx, [[10, 20]], {
        "columns": ["a", "b"],
        "name": "t2",
        "group": "g1"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "t10"}, ctx, [[100]], {
        "columns": ["a"],
        "name": "t10",
        "group": "g2"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "summary"}, ctx, [[999]],
                             {
                                 "note": "summary",
                                 "name": "summary",
                                 "group": "g2"
                             })


def test_get_data_from_various_selectors():
    ctx = DummyContext()
    _setup_dataset(ctx)

    # all tables
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys=None,
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names >= {"t1", "t2", "t10", "summary"}

    # exact table_keys
        dataname = "ds_selectors"
        _setup_dataset(ctx, dataname=dataname)
                                          table_keys={"name": "t1"},
                                          context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1"}
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys=None, context=ctx)
    # glob
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "t*"},
                                          context=ctx)
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "t1"}, context=ctx)

    # regex
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "re:^t[0-9]+$"},
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "t*"}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "t2", "t10"}

    # substring
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "re:^t[0-9]+$"}, context=ctx)
                                          context=ctx)
    assert _table_names_from_meta(meta_map) == {"summary"}

    # list-of-values
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": "in:sum"}, context=ctx)
        "ds", ["p1"], table_keys={"name": ["t1", "summary"]}, context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "summary"}
            dataname, ["p1"], table_keys={"name": ["t1", "summary"]}, context=ctx)
    # callable predicate
    pred = lambda v: v.endswith("1")
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": pred},
                                          context=ctx)
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": pred}, context=ctx)

    # selector on metadata column 'group'
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"group": "g1"},
        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"group": "g1"}, context=ctx)
    assert _table_names_from_meta(meta_map) >= {"t1", "t2"}

    # path as dict selector (should act like metadata selector)
    data_map, meta_map = pl.get_data_from("ds", {"name": "t2"},
        data_map, meta_map = pl.get_data_from(dataname, {"name": "t2"}, table_keys=None, context=ctx)
                                          context=ctx)
    assert _table_names_from_meta(meta_map) == {"t2"}

    # merged selectors: path dict + table_keys, table_keys take precedence
        data_map, meta_map = pl.get_data_from(dataname, {"name": "t1"}, table_keys={"name": "t2"}, context=ctx)
                                          table_keys={"name": "t2"},
                                          context=ctx)
    assert _table_names_from_meta(meta_map) == {"t2"}

        data_map, meta_map = pl.get_data_from(dataname, ["p1"], table_keys={"name": ["t1", "t10"]}, context=ctx)
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": ["t1", "t10"]},
                                          context=ctx)
    assert _table_names_from_meta(meta_map) == {"t1", "t10"}
