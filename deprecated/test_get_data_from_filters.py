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


def _setup_dataset(ctx, dataname="ds", path=["p1"]):
    # create several tables under same path
    pl.write_data_to_dataset(dataname, path, {"name": "t1"}, ctx,
                             [[1, 2], [3, 4]], {
                                 "columns": ["a", "b"],
                                 "name": "t1"
                             })
    pl.write_data_to_dataset(dataname, path, {"name": "t2"}, ctx, [[10, 20]], {
        "columns": ["a", "b"],
        "name": "t2"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "t10"}, ctx, [[100]], {
        "columns": ["a"],
        "name": "t10"
    })
    pl.write_data_to_dataset(dataname, path, {"name": "summary"}, ctx, [[999]],
                             {
                                 "note": "summary",
                                 "name": "summary"
                             })


def _table_keys_of(data_map):
    # extract table_key prefixes from flattened keys (split by first underscore)
    keys = set()
    for k in data_map.keys():
        if isinstance(k, str) and "_" in k:
            keys.add(k.split("_", 1)[0])
        else:
            keys.add(k)
    return keys


def _table_names_from_meta(meta_map):
    # extract the stored metadata 'name' values for returned tables
    names = set()
    for k, v in meta_map.items():
        # expected key format: <table_key>_metadata_name
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == "metadata" and parts[2] == "name":
            names.add(v)
    return names


def test_get_data_from_all_and_exact_and_glob_and_regex_and_in_and_callable():
    ctx = DummyContext()
    _setup_dataset(ctx)

    # all tables (collect names from returned metadata)
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys=None,
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names >= {"t1", "t2", "t10", "summary"}

    # exact match
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "t1"},
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names == {"t1"}

    # glob match (t*) matches t1,t2,t10
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "t*"},
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names >= {"t1", "t2", "t10"}
    assert "summary" not in names

    # regex match for numeric suffix
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "re:^t[0-9]+$"},
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names == {"t1", "t2", "t10"}

    # substring match
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": "in:sum"},
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names == {"summary"}

    # list of patterns
    data_map, meta_map = pl.get_data_from(
        "ds", ["p1"], table_keys={"name": ["t1", "summary"]}, context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names == {"t1", "summary"}

    # callable predicate
    pred = lambda v: v.endswith("1")
    data_map, meta_map = pl.get_data_from("ds", ["p1"],
                                          table_keys={"name": pred},
                                          context=ctx)
    names = _table_names_from_meta(meta_map)
    assert names == {"t1"}
