import json

from utils.pipeline import write_data_to_dataset, get_data_from
from deprecated.table_backend import DEFAULT_BACKEND


class DummyContext:

    def __init__(self):
        self._data = {}
        self._shared = {}

    def get_data(self, key_path, default=None):
        if not key_path:
            return self._data
        cur = self._data
        for k in key_path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    def get_shared(self, key_path, default=None):
        if not key_path:
            return self._shared
        cur = self._shared
        for k in key_path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    def set_shared(self, key_path, value):
        cur = self._shared
        for k in key_path[:-1]:
            cur = cur.setdefault(k, {})
        cur[key_path[-1]] = value

    def setdefault_data(self, key_path, default):
        cur = self._data
        for k in key_path[:-1]:
            cur = cur.setdefault(k, {})
        return cur.setdefault(key_path[-1], default)


def test_write_and_read_basic():
    # ensure a clean backend store for the test
    DEFAULT_BACKEND._store.clear()

    ctx = DummyContext()
    dataname = "test_ds"
    path = ["x", "y"]
    table_keys = {"k": "v"}
    data = [[1, 2], [3, 4]]
    metadata = {"note": "ok"}

    # write
    got_meta = write_data_to_dataset(dataname, path, table_keys, ctx, data,
                                     metadata)
    assert got_meta == metadata

    # compute expected serialized prefix used by the InMemoryTableBackend
    # write_data_to_dataset maps path parts to default categories 'level0','level1'
    extra_cols = {"level0": "x", "level1": "y", "k": "v"}
    expected_prefix = json.dumps(extra_cols,
                                 sort_keys=True,
                                 separators=(",", ":"))

    # read back via pipeline adapter
    data_map, updated_meta = get_data_from(dataname,
                                           path,
                                           table_keys=table_keys,
                                           context=ctx)

    # data_map should contain the stored table under the serialized prefix
    data_key = f"{expected_prefix}_data_0"
    assert data_key in data_map
    assert data_map[data_key] == data

    # updated_meta should include an injected path column for the discovered prefix
    path_key = f"{expected_prefix}_path"
    assert updated_meta.get(path_key) == "x/y"


def test_table_keys_none():
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dataname = "t_none"
    path = ["p0", "p1"]
    table_keys = None
    data = [[10]]

    write_data_to_dataset(dataname, path, table_keys, ctx, data, {})

    extra_cols = {"level0": "p0", "level1": "p1"}
    expected_prefix = json.dumps(extra_cols,
                                 sort_keys=True,
                                 separators=(",", ":"))

    data_map, meta = get_data_from(dataname,
                                   path,
                                   table_keys=None,
                                   context=ctx)
    assert f"{expected_prefix}_data_0" in data_map


def test_path_string_and_pathobj():
    from pathlib import Path as _P
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dataname = "t_path"

    # string path
    write_data_to_dataset(dataname, "a/b", {"k": "1"}, ctx, [[1]], {})
    # Path object path
    write_data_to_dataset(dataname, _P("c/d"), {"k": "2"}, ctx, [[2]], {})

    # verify both prefixes exist
    p1 = json.dumps({
        "level0": "a",
        "level1": "b",
        "k": "1"
    },
                    sort_keys=True,
                    separators=(",", ":"))
    p2 = json.dumps({
        "level0": "c",
        "level1": "d",
        "k": "2"
    },
                    sort_keys=True,
                    separators=(",", ":"))

    dm, mm = get_data_from(dataname, ["a", "b"],
                           table_keys={"k": "1"},
                           context=ctx)
    assert f"{p1}_data_0" in dm
    dm2, mm2 = get_data_from(dataname, ["c", "d"],
                             table_keys={"k": "2"},
                             context=ctx)
    assert f"{p2}_data_0" in dm2


def test_table_keys_conflict_priority():
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dataname = "t_conflict"
    path = ["Z"]
    # table_keys tries to override 'level0' but pipeline should keep path-derived value
    write_data_to_dataset(dataname, path, {
        "level0": "OVERRIDE",
        "k": "v"
    }, ctx, [[9]], {})

    expected = json.dumps({
        "level0": "Z",
        "k": "v"
    },
                          sort_keys=True,
                          separators=(",", ":"))
    dm, mm = get_data_from(dataname, path, table_keys={"k": "v"}, context=ctx)
    assert f"{expected}_data_0" in dm


def test_selector_matching_variants():
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dn = "t_sel"

    # write several records with different keys
    write_data_to_dataset(dn, ["a"], {"color": "red"}, ctx, [[1]], {})
    write_data_to_dataset(dn, ["a"], {"color": "blue"}, ctx, [[2]], {})
    write_data_to_dataset(dn, ["a"], {"color": "green"}, ctx, [[3]], {})

    # exact match
    dm, _ = get_data_from(dn, ["a"], table_keys={"color": "blue"}, context=ctx)
    # find the data under prefix for blue
    prefix_blue = json.dumps({
        "level0": "a",
        "color": "blue"
    },
                             sort_keys=True,
                             separators=(",", ":"))
    assert f"{prefix_blue}_data_0" in dm

    # list membership
    dm2, _ = get_data_from(dn, ["a"],
                           table_keys={"color": ["red", "green"]},
                           context=ctx)
    prefix_red = json.dumps({
        "level0": "a",
        "color": "red"
    },
                            sort_keys=True,
                            separators=(",", ":"))
    prefix_green = json.dumps({
        "level0": "a",
        "color": "green"
    },
                              sort_keys=True,
                              separators=(",", ":"))
    assert any(
        k.startswith(prefix_red) or k.startswith(prefix_green)
        for k in dm2.keys())

    # glob-ish (fnmatch) — use '*' style
    dm3, _ = get_data_from(dn, ["a"], table_keys={"color": "g*"}, context=ctx)
    assert any(k.startswith(prefix_green) for k in dm3.keys())


def test_multiple_records_same_prefix():
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dn = "t_multi"
    key = {"k": "v"}
    write_data_to_dataset(dn, ["x"], key, ctx, [[1]], {})
    write_data_to_dataset(dn, ["x"], key, ctx, [[2]], {})

    prefix = json.dumps({
        "level0": "x",
        "k": "v"
    },
                        sort_keys=True,
                        separators=(",", ":"))
    dm, mm = get_data_from(dn, ["x"], table_keys=key, context=ctx)
    # expect data_0 and data_1
    assert f"{prefix}_data_0" in dm and f"{prefix}_data_1" in dm


def test_no_special_path_handling():
    DEFAULT_BACKEND._store.clear()
    ctx = DummyContext()
    dn = "t_nopath"
    # include a '__path__' key — backend should treat it as normal key
    write_data_to_dataset(dn, {"__path__": ["a", "b"]}, {
        "k": "v",
        "__path__": ["a", "b"]
    }, ctx, [[7]], {})
    selector = {"__path__": ["a", "b"], "k": "v"}
    dm, mm = get_data_from(dn, {"__path__": ["a", "b"]},
                           table_keys={"k": "v"},
                           context=ctx)
    pref = json.dumps({
        "__path__": ["a", "b"],
        "k": "v"
    },
                      sort_keys=True,
                      separators=(",", ":"))
    assert any(k.startswith(pref) for k in dm.keys())
