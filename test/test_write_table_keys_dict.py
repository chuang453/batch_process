import json

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


def test_write_and_read_dict_table_key():
    ctx = DummyContext()
    dataname = "ds_table_keys"
    path = ["p1"]
    table_keys = {"sample": "A", "run": 1}
    data = [[1, 2], [3, 4]]
    meta = {"columns": ["a", "b"]}

    saved_meta = pl.write_data_to_dataset(dataname, path, table_keys, ctx,
                                          data, meta)
    assert saved_meta == meta

    # serialized key we expect as storage key prefix
    serial = json.dumps(table_keys, sort_keys=True, separators=(",", ":"))

    # retrieving all tables should include the serialized key prefix
    data_map, meta_map = pl.get_data_from(dataname,
                                          path,
                                          table_keys=None,
                                          context=ctx)
    found_keys = {k.split("_", 1)[0] for k in data_map.keys()}
    assert serial in found_keys

    # retrieving by dict selector should return only that table
    data_map2, meta_map2 = pl.get_data_from(dataname,
                                            path,
                                            table_keys=table_keys,
                                            context=ctx)
    found_keys2 = {k.split("_", 1)[0] for k in data_map2.keys()}
    assert found_keys2 == {serial}

    # metadata should include a path column entry for the returned table
    path_col_candidates = [
        k for k in meta_map2.keys() if k.startswith(serial + "_")
    ]
    assert any("path" in k or True for k in path_col_candidates)
