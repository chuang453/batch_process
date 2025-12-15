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


def test_pipeline_forwards_path_columns_to_backend():
    ctx = DummyContext()
    dataname = "itest"
    path = ["p1", "p2"]
    table_keys = {"name": "tX"}
    data = [[1, 2], [3, 4]]
    metadata = {"columns": ["a", "b"]}

    # write via pipeline
    saved_meta = pl.write_data_to_dataset(dataname, path, table_keys, ctx,
                                          data, metadata)

    # original metadata should be returned
    assert saved_meta is metadata

    # read back via pipeline adapter; this should inject a path column
    data_map, meta_map = pl.get_data_from(dataname,
                                          path,
                                          table_keys=table_keys,
                                          context=ctx)

    # path value should be present in one of the returned metadata values
    path_value = "/".join(path)
    found = False
    for v in meta_map.values():
        if v == path_value:
            found = True
            break
        if isinstance(v, str) and path_value in v:
            found = True
            break
        if isinstance(v, (list, tuple)) and path_value in v:
            found = True
            break
    assert found, f"expected path '{path_value}' in returned metadata values"
