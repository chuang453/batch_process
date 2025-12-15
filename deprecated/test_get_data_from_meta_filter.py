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


def test_get_data_from_metadata_filter():
    ctx = DummyContext()
    ds = 'ds'
    path = ['p1']

    # write two tables with metadata including sample/run (use dict table_keys)
    pl.write_data_to_dataset(ds, path, {'name': 'tA'}, ctx, [[1]], {
        'sample': 'A',
        'run': 1,
        'name': 'tA'
    })
    pl.write_data_to_dataset(ds, path, {'name': 'tB'}, ctx, [[2]], {
        'sample': 'B',
        'run': 1,
        'name': 'tB'
    })

    # filter by metadata column value exact
    data_map, meta_map = pl.get_data_from(ds,
                                          path,
                                          table_keys={'sample': 'A'},
                                          context=ctx)
    # extract returned table 'name' values from metadata map
    names = set()
    for k, v in meta_map.items():
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == 'metadata' and parts[2] == 'name':
            names.add(v)
    assert 'tA' in names

    # filter by metadata using in: substring (matches 'A' in sample)
    data_map2, meta_map2 = pl.get_data_from(ds,
                                            path,
                                            table_keys={'sample': 'in:A'},
                                            context=ctx)
    names2 = set()
    for k, v in meta_map2.items():
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == 'metadata' and parts[2] == 'name':
            names2.add(v)
    assert len(names2) >= 1

    # filter by a numeric equality
    data_map3, meta_map3 = pl.get_data_from(ds,
                                            path,
                                            table_keys={'run': 1},
                                            context=ctx)
    names3 = set()
    for k, v in meta_map3.items():
        parts = k.split("_", 2)
        if len(parts) == 3 and parts[1] == 'metadata' and parts[2] == 'name':
            names3.add(v)
    assert names3 >= {'tA', 'tB'}
