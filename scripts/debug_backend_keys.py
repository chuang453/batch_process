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

    # minimal get_data/setdefault_data used by pipeline
    def get_data(self, path, default=None):
        node = self.shared
        for p in path:
            if not isinstance(node, dict) or p not in node:
                return default
            node = node[p]
        return node

    def setdefault_data(self, path, default=None):
        node = self.shared
        for p in path:
            node = node.setdefault(p, {})
        return node


ctx = DummyContext()

# Test write
dataname = 'ds_table_keys'
path = ['p1']
table_keys = {'sample': 'A', 'run': 1}
data = [[1, 2], [3, 4]]
meta = {'columns': ['a', 'b']}

saved_meta = pl.write_data_to_dataset(dataname, path, table_keys, ctx, data,
                                      meta)
print('saved_meta:', saved_meta)

# Read all
data_map, meta_map = pl.get_data_from(dataname,
                                      path,
                                      table_keys=None,
                                      context=ctx)
print('data_map keys:')
for k in sorted(data_map.keys()):
    print(repr(k))

print('meta_map keys:')
for k in sorted(meta_map.keys()):
    print(repr(k))

serial = json.dumps(table_keys, sort_keys=True, separators=(',', ':'))
print('expected serial:', serial)
found_keys = {k.split('_', 1)[0] for k in data_map.keys()}
print('found_keys:', found_keys)
