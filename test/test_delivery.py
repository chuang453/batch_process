import zipfile

import pandas as pd

from stage2_platform.delivery import BundleWriter, FileWriter, MemoryWriter


def test_memory_writer_policies():
    target = {}
    w = MemoryWriter(target)
    df = pd.DataFrame({'a': [1]})

    k1 = w.write('x', df, {'policy': 'overwrite'})
    k2 = w.write('x', df, {'policy': 'versioned'})
    assert k1 == 'x'
    assert k2.startswith('x')
    assert len(target) == 2


def test_file_writer_csv(tmp_path):
    w = FileWriter()
    df = pd.DataFrame({'a': [1, 2]})
    out = w.write('demo', df, {'output_dir': str(tmp_path), 'format': 'csv'})

    assert out.endswith('.csv')


def test_bundle_writer_creates_zip(tmp_path):
    w = BundleWriter()
    df = pd.DataFrame({'a': [1, 2]})
    out = w.write('demo', df, {'output_dir': str(tmp_path)})

    with zipfile.ZipFile(out, 'r') as zf:
        names = set(zf.namelist())
        assert 'metadata.json' in names
        assert any(name.startswith('demo.') for name in names)
