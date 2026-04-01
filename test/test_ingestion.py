import json

import pandas as pd

from stage2_platform.contracts import DatasetRef
from stage2_platform.ingestion.loader import load_dataset


def test_memory_adapter_loads_dataframe():
    ref = DatasetRef(
        name='m1',
        source_type='memory',
        source_params={'df': pd.DataFrame({'a': [1, 2]})},
    )
    out = load_dataset(ref)
    assert list(out['a']) == [1, 2]


def test_file_adapter_csv(tmp_path):
    csv_path = tmp_path / 'x.csv'
    csv_path.write_text('a,b\n1,2\n3,4\n', encoding='utf-8')

    ref = DatasetRef(
        name='f1',
        source_type='file',
        source_params={'path': str(csv_path)},
    )
    out = load_dataset(ref)
    assert out.shape == (2, 2)


def test_stage1_artifact_adapter(tmp_path):
    artifact = tmp_path / 'artifact'
    artifact.mkdir(parents=True)
    data_csv = artifact / 'df.csv'
    data_csv.write_text('v\n10\n20\n', encoding='utf-8')

    metadata = {
        'datasets': {
            'df': {'file': 'df.csv', 'format': 'csv'}
        }
    }
    (artifact / 'metadata.json').write_text(json.dumps(metadata), encoding='utf-8')

    ref = DatasetRef(
        name='a1',
        source_type='stage1_artifact',
        source_params={'artifact_path': str(artifact), 'dataset': 'df'},
    )
    out = load_dataset(ref)
    assert list(out['v']) == [10, 20]


def test_unsupported_source_type():
    ref = DatasetRef(name='bad', source_type='unknown', source_params={})
    try:
        load_dataset(ref)
        assert False, 'expected ValueError'
    except ValueError as exc:
        assert 'unsupported source_type' in str(exc)
