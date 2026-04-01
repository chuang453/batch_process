import pandas as pd

from decorators.processor import ProcessingContext
from stage1_bridge import export_artifact
from stage2_platform.contracts import DatasetRef
from stage2_platform.ingestion.loader import load_dataset


def test_stage1_bridge_export_and_stage2_artifact_load(tmp_path):
    ctx = ProcessingContext()
    ctx.set_main('df', pd.DataFrame({'a': [1, 2], 'b': [3, 4]}))

    artifact = export_artifact(ctx, tmp_path / 'artifact')
    ref = DatasetRef(
        name='df',
        source_type='stage1_artifact',
        source_params={'artifact_path': str(artifact), 'dataset': 'df'},
    )
    out = load_dataset(ref)

    assert list(out['a']) == [1, 2]
