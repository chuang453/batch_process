import pandas as pd

from stage2_platform.contracts import SeriesSpec
from stage2_platform.execution import SeriesExecutor, Stage2Context


def test_series_executor_runs_and_routes_output():
    ctx = Stage2Context()
    ctx.catalog.put('df', pd.DataFrame({'a': [1, None, 3], 'b': [3, 2, 1]}))

    spec = SeriesSpec(
        name='clean',
        input_key='df',
        output_key='df_clean',
        steps=[{'dropna': {'subset': ['a']}}, {'sort': {'by': 'b', 'ascending': True}}],
    )

    manifest = SeriesExecutor().run(spec, ctx)
    out = ctx.catalog.get('df_clean')

    assert manifest.status == 'success'
    assert list(out['b']) == [1, 3]


def test_series_executor_missing_input_fails():
    ctx = Stage2Context()
    spec = SeriesSpec(name='x', input_key='missing', output_key='out', steps=[{'head': 1}])

    manifest = SeriesExecutor().run(spec, ctx)
    assert manifest.status == 'failed'
    assert 'input dataset not found' in manifest.error
