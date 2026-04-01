import pandas as pd

from stage2_platform.contracts import SeriesSpec, StageSpec
from stage2_platform.execution import Stage2Context, StageOrchestrator


def test_stage_orchestrator_runs_multiple_series():
    ctx = Stage2Context()
    ctx.catalog.put('df', pd.DataFrame({'a': [1, 2, 3], 'b': [3, 2, 1]}))
    stage = StageSpec(
        name='s1',
        source_key='df',
        series=[
            SeriesSpec(name='head1', input_key='df', output_key='out1', steps=[{'head': 1}]),
            SeriesSpec(name='tail1', input_key='df', output_key='out2', steps=[{'tail': 1}]),
        ],
    )

    records = StageOrchestrator().run_stage(stage, ctx)
    assert len(records) == 2
    assert records[0].status == 'success'
    assert records[1].status == 'success'
    assert len(ctx.catalog.get('out1')) == 1
    assert len(ctx.catalog.get('out2')) == 1


def test_stage_orchestrator_honors_fail_fast_series_flag():
    ctx = Stage2Context()
    ctx.catalog.put('df', pd.DataFrame({'a': [1, 2]}))
    stage = StageSpec(
        name='s1',
        source_key='df',
        series=[
            SeriesSpec(name='bad', input_key='df', output_key='out_bad', steps=[{'select': ['missing']}], continue_on_error=False),
            SeriesSpec(name='later', input_key='df', output_key='out_later', steps=[{'head': 1}]),
        ],
    )

    records = StageOrchestrator().run_stage(stage, ctx)
    assert len(records) == 1
    assert records[0].status == 'failed'
    assert not ctx.catalog.has('out_later')
