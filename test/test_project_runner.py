import pandas as pd

from stage2_platform.execution import ProjectRunner, Stage2Context


def test_project_runner_loads_memory_input_and_runs():
    project = {
        'name': 'demo',
        'inputs': [
            {
                'name': 'raw',
                'source_type': 'memory',
                'source_params': {'df': pd.DataFrame({'a': [1, None, 3], 'b': [3, 2, 1]})},
            }
        ],
        'stages': [
            {
                'name': 'clean',
                'type': 'data',
                'source': 'raw',
                'series': [
                    {
                        'name': 'default',
                        'input_key': 'raw',
                        'output_key': 'raw_clean',
                        'steps': [{'dropna': {'subset': ['a']}}, {'head': 1}],
                    }
                ],
            }
        ],
    }

    ctx = Stage2Context()
    manifest = ProjectRunner().run(project, ctx)
    out = ctx.catalog.get('raw_clean')

    assert manifest.status == 'done'
    assert len(manifest.series_records) == 1
    assert len(out) == 1


def test_project_runner_cancel_stops_after_current_stage():
    project = {
        'name': 'demo',
        'inputs': [
            {
                'name': 'raw',
                'source_type': 'memory',
                'source_params': {'df': pd.DataFrame({'a': [1, 2, 3]})},
            }
        ],
        'stages': [
            {
                'name': 's1',
                'type': 'data',
                'source': 'raw',
                'series': [{'name': 'one', 'input_key': 'raw', 'output_key': 'out1', 'steps': [{'head': 1}]}],
            },
            {
                'name': 's2',
                'type': 'data',
                'source': 'raw',
                'series': [{'name': 'two', 'input_key': 'raw', 'output_key': 'out2', 'steps': [{'tail': 1}]}],
            },
        ],
    }

    runner = ProjectRunner()
    seen = []

    def observer(kind, payload):
        seen.append(kind)
        if kind == 'series_finished':
            runner.cancel()

    ctx = Stage2Context()
    manifest = runner.run(project, ctx, observer=observer)

    assert manifest.status == 'cancelled'
    assert ctx.catalog.has('out1')
    assert not ctx.catalog.has('out2')
