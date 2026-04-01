import pandas as pd

from stage2_platform.api import Stage2Service
from stage2_platform.ui.workers import ProjectWorker, SeriesWorker


def _project():
    return {
        'name': 'worker_demo',
        'inputs': [
            {
                'name': 'raw',
                'source_type': 'memory',
                'source_params': {'df': pd.DataFrame({'a': [1, 2, 3]})},
            }
        ],
        'stages': [
            {
                'name': 'clean',
                'type': 'data',
                'source': 'raw',
                'series': [
                    {'name': 'one', 'input_key': 'raw', 'output_key': 'out1', 'steps': [{'head': 1}]},
                    {'name': 'two', 'input_key': 'raw', 'output_key': 'out2', 'steps': [{'tail': 1}]},
                ],
            }
        ],
    }


def test_series_worker_emits_finished_signal():
    service = Stage2Service()
    service.set_project(_project())
    seen = []
    worker = SeriesWorker(service, 'clean', 'one')
    worker.series_started.connect(lambda name: seen.append(('start', name)))
    worker.series_finished.connect(lambda name, status: seen.append(('finish', name, status)))
    worker.run()

    assert seen[0] == ('start', 'one')
    assert seen[1][0] == 'finish'
    assert seen[1][2] == 'success'


def test_project_worker_emits_run_finished():
    service = Stage2Service()
    service.set_project(_project())
    service.context = service._prepare_context(service.project)
    worker = ProjectWorker(service)
    seen = []
    worker.run_started.connect(lambda run_id: seen.append(('run', run_id)))
    worker.series_started.connect(lambda name: seen.append(('series', name)))
    worker.run_finished.connect(lambda manifest: seen.append(('done', manifest.status)))
    worker.run()

    assert any(item[0] == 'run' for item in seen)
    assert any(item[0] == 'series' for item in seen)
    assert any(item[0] == 'done' and item[1] == 'done' for item in seen)
