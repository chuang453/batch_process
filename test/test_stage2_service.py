import json

import pandas as pd

from stage2_platform.api import Stage2Service
from stage2_platform.cli.app import main as cli_main


def _project_dict():
    return {
        'name': 'svc_demo',
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
                        'name': 'keep_one',
                        'input_key': 'raw',
                        'output_key': 'raw_one',
                        'steps': [{'dropna': {'subset': ['a']}}, {'head': 1}],
                    },
                    {
                        'name': 'keep_tail',
                        'input_key': 'raw',
                        'output_key': 'raw_tail',
                        'steps': [{'tail': 1}],
                    },
                ],
            }
        ],
    }


def test_stage2_service_simulate_and_run_paths():
    service = Stage2Service()
    service.set_project(_project_dict())

    sim = service.simulate()
    assert sim['stages'][0]['stage_name'] == 'clean'
    assert sim['stages'][0]['series'][0]['series_name'] == 'keep_one'

    record = service.run_series('clean', 'keep_one')
    assert record.status == 'success'
    assert service.context.catalog.has('raw_one')

    records = service.run_stage('clean')
    assert len(records) == 2
    assert service.context.catalog.has('raw_tail')

    manifest = service.run_project()
    assert manifest.status == 'done'
    assert len(manifest.series_records) == 2


def test_stage2_service_load_validate_and_status(tmp_path):
    project = {
        'name': 'svc_file',
        'inputs': [],
        'stages': [],
    }
    path = tmp_path / 'project.json'
    path.write_text(json.dumps(project), encoding='utf-8')

    service = Stage2Service()
    loaded = service.load_project(str(path))
    assert loaded['name'] == 'svc_file'
    assert service.validate_project() == []
    assert service.get_run_status() is None


def test_stage2_cli_validate_simulate_and_list_ops(tmp_path, capsys):
    project = {
        'name': 'cli_demo',
        'inputs': [],
        'stages': [],
    }
    path = tmp_path / 'project.json'
    path.write_text(json.dumps(project), encoding='utf-8')

    assert cli_main(['validate', str(path)]) == 0
    captured = capsys.readouterr().out
    assert '"ok": true' in captured.lower()

    assert cli_main(['simulate', str(path)]) == 0
    captured = capsys.readouterr().out
    assert '"name": "cli_demo"' in captured

    assert cli_main(['list-ops']) == 0
    captured = capsys.readouterr().out
    assert 'builtin_ops' in captured
