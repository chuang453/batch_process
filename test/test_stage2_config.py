import json

from stage2_platform.config.loader import load_project
from stage2_platform.config.normalizer import normalize_project
from stage2_platform.config.template import generate_stage2_template
from stage2_platform.config.validator import validate_project


def test_normalize_project_legacy_steps():
    raw = {
        'name': 'p',
        'stages': [
            {
                'name': 's1',
                'type': 'data',
                'source': 'df',
                'steps': [{'head': 1}],
            }
        ],
    }
    out = normalize_project(raw)
    assert out['stages'][0]['series'][0]['name'] == 'default'
    assert out['stages'][0]['series'][0]['steps'] == [{'head': 1}]


def test_validate_project_errors_and_ok():
    bad = {'name': 'x', 'inputs': [{'name': 'i1', 'source_type': 'bad', 'source_params': {}}]}
    errs = validate_project(normalize_project(bad))
    assert any('source_type unsupported' in e for e in errs)

    good = normalize_project({
        'name': 'x',
        'inputs': [{'name': 'i1', 'source_type': 'memory', 'source_params': {'df': []}}],
        'stages': [
            {
                'name': 's1',
                'type': 'data',
                'source': 'df',
                'series': [{'name': 'a', 'input_key': 'df', 'output_key': 'out', 'steps': [{'head': 1}]}],
            }
        ],
    })
    errs2 = validate_project(good)
    # memory adapter parameter type is validated at adapter level; schema-level should pass.
    assert errs2 == []


def test_load_project_and_template(tmp_path):
    p = tmp_path / 'project.yaml'
    generate_stage2_template(p)

    cfg = load_project(p)
    assert cfg['name']
    assert isinstance(cfg['inputs'], list)

    j = tmp_path / 'project.json'
    j.write_text(json.dumps({'name': 'j', 'inputs': [], 'stages': []}), encoding='utf-8')
    cfg2 = load_project(j)
    assert cfg2['name'] == 'j'
