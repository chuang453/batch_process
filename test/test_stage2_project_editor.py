import pandas as pd
import pytest

from stage2_platform.ui.project_editor import (
    get_source_param_template,
    parse_list_text,
    parse_mapping_text,
    serialize_project_for_disk,
)


def test_project_editor_parses_json_and_yaml_fragments():
    assert parse_mapping_text('{"path": "demo.csv"}', 'params')['path'] == 'demo.csv'
    assert parse_mapping_text('path: demo.csv', 'params')['path'] == 'demo.csv'
    assert parse_list_text('- head: 5\n- dropna: {subset: [a]}', 'steps')[0]['head'] == 5


def test_project_editor_rejects_memory_dataframe_serialization():
    project = {
        'name': 'demo',
        'inputs': [
            {
                'name': 'raw',
                'source_type': 'memory',
                'source_params': {'df': pd.DataFrame({'a': [1]})},
            }
        ],
        'stages': [],
    }

    with pytest.raises(ValueError):
        serialize_project_for_disk(project)


def test_project_editor_source_templates_are_copy_safe():
    tpl = get_source_param_template('api')
    assert tpl['url'] == ''
    tpl['url'] = 'https://example.com'
    assert get_source_param_template('api')['url'] == ''