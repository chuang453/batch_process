from core.pipeline import Pipeline


def test_pipeline_simulate_non_sequence_returns_actions_shape(tmp_path):
    stages = [
        {
            'name': 'data_stage',
            'type': 'data',
            'source': 'df',
            'steps': [{'head': 1}],
        }
    ]
    p = Pipeline(stages=stages)

    actions = p.simulate(root_path=tmp_path, max_items=1000)

    assert isinstance(actions, list)
    assert actions
    first = actions[0]
    assert 'path' in first
    assert 'processors' in first


def test_pipeline_simulate_sequence_returns_step_dict(tmp_path):
    stages = [
        {
            'name': 'data_stage',
            'type': 'data',
            'source': 'df',
            'steps': [{'head': 1}, {'tail': 1}],
        }
    ]
    p = Pipeline(stages=stages)

    seq = p.simulate(root_path=tmp_path, sequence=True)

    assert isinstance(seq, dict)
    assert 'total_steps' in seq
    assert 'steps' in seq
    assert seq['total_steps'] == len(seq['steps'])
    if seq['steps']:
        row = seq['steps'][0]
        assert 'step' in row
        assert 'phase' in row
        assert 'proc_name' in row
