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


def test_pipeline_simulate_data_step_fields(tmp_path):
    """Data steps in sequence mode carry data_step_id, data_step_type, data_op_name, data_level."""
    stages = [
        {
            'name': 'ds1',
            'type': 'data',
            'steps': [
                {'dropna': {}},
                {'group_by': ['col'], 'steps': [{'sort': {'by': 'x'}}]},
            ],
        }
    ]
    p = Pipeline(stages=stages)
    seq = p.simulate(root_path=tmp_path, sequence=True)
    steps = seq['steps']

    # flat: dropna(1) + group_by(2) + sub sort(2.1) = 3 steps
    assert len(steps) == 3

    # first step: builtin_op
    assert steps[0]['data_step_id'] == '1'
    assert steps[0]['data_step_type'] == 'builtin_op'
    assert steps[0]['data_op_name'] == 'dropna'
    assert steps[0]['data_level'] == 0

    # second step: group_by parent
    assert steps[1]['data_step_id'] == '2'
    assert steps[1]['data_step_type'] == 'group_by'
    assert steps[1]['data_level'] == 0

    # third step: nested sub-step
    assert steps[2]['data_step_id'] == '2.1'
    assert steps[2]['data_step_type'] == 'builtin_op'
    assert steps[2]['data_level'] == 1
