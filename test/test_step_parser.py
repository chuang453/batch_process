from stage2_platform.execution.step_parser import parse_steps


def test_parse_steps_flat_and_nested_flattened():
    steps = [
        {'dropna': {}},
        {'group_by': ['g'], 'steps': [{'sort': {'by': 'x'}}, {'run': ['f1']}]},
    ]
    desc = parse_steps(steps, flatten=True)

    assert [d.step_id for d in desc] == ['1', '2', '2.1', '2.2']
    assert desc[0].op_type == 'builtin'
    assert desc[1].op_type == 'group_by'
    assert desc[2].op_name == 'sort'
    assert desc[3].op_type == 'transform'


def test_parse_steps_empty():
    assert parse_steps([], flatten=True) == []
    assert parse_steps(None, flatten=True) == []
