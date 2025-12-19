import numpy as np
from utils.adapters import plot_helpers as ph


def test_generate_plot_style_dict_of_lists_basic():
    group = {'grp': ['a', 'b', 'a'], 'type': [1, 1, 2]}
    styles = ph.generate_plot_style(group)
    assert isinstance(styles, list)
    assert len(styles) == 3
    for s in styles:
        assert isinstance(s, dict)
        # color should be present and a tuple
        assert 'color' in s
        assert isinstance(s['color'], tuple)


def test_generate_plot_style_list_of_dicts_equiv():
    rows = [
        {
            'grp': 'a',
            'type': 1
        },
        {
            'grp': 'b',
            'type': 1
        },
        {
            'grp': 'a',
            'type': 2
        },
    ]
    styles = ph.generate_plot_style(rows)
    assert isinstance(styles, list)
    assert len(styles) == 3
    # ensure outputs are comparable: same number of colors and linestyles
    d_styles = ph.generate_plot_style({
        'grp': ['a', 'b', 'a'],
        'type': [1, 1, 2]
    })
    assert len(d_styles) == len(styles)
    for s in styles:
        assert 'color' in s and isinstance(s['color'], tuple)


def test_generate_plot_style_with_options_keys():
    # specify which key maps to color explicitly
    rows = [{'k': 'x'}, {'k': 'y'}, {'k': 'x'}]
    styles = ph.generate_plot_style(rows, {'color_key': 'k'})
    assert len(styles) == 3
    # colors for 'x' should equal for indices 0 and 2
    assert styles[0]['color'] == styles[2]['color']


def test_generate_plot_style_empty_inputs():
    assert ph.generate_plot_style([]) == []
    assert ph.generate_plot_style({'a': []}) == []


def test_generate_plot_style_unequal_lengths_dict_of_lists():
    # shorter lists should be padded with None
    group = {'g': ['a', 'b'], 't': [1]}
    styles = ph.generate_plot_style(group)
    assert len(styles) == 2
    # second row should have t == None
    # generate_plot_style does not expose raw rows, but should produce two styles
    assert isinstance(styles[1], dict)


def test_generate_plot_style_list_of_dicts_missing_keys():
    rows = [{'a': 1}, {'b': 2}, {}]
    styles = ph.generate_plot_style(rows)
    assert len(styles) == 3
    for s in styles:
        assert isinstance(s, dict)
