import numpy as np
from utils.adapters import plot_helpers as ph


def extract_f(param):
    if isinstance(param, str) and param == 'x':
        return np.arange(20)
    if isinstance(param, str) and param == 'y':
        return np.arange(20) * 1.5
    # support returning (data, meta)
    return (np.arange(5), {'meta': True})


# test generate_plot_style
group = {'g1': ['a', 'a', 'b'], 'g2': [1, 2, 1]}
styles = ph.generate_plot_style(group)
print('generate_plot_style produced', len(styles), 'styles')

# test generic_plot with a simple spec
spec = {
    'subplots': [{
        'pos': (1, 1, 1),
        'title':
        'quick test',
        'lines': [{
            'x': 'x',
            'y': ['y', 'series1', styles[0] if styles else {}]
        }, {
            'x': 'x',
            'y': ['y', 'series2', styles[1] if len(styles) > 1 else {}]
        }]
    }],
    'save_path':
    'debug_logs/quick_test_plot.png'
}

fig = ph.generic_plot(extract_f, spec, {
    'return_figure': True,
    'tight_layout': True
})
print('generic_plot returned figure:', isinstance(fig, object))
