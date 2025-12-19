from pprint import pprint
import traceback


def load_generate_plot_style():
    try:
        # try normal import first
        from utils.adapters.plot_helpers import generate_plot_style
        return generate_plot_style
    except Exception:
        # fallback: matplotlib not available or top-level import failed
        try:
            src_path = 'd:/git_reposity/batch_process/utils/adapters/plot_helpers.py'
            with open(src_path, 'r', encoding='utf-8') as f:
                src = f.read()

            # remove matplotlib/font_manager import lines
            lines = []
            for ln in src.splitlines():
                s = ln.strip()
                if s.startswith('import matplotlib'
                                ) or 'font_manager' in s or s.startswith(
                                    'from matplotlib'):
                    continue
                lines.append(ln)
            sanitized = '\n'.join(lines)

            ns = {'__name__': '__main__'}
            import numpy as np
            import colorsys
            ns['np'] = np
            ns['colorsys'] = colorsys
            exec(sanitized, ns)
            return ns['generate_plot_style']
        except Exception:
            traceback.print_exc()
            raise


def run():
    generate_plot_style = load_generate_plot_style()

    tests = []

    g1 = {'type': ['A', 'A', 'B'], 'series': [1, 2, 1]}
    tests.append(('dict_of_lists_default', g1, None))

    g2 = [
        {
            'type': 'A',
            'series': 1
        },
        {
            'type': 'A',
            'series': 2
        },
        {
            'type': 'B',
            'series': 1
        },
    ]
    tests.append(('list_of_dicts_default', g2, None))

    # explicit color_key only
    tests.append(('dict_color_key', g1, {'color_key': 'type'}))

    # explicit color + linestyle
    tests.append(('dict_color_linestyle', g1, {
        'color_key': 'type',
        'linestyle_key': 'series'
    }))

    # explicit marker_map provided
    tests.append(('dict_marker_map', g1, {
        'marker_map': {
            'A': {
                'marker': 'o'
            },
            'B': {
                'marker': 's'
            }
        }
    }))

    for name, g, opts in tests:
        print('\n---', name, 'opts=', opts)
        try:
            res = generate_plot_style(g, opts)
            pprint(res)
        except Exception:
            traceback.print_exc()


if __name__ == '__main__':
    run()
