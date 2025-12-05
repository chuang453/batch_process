print('DEBUG START')
try:
    print('about to import generic_plot')
    from utils.adapters.plot_helpers import generic_plot
    print('imported generic_plot')

    def extract_f(param):
        if param == 'x': return [0, 1, 2]
        if isinstance(param, str) and param.startswith('y'):
            i = int(param[1:])
            return [0, 1, 2]
        return []

    subplots = [{
        "pos": (1, 1, 1),
        "lines": [{
            "x": "x",
            "y": ("y0", "l0", {})
        }]
    }]
    print('about to call generic_plot')
    generic_plot(extract_f, {
        "subplots": subplots,
        "save_path": "debug_logs/test.png"
    },
                 plot_style={
                     "legend_threshold": 1,
                     "legend_position": "bottom"
                 })
    print('generic_plot returned')
except Exception as e:
    import traceback
    traceback.print_exc()
    print('EXC', e)
print('DEBUG END')
