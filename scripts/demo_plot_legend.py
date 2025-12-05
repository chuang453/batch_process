from pathlib import Path
import os
# force non-interactive Agg backend for safety in headless/background runs
os.environ.setdefault('MPLBACKEND', 'Agg')
os.environ.setdefault('MPLCONFIGDIR', str(Path('debug_logs') / 'mplconfig'))
from utils.adapters.plot_helpers import generic_plot

OUTPUT = Path('debug_logs') / 'legend_demo.png'
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def extract_f(param):
    if param == 'x':
        return list(range(50))
    if isinstance(param, str) and param.startswith('y'):
        i = int(param[1:])
        return [j + i * 0.1 for j in range(50)]
    return []


subplots = [{
    "pos": (1, 1, 1),
    "title":
    "many-lines test",
    "lines": [{
        "x": "x",
        "y": (f"y{i}", f"line{i}", {})
    } for i in range(20)]
}]

plot_spec = {"subplots": subplots, "save_path": str(OUTPUT)}

try:
    fig = generic_plot(
        extract_f,
        plot_spec,
        plot_style={
            "legend_threshold": 1,
            "legend_position": "bottom",
            "debug": True,
        },
    )
    print(f"Saved demo plot to: {OUTPUT.resolve()}")
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Failed to create plot: {e}")
    raise
