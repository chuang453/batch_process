import numpy as np
import os
from pathlib import Path
from utils.adapters.plot_helpers import generate_plot_style
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUT_DIR = Path('debug_logs')
OUT_DIR.mkdir(exist_ok=True)
OUT_PNG = OUT_DIR / 'test_generate_plot_style.png'

# Create sample data: 6 curves with two grouping keys
x = np.linspace(0, 10, 200)
# group A/B, series 1/2/3 to create collisions
groups = [
    {
        'type': 'A',
        'series': 1
    },
    {
        'type': 'A',
        'series': 2
    },
    {
        'type': 'A',
        'series': 1
    },
    {
        'type': 'B',
        'series': 1
    },
    {
        'type': 'B',
        'series': 2
    },
    {
        'type': 'C',
        'series': 1
    },
]

# build y values
ys = []
for i, g in enumerate(groups):
    phase = (g['series'] - 1) * 0.4
    amp = 1.0 + (0.2 * (ord(g['type'][0]) % 5))
    ys.append(np.sin(x * (0.5 + 0.15 * i) + phase) * amp + i * 0.2)

# Generate styles: explicitly map color by 'type' only
styles = generate_plot_style(groups, {'color_key': 'type'})  #

fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
for i, y in enumerate(ys):
    s = styles[i] if i < len(styles) else {}
    label = f"{groups[i]['type']}/{groups[i]['series']}"
    # pass style keys directly; filter unknowns
    allowed = {
        'color', 'linestyle', 'marker', 'markersize', 'markerfacecolor',
        'markeredgecolor', 'markeredgewidth', 'alpha'
    }
    kw = {k: v for k, v in s.items() if k in allowed}
    ax.plot(x, y, label=label, **kw)

ax.set_title('generate_plot_style visual test')
ax.legend(ncol=2, fontsize=8)
ax.grid(True)
fig.tight_layout()
fig.savefig(OUT_PNG)
print('Saved:', OUT_PNG)
