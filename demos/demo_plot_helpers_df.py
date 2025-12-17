"""Demo: use utils.adapters.plot_helpers to plot a DataFrame

This script creates a small DataFrame and demonstrates two ways to
produce a PNG plot using functions in `utils.adapters.plot_helpers`:

1. `generic_plot` with a simple `extract_f` that reads from an in-memory
   DataFrame.
2. `plot_from_spec_adapter` as a higher-level adapter (may return an
   error dict if the processor implementation is not available in this
   environment).

Run:
    python demos/demo_plot_helpers_df.py

Output:
    out/demo_plot_generic.png  (created by generic_plot)
    optionally other files if adapter produces them
"""
from pathlib import Path
import math
import pandas as pd

from utils.adapters import plot_helpers

OUT_DIR = Path("out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# create example DataFrame
n = 60
dates = pd.date_range("2023-01-01", periods=n, freq="D")
df = pd.DataFrame({
    "date":
    dates,
    "series_a": (pd.Series(range(n)) +
                 (pd.Series(range(n)).apply(lambda x: math.sin(x / 6)))),
    "series_b": (pd.Series(range(n)) +
                 (pd.Series(range(n)).apply(lambda x: math.cos(x / 8))))
})


def extract_f_from_df(param):
    """Simple extractor used by generic_plot.

    Supported param values:
    - 'index' -> returns the DataFrame index (datetime objects)
    - column name (str) -> returns column values as a list
    """
    if isinstance(param, str):
        if param == 'index':
            return df['date'].tolist()
        if param in df.columns:
            return df[param].tolist()
        raise KeyError(f"unknown param: {param}")
    raise TypeError("param must be a string column name or 'index'")


def run_generic_plot():
    spec = {
        "subplots": [{
            "pos": (1, 1, 1),
            "title":
            "Demo: series_a / series_b",
            "xlabel":
            "date",
            "ylabel":
            "value",
            "lines": [
                {
                    "x": "index",
                    "y": ["series_a", "series_a"]
                },
                {
                    "x": "index",
                    "y": ["series_b", "series_b"]
                },
            ],
        }],
        "save_path":
        str(OUT_DIR / "demo_plot_generic.png")
    }

    style = {"figsize": (10, 4), "grid": True, "tight_layout": True}

    # generic_plot closes the figure after saving
    plot_helpers.generic_plot(extract_f_from_df, spec, plot_style=style)
    print("Wrote:", OUT_DIR / "demo_plot_generic.png")


def run_adapter_plot():
    # plot_from_spec_adapter accepts a `data` argument; pass the DataFrame
    spec = {
        "subplots": [{
            "pos": (1, 1, 1),
            "title": "Adapter demo: series_a",
            "xlabel": "date",
            "ylabel": "value",
            "lines": [
                {
                    "x": "date",
                    "y": ["series_a", "series_a"]
                },
            ],
        }]
    }
    target = Path("plots/adapter_demo")
    try:
        res = plot_helpers.plot_from_spec_adapter(target,
                                                  data=df,
                                                  spec=spec,
                                                  out_dir=str(OUT_DIR),
                                                  fmt='png')
        print("Adapter result:", res)
    except Exception as e:
        print("Adapter call failed:", e)


if __name__ == '__main__':
    run_generic_plot()
    run_adapter_plot()
