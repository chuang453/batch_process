from pathlib import Path
import pandas as pd

from utils.adapters.plot_helpers import plot_from_spec_adapter


def main():
    # target is used by the adapter to derive a default output path if none provided
    target = Path(__file__)

    # Example data
    df = pd.DataFrame({
        "x": list(range(10)),
        "y1": [i * 1.5 for i in range(10)],
        "y2": [i**1.2 for i in range(10)],
        "y3": [5 + ((-1)**i) * i for i in range(10)],
    })

    # A spec that demonstrates spanning and multiple subplots
    spec = {
        "title":
        "plot_from_spec_adapter Demo",
        "layout": {
            "rows": 2,
            "cols": 2
        },
        "subplots": [
            {
                "row": 0,
                "col": 0,
                "rowspan": 2,
                "colspan": 1,
                "title": "Y1 (spanning)",
                "series": [{
                    "x": "x",
                    "y": "y1",
                    "label": "Y1",
                    "style": "-"
                }],
                "legend": True,
            },
            {
                "row": 0,
                "col": 1,
                "title": "Y2",
                "series": [{
                    "x": "x",
                    "y": "y2",
                    "label": "Y2",
                    "style": "--"
                }],
            },
            {
                "row": 1,
                "col": 1,
                "title": "Y3",
                "series": [{
                    "x": "x",
                    "y": "y3",
                    "label": "Y3",
                    "style": ":"
                }],
            },
        ],
        "save": {
            "filename": "adapter_demo.png",
            "dpi": 150
        },
    }

    out_dir = Path("demos") / "demo_plot_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Call the adapter
    res = plot_from_spec_adapter(target,
                                 data=df,
                                 spec=spec,
                                 out_dir=str(out_dir),
                                 fmt="png",
                                 dpi=150)

    print("plot_from_spec_adapter result:", res)
    if res.get("status") == "success":
        print("Figure written to:", res.get("figure_path"))


if __name__ == "__main__":
    main()
