"""Small demo: show using the processor `plot_from_spec` with a closure
extractor that returns meta, and demonstrate how a downstream processor
would read extractor meta from the `ProcessingContext`.

Behavior & storage
- The injected `extract_f` can return either a DataFrame-like object or
    a tuple `(df, meta)` where `meta` is an arbitrary dict (e.g. cache keys,
    row counts, source info).
- `plot_from_spec_impl` collects extractor meta and returns it under
    `res['extract_meta']` (list). The `plot_from_spec` processor converts
    that list into a mapping keyed by the series label (fallback
    `subplot_<index>`) and stores it into the processing context at
    `context.data['plot_extract_meta'][str(target)]` where `target` is the
    `Path` passed to the processor.

Downstream usage
- The provided downstream processor `write_plot_extract_summary` reads
    `context.data['plot_extract_meta'][str(path)]` and writes a JSON
    summary file (by default under `summary_dir`) so other processors or
    humans can inspect extractor metadata.

Run:
    python -m demos.demo_extract_closure_demo

"""
from pathlib import Path
import pandas as pd
from processors.plotting import plot_from_spec
from decorators.processor import ProcessingContext
from processors.builtin_recorders import write_plot_extract_summary

OUT_DIR = Path("demos/demo_extract_output")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Shared DataFrame
df = pd.DataFrame({
    "x": list(range(12)),
    "y": [i * 1.5 for i in range(12)],
    "group": ["A"] * 6 + ["B"] * 6,
})


# Closure extractor that filters by group and returns meta (cache_key)
def make_extractor(shared_df):

    def extractor(series, data, target):
        # per-series inline data overrides
        if series.get("data") is not None:
            return pd.DataFrame(series.get("data")), {"source": "inline"}
        grp = series.get("group")
        if grp:
            subset = shared_df[shared_df["group"] == grp]
            meta = {"cache_key": f"group_{grp}", "rows": len(subset)}
            return subset, meta
        # default: return full df with meta
        return shared_df, {"cache_key": "full", "rows": len(shared_df)}

    return extractor


def run_demo():
    extractor = make_extractor(df)

    spec = {
        "layout": {
            "rows": 1,
            "cols": 1
        },
        "subplots": [{
            "title":
            "Group A",
            "series": [{
                "x": "x",
                "y": "y",
                "group": "A",
                "label": "A"
            }]
        }],
        "save": {
            "filename": "extract_demo.png"
        }
    }

    ctx = ProcessingContext()
    target = Path("demo_target")
    res = plot_from_spec(target,
                         ctx,
                         data=None,
                         spec=spec,
                         out_dir=str(OUT_DIR),
                         extract_f=extractor)
    print("Result:", res)
    print("Written file:", res.get("figure_path"))

    # Demonstrate downstream processor reading extractor meta from context
    meta = ctx.get_data(["plot_extract_meta", str(target)])
    print(
        "Context plot_extract_meta for target (mapping keyed by series label):"
    )
    if isinstance(meta, dict):
        for k, v in meta.items():
            print(f" - {k}: {v}")
    else:
        print(meta)

        # Example: call downstream processor to write a summary file
        summary_res = write_plot_extract_summary(target,
                                                 ctx,
                                                 summary_dir=str(OUT_DIR))
        print("write_plot_extract_summary ->", summary_res)


if __name__ == '__main__':
    run_demo()
