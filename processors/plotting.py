"""Processors wrapper for plotting utilities.

This module exposes processor wrappers that delegate to the pure
implementation in `processors._impl.plotting_impl` via the adapter
layer in `utils.adapters.plot_helpers`.

Notable behavior
- The `plot_from_spec` processor accepts an optional `extract_f`
    callable in `kwargs`. `extract_f(series, data, target)` may return
    either a DataFrame-like object or a tuple `(df, meta)`. When provided,
    the extractor is called for each series to obtain its DataFrame and
    optional `meta` dict (for example cache keys or provenance).

- `plot_from_spec_impl` will collect extractor meta for all series and
    return it under `res['extract_meta']` (a list). The processor then
    converts this list into a mapping keyed by the series label
    (fallback to `subplot_<index>`) and stores it in the processing
    context at `context.data['plot_extract_meta'][str(target)]`.

Downstream processors can read that mapping to access per-series
metadata produced by extractors.
"""
from pathlib import Path
from decorators.processor import processor
from core.engine import ProcessingContext
from utils.adapters.plot_helpers import (
    prepare_plot_data_adapter as prepare_plot_data_impl,
    plot_from_spec_adapter as plot_from_spec_impl,
)

SCRIPT_DIR = Path(__file__).parent.resolve()


@processor(
    name="plot_from_spec",
    priority=40,
    source=SCRIPT_DIR,
    metadata={
        "name": "Plot From Spec",
        "author": "ai-assistant",
        "version": "1.0",
        "description": "Wrapper processor: delegates to plot_from_spec_impl",
    },
)
def plot_from_spec(target: Path, context: ProcessingContext, **kwargs):
    """Processor wrapper that creates plots from a spec.

    Parameters (kwargs accepted):
    - data: optional DataFrame or data convertible to DataFrame used as
        the default data for series that do not specify inline data.
    - data_key: key to look up prepared plot data in
        `context.data['plot_data']`.
    - spec: dict describing layout, subplots and save options (passed to
        the implementation).
    - out_dir, fmt, dpi: output file options forwarded to the impl.
    - extract_f: optional callable `extract_f(series, data, target)` that
        returns either a DataFrame-like object or a tuple `(df, meta)`.
    Behavior:
    - If `extract_f` is provided it will be used to obtain per-series
        DataFrames and optional metadata. The implementation collects all
        returned `meta` dicts and returns them under `res['extract_meta']`.
    - This processor converts the returned `extract_meta` list into a
        mapping keyed by series label (fallback to `subplot_<index>`) and
        stores it into `context.data['plot_extract_meta'][str(target)]` as
        a dict of lists. Downstream processors can read that location to
        access extractor metadata.
    Returns the same dict returned by the implementation (including
    `status`, `figure_path`, `warnings`, and optionally `extract_meta`).
    """
    # collect inputs
    data = kwargs.get("data")
    data_key = kwargs.get("data_key")
    spec = kwargs.get("spec")
    out_dir = kwargs.get("out_dir")
    fmt = kwargs.get("fmt", "png")
    dpi = kwargs.get("dpi", 150)
    extract_f = kwargs.get("extract_f")

    # if data_key provided, load from context
    if data is None and data_key:
        store = context.get_data(["plot_data"], {}) or {}
        entry = store.get(data_key)
        if isinstance(entry, dict) and "path" in entry:
            p = Path(entry["path"])
            if p.suffix in (".parquet", ".pq"):
                try:
                    import pandas as _pd

                    data = _pd.read_parquet(p)
                except Exception:
                    data = None
            else:
                import pandas as _pd

                data = _pd.read_csv(p)
        else:
            data = entry

    res = plot_from_spec_impl(
        target,
        data=data,
        spec=spec,
        out_dir=out_dir,
        fmt=fmt,
        dpi=dpi,
        base_style=context.get_data(["plot_style_base"], {}) or {},
        extract_f=extract_f)

    # record into context if success
    if res.get("status") == "success":
        context.setdefault_data(["plots"], []).append(res.get("figure_path"))
        # Persist any extractor meta returned by the impl so downstream
        # processors can access it. Stored under data['plot_extract_meta'][<target>]
        if "extract_meta" in res:
            # Convert list of extract_meta entries into a mapping keyed by
            # series label (fallback to subplot index) for easier lookup by
            # downstream processors. If multiple entries share the same key,
            # store them in a list.
            meta_list = res.get("extract_meta") or []
            meta_map = {}
            for ent in meta_list:
                key = ent.get(
                    "series_label") or f"subplot_{ent.get('subplot_index')}"
                if key in meta_map:
                    # append to existing list
                    if isinstance(meta_map[key], list):
                        meta_map[key].append(ent.get("meta"))
                    else:
                        meta_map[key] = [meta_map[key], ent.get("meta")]
                else:
                    # store as single-item list for consistency
                    meta_map[key] = [ent.get("meta")]

            meta_store = context.setdefault_data(["plot_extract_meta"], {})
            try:
                meta_store[str(target)] = meta_map
            except Exception:
                # non-fatal
                pass
    context.add_result({
        "file": str(target),
        "processor": "plot_from_spec",
        **res
    })
    return res


@processor(
    name="prepare_plot_data",
    priority=45,
    source=SCRIPT_DIR,
    metadata={
        "name": "Prepare Plot Data",
        "author": "ai-assistant",
        "version": "1.0",
        "description":
        "Wrapper processor: delegates to prepare_plot_data_impl",
    },
)
def prepare_plot_data(target: Path, context: ProcessingContext, **kwargs):
    # pull args from kwargs
    cache_key = kwargs.get("cache_key")
    db_url = kwargs.get("db_url")
    query = kwargs.get("query")
    csv_path = kwargs.get("csv_path")
    data = kwargs.get("data")
    to_disk = kwargs.get("to_disk", False)
    force = kwargs.get("force", False)
    encoding = kwargs.get("encoding", "utf-8")

    res = prepare_plot_data_impl(target,
                                 cache_key=cache_key,
                                 db_url=db_url,
                                 query=query,
                                 csv_path=csv_path,
                                 data=data,
                                 to_disk=to_disk,
                                 force=force,
                                 encoding=encoding)

    # store result in context
    store = context.setdefault_data(["plot_data"], {})
    if res.get("status") == "cached":
        if "df" in res:
            store[res["cache_key"]] = res["df"]
        elif "path" in res:
            store[res["cache_key"]] = {
                "path": res["path"],
                "format":
                "parquet" if res["path"].endswith('.parquet') else 'csv'
            }

    context.add_result({
        "file": str(target),
        "processor": "prepare_plot_data",
        **res
    })
    return res
