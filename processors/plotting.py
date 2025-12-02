from pathlib import Path
from decorators.processor import processor
from core.engine import ProcessingContext

# matplotlib in headless mode
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3
import hashlib
import json

SCRIPT_DIR = Path(__file__).parent.resolve()


@processor(
    name="plot_from_spec",
    priority=40,
    from pathlib import Path
    from decorators.processor import processor
    from core.engine import ProcessingContext
    from processors._impl.plotting_impl import (
        prepare_plot_data_impl,
        plot_from_spec_impl,
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
        # collect inputs
        data = kwargs.get("data")
        data_key = kwargs.get("data_key")
        spec = kwargs.get("spec")
        out_dir = kwargs.get("out_dir")
        fmt = kwargs.get("fmt", "png")
        dpi = kwargs.get("dpi", 150)

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

        res = plot_from_spec_impl(target, data=data, spec=spec, out_dir=out_dir, fmt=fmt, dpi=dpi,
                                  base_style=context.get_data(["plot_style_base"], {}) or {})

        # record into context if success
        if res.get("status") == "success":
            context.setdefault_data(["plots"], []).append(res.get("figure_path"))
        context.add_result({"file": str(target), "processor": "plot_from_spec", **res})
        return res


    @processor(
        name="prepare_plot_data",
        priority=45,
        source=SCRIPT_DIR,
        metadata={
            "name": "Prepare Plot Data",
            "author": "ai-assistant",
            "version": "1.0",
            "description": "Wrapper processor: delegates to prepare_plot_data_impl",
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
                store[res["cache_key"]] = {"path": res["path"], "format": "parquet" if res["path"].endswith('.parquet') else 'csv'}

        context.add_result({"file": str(target), "processor": "prepare_plot_data", **res})
        return res
            try:
