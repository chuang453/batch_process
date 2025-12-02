from pathlib import Path
import pandas as pd
import sqlite3
import hashlib

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt


def _ensure_df(maybe_df_or_data):
    if maybe_df_or_data is None:
        return None
    if isinstance(maybe_df_or_data, pd.DataFrame):
        return maybe_df_or_data
    try:
        return pd.DataFrame(maybe_df_or_data)
    except Exception:
        return None


def prepare_plot_data_impl(target: Path,
                           *,
                           cache_key: str = None,
                           db_url: str = None,
                           query: str = None,
                           csv_path: str = None,
                           data=None,
                           to_disk: bool = False,
                           force: bool = False,
                           encoding: str = 'utf-8') -> dict:
    """Prepare / collect data for plotting (pure function, no context).

    This function centralizes reading/preparing tabular data for plotting.
    It accepts several mutually-compatible input sources and returns a
    standardized dict describing the result. The returned dict has the
    following shapes:

    - On success with an in-memory DataFrame:
        {
            "status": "cached",
            "cache_key": "...",
            "rows": <int>,
            "df": <pandas.DataFrame>
        }

    - When `to_disk=True` and data is persisted to a file:
        {
            "status": "cached",
            "cache_key": "...",
            "rows": <int>,
            "path": "<path to file>"
        }

    - When no data source is found:
        {"status": "skipped", "reason": "no data source found"}

    - On error:
        {"status": "error", "error": "..."}

    Parameters
    - target: Path used as a reference (typically the file being processed);
      used to choose a sensible default output directory when persisting.
    - cache_key: optional string to identify cached data; generated from
      `target` + inputs if omitted.
    - db_url / query: when provided together, run the SQL `query` against
      `db_url` (supports `sqlite:///` and SQLAlchemy URLs).
    - csv_path: read CSV if provided and exists.
    - data: in-memory data (pandas DataFrame or any structure accepted by
      `pd.DataFrame`). If present, this takes precedence.
    - to_disk: if True, persist the prepared data under
      `Path(target).parent / 'plot_data'` returning its path.
    - force / encoding: auxiliary options.

    Examples
    - Prepare from an in-memory DataFrame::

        >>> df = pd.DataFrame({"x": [1,2,3], "y": [4,5,6]})
        >>> prepare_plot_data_impl(Path("/tmp/foo.txt"), data=df)
        {"status": "cached", "cache_key": "...", "rows": 3, "df": <DataFrame>}

    - Read from CSV and save to disk::

        >>> prepare_plot_data_impl(Path("/tmp/foo.txt"), csv_path="/data/my.csv", to_disk=True)
        {"status": "cached", "cache_key": "...", "rows": 1000, "path": "/tmp/plot_data/<key>.parquet"}

    The function is intentionally pure and returns DataFrame objects when
    possible so callers (or tests) can inspect the data without file IO.
    """
    if not cache_key:
        h = hashlib.sha1()
        h.update(str(target).encode('utf-8'))
        if query:
            h.update(query.encode('utf-8'))
        if csv_path:
            h.update(csv_path.encode('utf-8'))
        cache_key = h.hexdigest()[:16]

    df = None
    if data is not None:
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    elif csv_path:
        p = Path(csv_path)
        if p.is_file():
            df = pd.read_csv(p, encoding=encoding)
    elif db_url and query:
        try:
            if db_url.startswith('sqlite:///'):
                dbpath = db_url.replace('sqlite:///', '')
                conn = sqlite3.connect(dbpath)
                df = pd.read_sql_query(query, conn)
                conn.close()
            else:
                from sqlalchemy import create_engine
                engine = create_engine(db_url)
                df = pd.read_sql_query(query, engine)
        except Exception as e:
            return {"status": "error", "error": str(e)}

    if df is None:
        return {"status": "skipped", "reason": "no data source found"}

    if to_disk:
        outdir = Path(target).parent
        outdir = outdir / 'plot_data'
        outdir.mkdir(parents=True, exist_ok=True)
        p = outdir / f"{cache_key}.parquet"
        try:
            df.to_parquet(p)
            return {
                "status": "cached",
                "cache_key": cache_key,
                "rows": len(df),
                "path": str(p)
            }
        except Exception:
            p = outdir / f"{cache_key}.csv"
            df.to_csv(p, index=False, encoding=encoding)
            return {
                "status": "cached",
                "cache_key": cache_key,
                "rows": len(df),
                "path": str(p)
            }

    return {
        "status": "cached",
        "cache_key": cache_key,
        "rows": len(df),
        "df": df
    }


def plot_from_spec_impl(target: Path,
                        *,
                        data=None,
                        spec: dict = None,
                        out_dir: str = None,
                        fmt: str = 'png',
                        dpi: int = 150,
                        base_style: dict = None) -> dict:
    """Create plot(s) from a specification using the provided data.

    This function accepts a `spec` describing subplot layout, series and
    saving options, and draws figures using matplotlib (Agg backend).
    It returns a standardized dict on success or error:

    - Success:
        {"status": "success", "figure_path": "<saved path>", "warnings": [...]} 
    - Skipped (no spec):
        {"status": "skipped", "reason": "no spec"}
    - Error:
        {"status": "error", "error": "..."}

    Spec format (minimal example)::

        spec = {
            "title": "My plot",
            "layout": {"rows": 1, "cols": 1},
            "subplots": [
                {
                    "title": "Series A",
                    "x_label": "x",
                    "y_label": "value",
                    "series": [
                        {"x": "x", "y": "y", "label": "A", "style": "-"}
                    ],
                    "legend": True
                }
            ],
            "save": {"filename": "myplot.png", "dpi": 150}
        }

    Grid placement and spanning
    - Each subplot entry may include `row` and `col` (integers) to specify
      its starting grid cell, plus optional `rowspan` and `colspan` (integers)
      to span multiple grid cells. Indices are 0-based.
    - If `row`/`col` are omitted the function will auto-place the subplot
      into the next available cell scanning left-to-right, top-to-bottom.
    - If a requested block overlaps an already-placed subplot the
      subplot will be skipped and a warning will be included in the
      returned `warnings` list.

    Placement example (2x2 grid)::

        spec = {
            "layout": {"rows": 2, "cols": 2},
            "subplots": [
                {"row": 0, "col": 0, "rowspan": 2, "colspan": 1, "series": [...]},
                {"row": 0, "col": 1, "series": [...]},
                {"row": 1, "col": 1, "series": [...]}
            ]
        }

    Notes
    - `row`/`col` are 0-based by design (the test-suite and examples use 0-based
      indices). If you prefer 1-based indices I can add an option to accept
      that and convert internally.
    - The function uses a GridSpec internally and returns warnings when
      placement cannot be honored rather than raising an exception, because
      specs are often generated by users and we prefer robust behavior in
      batch runs.

    Parameters
    - target: a reference Path used to derive default output filename.
    - data: pandas DataFrame or data convertible to DataFrame. If a series
      includes an explicit `data` entry it will be used for that series.
    - spec: dict describing layout, series and save options (see example).
    - out_dir: directory to write output files; defaults to `Path(target).parent`.
    - fmt / dpi / base_style: appearance options.

    Examples
    - Plot from an existing DataFrame::

        >>> df = pd.DataFrame({"x": [1,2,3], "y": [2,3,5]})
        >>> spec = {"layout": {"rows":1, "cols":1}, "subplots": [{"series": [{"x":"x","y":"y"}]}]}
        >>> plot_from_spec_impl(Path("/tmp/foo.txt"), data=df, spec=spec, out_dir=str(tmpdir))
        {"status": "success", "figure_path": "/tmp/myplot.png", "warnings": []}

    The function closes the created figure before returning so it is safe
    to call repeatedly in long-running processes.
    """
    data = _ensure_df(data)
    base_style = base_style or {}

    if spec is None:
        return {"status": "skipped", "reason": "no spec"}

    out_root = Path(out_dir) if out_dir else Path(target).parent
    out_root.mkdir(parents=True, exist_ok=True)

    warnings = []
    try:
        layout = spec.get('layout', {})
        rows = int(layout.get('rows', 1))
        cols = int(layout.get('cols', 1))
        figsize = tuple(spec.get('figsize', [8 * cols, 4 * rows]))

        fig = plt.figure(figsize=figsize)
        gs = fig.add_gridspec(rows, cols)

        # occupancy grid to support rowspan/colspan placement
        occupied = [[False for _ in range(cols)] for _ in range(rows)]

        def _find_next_empty():
            for ri in range(rows):
                for ci in range(cols):
                    if not occupied[ri][ci]:
                        return ri, ci
            return None, None

        subplots = spec.get('subplots', [])
        base_series_style = base_style.get('series', {}) if isinstance(
            base_style, dict) else {}

        for si, subplot in enumerate(subplots):
            # determine target position and span
            r = subplot.get('row', None)
            c = subplot.get('col', None)
            rowspan = int(subplot.get('rowspan', 1))
            colspan = int(subplot.get('colspan', 1))

            if r is None or c is None:
                r, c = _find_next_empty()
                if r is None:
                    warnings.append(f"subplot {si}: no space left in grid")
                    break

            # validate indices
            try:
                r = int(r)
                c = int(c)
            except Exception:
                warnings.append(
                    f"subplot {si}: invalid row/col '{subplot.get('row')}/{subplot.get('col')}'"
                )
                continue

            if r < 0 or c < 0 or r >= rows or c >= cols:
                warnings.append(
                    f"subplot {si}: position ({r},{c}) out of grid bounds")
                continue

            # clamp spans to grid
            if rowspan < 1:
                rowspan = 1
            if colspan < 1:
                colspan = 1

            end_r = min(rows, r + rowspan)
            end_c = min(cols, c + colspan)

            # check occupancy for requested block
            conflict = False
            for ri in range(r, end_r):
                for ci in range(c, end_c):
                    if occupied[ri][ci]:
                        conflict = True
                        break
                if conflict:
                    break
            if conflict:
                warnings.append(
                    f"subplot {si}: requested block ({r}:{end_r},{c}:{end_c}) overlaps existing subplot"
                )
                continue

            # mark occupied
            for ri in range(r, end_r):
                for ci in range(c, end_c):
                    occupied[ri][ci] = True

            ax = fig.add_subplot(gs[r:end_r, c:end_c])
            for series in subplot.get('series', []):
                try:
                    df = data
                    if series.get('data') is not None:
                        df = pd.DataFrame(series.get('data'))

                    merged_series = dict(base_series_style)
                    if isinstance(series, dict):
                        merged_series.update(series)

                    xcol = merged_series.get('x')
                    ycol = merged_series.get('y')
                    style = merged_series.get('style', '-')
                    label = merged_series.get(
                        'label',
                        merged_series.get('name') or ycol)

                    plot_kwargs = {}
                    for k in ('color', 'linewidth', 'linestyle', 'marker',
                              'alpha'):
                        if k in merged_series:
                            plot_kwargs[k] = merged_series[k]

                    if df is None:
                        warnings.append(f"series {label}: no data available")
                        continue

                    if isinstance(ycol, (list, tuple)):
                        for y in ycol:
                            if isinstance(style, str):
                                ax.plot(df[xcol],
                                        df[y],
                                        style,
                                        label=f"{label}:{y}",
                                        **plot_kwargs)
                            else:
                                ax.plot(df[xcol],
                                        df[y],
                                        label=f"{label}:{y}",
                                        **plot_kwargs)
                    else:
                        if isinstance(style, str):
                            ax.plot(df[xcol],
                                    df[ycol],
                                    style,
                                    label=label,
                                    **plot_kwargs)
                        else:
                            ax.plot(df[xcol],
                                    df[ycol],
                                    label=label,
                                    **plot_kwargs)
                except Exception as e:
                    warnings.append(
                        f"subplot {si} series {series.get('name', '')} failed: {e}"
                    )

            if subplot.get('legend', True):
                try:
                    ax.legend()
                except Exception:
                    pass
            ax.set_title(subplot.get('title', ''))
            if subplot.get('x_label'):
                ax.set_xlabel(subplot.get('x_label'))
            if subplot.get('y_label'):
                ax.set_ylabel(subplot.get('y_label'))

        fig.suptitle(spec.get('title', ''))
        save_info = spec.get('save', {})
        filename = save_info.get('filename', f"plot_{Path(target).stem}.{fmt}")
        outpath = out_root / filename
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.savefig(outpath, dpi=save_info.get('dpi', dpi), format=fmt)
        plt.close(fig)

        return {
            "status": "success",
            "figure_path": str(outpath),
            "warnings": warnings
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
