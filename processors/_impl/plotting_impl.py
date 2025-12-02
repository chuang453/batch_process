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
    """Prepare/collect data for plotting without any ProcessingContext.

    Returns a dict: on success `{'status':'cached','cache_key':..., 'rows': N, 'df': DataFrame}`
    or when saved to disk returns `path` instead of `df`.
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
    """Create plot files from a spec using provided DataFrame (no context).

    Returns {'status':'success','figure_path': str, 'warnings': [...]} or error dict.
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

        fig, axes = plt.subplots(rows, cols, figsize=figsize)
        # normalize axes to flat iterable
        if isinstance(axes, (list, tuple)):
            axes_flat = list(axes)
        else:
            try:
                axes_flat = list(axes.flat)
            except Exception:
                axes_flat = [axes]

        subplots = spec.get('subplots', [])
        base_series_style = base_style.get('series', {}) if isinstance(
            base_style, dict) else {}

        for si, subplot in enumerate(subplots):
            if si >= len(axes_flat):
                break
            ax = axes_flat[si]
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
