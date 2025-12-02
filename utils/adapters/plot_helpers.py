"""Plot helpers adapter: small convenience wrappers around the
pure plotting implementation.

This module exposes `plot_from_spec_adapter(..)` which forwards calls to
`processors._impl.plotting_impl.plot_from_spec_impl` and accepts an
optional `extract_f` callable identical to the processor-level API.

Extractor protocol
- `extract_f(series, data, target)` should return either a DataFrame
    (or data convertible to pd.DataFrame) or `(df, meta)` where `meta` is
    an arbitrary dict. The plotting implementation collects `meta` for
    all series and returns it under `res['extract_meta']`.

Storage
- The `plot_from_spec` processor converts the returned `extract_meta`
    list into a mapping keyed by series label and stores it at
    `context.data['plot_extract_meta'][str(target)]` for downstream
    processors to consume.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable


def save_plot_png_values(values: List[float], out_path: Path,
                         cfg: Dict[str, Any]) -> Path:
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
        fig = Figure(figsize=(cfg.get("fig_width",
                                      4), cfg.get("fig_height", 3)),
                     dpi=cfg.get("dpi", 100))
        ax = fig.add_subplot(111)
        ax.plot(values, marker="o")
        ax.grid(True)
        ax.set_title(cfg.get("title", "数据曲线"))
        fig.tight_layout()
        canvas = FigureCanvas(fig)
        canvas.draw()
        fig.savefig(out_path)
        return out_path
    except Exception:
        from PIL import Image, ImageDraw, ImageFont
        W, H = int(cfg.get("px_width", 600)), int(cfg.get("px_height", 400))
        im = Image.new("RGB", (W, H), (255, 255, 255))
        draw = ImageDraw.Draw(im)
        margin = 40
        plot_w = W - 2 * margin
        plot_h = H - 2 * margin
        vmin, vmax = (min(values), max(values)) if values else (0, 1)
        rng = vmax - vmin if vmax != vmin else 1.0
        pts = []
        for i, v in enumerate(values):
            x = margin + (i / max(1, len(values) - 1)) * plot_w
            y = margin + (1 - (v - vmin) / rng) * plot_h
            pts.append((x, y))
        draw.rectangle([margin, margin, margin + plot_w, margin + plot_h],
                       outline=(0, 0, 0))
        if len(pts) > 1:
            draw.line(pts, fill=(30, 120, 200), width=2)
        for x, y in pts:
            draw.ellipse([x - 3, y - 3, x + 3, y + 3], fill=(200, 50, 50))
        title = cfg.get("title", "数据曲线")
        try:
            font = ImageFont.load_default()
            draw.text((margin, 5), title, fill=(0, 0, 0), font=font)
        except Exception:
            draw.text((margin, 5), title, fill=(0, 0, 0))
        im.save(out_path)
        return out_path


def prepare_plot_data_adapter(target: Path,
                              *,
                              cache_key: str = None,
                              db_url: str = None,
                              query: str = None,
                              csv_path: str = None,
                              data=None,
                              to_disk: bool = False,
                              force: bool = False,
                              encoding: str = 'utf-8') -> Dict[str, Any]:
    try:
        from processors._impl.plotting_impl import prepare_plot_data_impl
        return prepare_plot_data_impl(target,
                                      cache_key=cache_key,
                                      db_url=db_url,
                                      query=query,
                                      csv_path=csv_path,
                                      data=data,
                                      to_disk=to_disk,
                                      force=force,
                                      encoding=encoding)
    except Exception as e:
        return {"status": "error", "error": str(e)}


def plot_from_spec_adapter(
        target: Path,
        *,
        data=None,
        spec: Dict[str, Any] = None,
        out_dir: str = None,
        fmt: str = 'png',
        dpi: int = 150,
        base_style: Dict[str, Any] = None,
        extract_f: Optional[Callable] = None) -> Dict[str, Any]:
    try:
        from processors._impl.plotting_impl import plot_from_spec_impl
        return plot_from_spec_impl(target,
                                   data=data,
                                   spec=spec,
                                   out_dir=out_dir,
                                   fmt=fmt,
                                   dpi=dpi,
                                   base_style=base_style,
                                   extract_f=extract_f)
    except Exception as e:
        return {"status": "error", "error": str(e)}
