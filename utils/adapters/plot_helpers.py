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


import math
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from typing import Callable, Any, Dict, List, Optional, Tuple, Union
from matplotlib import font_manager


def get_chinese_font():
    """返回一个可用的中文字体名称"""
    chinese_fonts = [
        'SimHei', 'Microsoft YaHei', 'PingFang SC', 'Hiragino Sans GB',
        'STHeiti', 'WenQuanYi Micro Hei'
    ]
    available_fonts = set(f.name for f in font_manager.fontManager.ttflist)
    for font in chinese_fonts:
        if font in available_fonts:
            return font
    return None  # 无中文字体


font = get_chinese_font()
if font:
    plt.rcParams['font.sans-serif'] = [font, 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
else:
    print("警告：未找到中文字体，中文可能显示为方框")


def generic_plot(extract_f: Callable[[Any], List[float]],
                 plot_spec: Dict[str, Any],
                 plot_style: Optional[Dict[str, Any]] = None):
    """
    通用绘图函数。
    
    Parameters:
    ----------
    extract_f : callable
        闭包函数，接受 param，返回一维数据列表（x 或 y）。
        示例: extract_f(('sheet0', 'col2')) -> [1.2, 3.4, ...]
    
    plot_spec : dict
        {
            "subplots": [
                {
                    "pos": (nrows, ncols, index) OR (row, col) for subplot2grid,
                    "title": str (optional),
                    "xlabel": str (optional),
                    "ylabel": str (optional),
                    "lines": [
                        {
                            "x": x_param,
                            "y": [y_param, label_str, style_dict]
                        },
                        ...
                    ]
                },
                ...
            ],
            "save_path": str (optional)
        }
    
    plot_style : dict (optional)
        {
            "figsize": (w, h),
            "grid": bool,
            "tight_layout": bool,
            "dpi": int
        }

  "pos"这个参数有2种格式：标准格式  (nrows, ncols, index) → 自动均匀划分                                      (2, 2, 1)
                                        扩展格式  (nrows, ncols, (r_start, r_end, c_start, c_end)) → 手动指定区域   (2, 2, (0, 2, 0, 1))
    """

    # 默认样式
    default_style = {
        "figsize": (10, 8),
        "grid": False,
        "dpi": 100,
        "tight_layout": True,
        # legend handling defaults
        "legend_threshold": 8,  # if > threshold, place legend outside
        "legend_ncol_max": 4,
        "legend_fontsize": 8
    }
    if plot_style:
        default_style.update(plot_style)

    fig = plt.figure(figsize=default_style["figsize"])

    subplots = plot_spec["subplots"]

    # === 自动推断全局 GridSpec 大小 ===
    max_rows, max_cols = 1, 1
    for sp in subplots:
        pos = sp["pos"]
        if len(pos) == 3:
            nrows, ncols = pos[0], pos[1]
        else:
            raise ValueError(
                "`pos` must be (nrows, ncols, index) or (nrows, ncols, (r0,r1,c0,c1))"
            )
        max_rows = max(max_rows, nrows)
        max_cols = max(max_cols, ncols)

    # 创建统一的 GridSpec（基于最大行列数）
    gs = gridspec.GridSpec(max_rows, max_cols, figure=fig)

    # === 绘制每个子图 ===
    for idx, sp in enumerate(subplots):  # 修复了这里的一个错误
        pos = sp["pos"]
        nrows, ncols = pos[0], pos[1]

        if isinstance(pos[2], int):
            # 标准格式: (nrows, ncols, index)
            index = pos[2] - 1  # MATLAB-style 1-based → 0-based
            if index < 0:
                raise ValueError("Subplot index must be >= 1")
            r = index // ncols
            c = index % ncols
            ax = fig.add_subplot(gs[r, c])
        elif isinstance(pos[2], (tuple, list)) and len(pos[2]) == 4:
            # 扩展格式: (nrows, ncols, (r0, r1, c0, c1))
            r0, r1, c0, c1 = pos[2]
            ax = fig.add_subplot(gs[r0:r1, c0:c1])
        else:
            raise ValueError("`pos[2]` must be an int (MATLAB-style index) "
                             "or a 4-tuple (r0, r1, c0, c1) for spanning.")

        # --- 设置标题、标签 ---
        if "title" in sp:
            ax.set_title(sp["title"])
        if "xlabel" in sp:
            ax.set_xlabel(sp["xlabel"])
        if "ylabel" in sp:
            ax.set_ylabel(sp["ylabel"])

        # --- 绘制线条 ---
        for line in sp.get("lines", []):  # 修正了这里的获取方式
            x_param = line["x"]
            y_info = line["y"]

            if len(y_info) == 1:
                y_param, label, style = y_info[0], "", {}
            elif len(y_info) == 2:
                y_param, label = y_info
                style = {}
            elif len(y_info) == 3:
                y_param, label, style = y_info
            else:
                raise ValueError(
                    "y must be [y_param] or [y_param, label] or [y_param, label, style]"
                )

            try:
                x_data = extract_f(x_param)
                y_data = extract_f(y_param)
            except Exception as e:
                raise RuntimeError(
                    f"Data extraction failed for {x_param}/{y_param}: {e}")

            if len(x_data) != len(y_data):
                raise ValueError(
                    f"x/y length mismatch: {len(x_data)} vs {len(y_data)}")

            ax.plot(x_data, y_data, label=label, **style)

        # handle legend intelligently: if many labels, place legend outside
        handles, labels = ax.get_legend_handles_labels()
        nlabels = len(labels)
        if nlabels:
            legend_threshold = default_style.get("legend_threshold", 8)
            if nlabels > legend_threshold:
                # choose number of columns to reduce legend height
                ncol = min(default_style.get("legend_ncol_max", 4),
                           max(1, math.ceil(nlabels / legend_threshold)))
                ax.legend(handles,
                          labels,
                          ncol=ncol,
                          bbox_to_anchor=(1.02, 1),
                          loc='upper left',
                          fontsize=default_style.get("legend_fontsize", 8),
                          markerscale=0.8)
                # make room on the right for the legend
                try:
                    fig.subplots_adjust(right=0.78)
                except Exception:
                    pass
            else:
                ax.legend()

        if default_style["grid"]:
            ax.grid(True)

    if default_style["tight_layout"]:
        plt.tight_layout()

    if "save_path" in plot_spec:
        #        # Save to file when requested (non-blocking)
        plt.savefig(plot_spec["save_path"],
                    dpi=default_style["dpi"],
                    bbox_inches='tight')

    # IMPORTANT: do NOT call `plt.show()` here — that will open an
    # interactive window and block the Qt event loop when called from
    # a worker thread. Instead, close the figure to free resources.
    try:
        plt.close(fig)
    except Exception:
        pass
