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
import colorsys
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from typing import Callable, Any, Dict, List, Optional, Tuple, Union
from matplotlib import font_manager


## 获取n种颜色的列表，默认使用tab20色图。颜色可有很多种
## 获取n种颜色的列表，默认使用tab20色图。颜色可有很多种
def get_n_colors(n: int,
                 cmap_name: str = 'tab20') -> List[Tuple[float, float, float]]:
    """Return `n` colors that are visually well-separated.

    For small n use qualitative matplotlib colormaps (`tab10`/`tab20`).
    For larger n generate colors by spacing hues using the golden-ratio
    conjugate and slightly varying saturation/value to increase contrast.
    """
    if n <= 0:
        return []
    try:
        if n <= 10:
            base = list(plt.get_cmap('tab10').colors)
            return [base[i % len(base)] for i in range(n)]
        if n <= 20:
            base = list(plt.get_cmap('tab20').colors)
            return [base[i % len(base)] for i in range(n)]
    except Exception:
        pass

    colors = []
    golden = 0.618033988749895
    for i in range(n):
        h = (i * golden) % 1.0
        s = 0.65 + 0.20 * ((i % 3) / 2)
        v = 0.9 - 0.15 * ((i % 4) / 3)
        r, g, b = colorsys.hsv_to_rgb(h, s, v)
        colors.append((r, g, b))
#   return colors
    return [{'color': colori} for colori in colors]


##返回n中线型样式
def get_n_linestyles(n: int):
    """
    返回 n 个尽可能区分的线型（linestyle）。
    包含实线、标准虚线和精心设计的自定义 dash patterns。
    """
    if n <= 0:
        return []

    # 1. 基础线型（优先使用）
    base_styles = ['solid', 'dashed', 'dotted', 'dashdot']

    # 2. 自定义 dash patterns（元组格式：(offset, (on, off, on, off, ...))）
    # 每个 pattern 设计为视觉上与其它明显不同
    custom_patterns = [
        (0, (5, 5)),  # 长虚线
        (0, (3, 1, 1, 1)),  # 点-点-划
        (0, (1, 1)),  # 密集点线（比 dotted 更紧凑）
        (0, (5, 1)),  # 长划-短空
        (0, (3, 5, 1, 5)),  # 划-长空-点-长空
        (0, (1, 3)),  # 短划-长空（稀疏点）
        (0, (4, 2, 1, 2)),  # 划-空-点-空
        (0, (2, 2, 2, 2)),  # 等长划空交替（类似 --.--）
    ]

    all_styles = base_styles + custom_patterns

    styles = []
    if n <= len(all_styles):
        styles = all_styles[:n]
    #  return all_styles[:n]
    else:
        # 超出预设数量时循环复用（避免报错）
        print(f"警告：请求 {n} 种线型，但仅预定义 {len(all_styles)} 种，将循环复用。")
        for i in range(n):
            styles.append(all_styles[i % len(all_styles)])

    return [{'linestyle': stylei} for stylei in styles]


## 返回n中marker类型
def get_n_markers(n: int, is_hollow='none'):
    """
    返回 n 个空心标记配置。
    每个元素为 (marker, style_dict)，其中 style_dict 包含 mfc/mec 等。
    """
    base_markers = ['o', 's', '^', 'D', 'v', 'P', '*', 'X', 'h', '+', 'x']

    # 定义一组区分度高的边框颜色（可选，也可统一用黑色）
    edge_colors = [
        'black', 'red', 'blue', 'green', 'purple', 'orange', 'brown', 'pink',
        'gray', 'olive', 'cyan'
    ]

    configs = []
    for i in range(n):
        marker = base_markers[i % len(base_markers)]
        edge_color = edge_colors[i % len(edge_colors)]

        # '+' 和 'x' 默认无填充，但为了统一也显式设置
        style = {
            'marker': marker,
            'markerfacecolor': is_hollow,  # ← 值为'none'时，代表空心核心
            'markeredgecolor': edge_color,
            'markeredgewidth': 1.2,  # 边框粗细（建议 ≥1）
            'markersize': 6
        }
        configs.append(style)

    return configs


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
        "legend_threshold": 8,  # if > threshold, move legend out of axes
        "legend_ncol_max": 4,
        "legend_fontsize": 8,
        # default placement strategy when many items: bottom
        "legend_position": "bottom"
    }
    if plot_style:
        default_style.update(plot_style)

    debug = bool(plot_style and plot_style.get('debug'))
    if debug:
        print('GENERIC_PLOT: start', flush=True)

    # Use a non-interactive Figure + Agg canvas to avoid starting a GUI
    # backend when called from a worker thread.
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
        fig = Figure(figsize=default_style["figsize"],
                     dpi=default_style.get("dpi", 100))
        canvas = FigureCanvas(fig)
        if debug:
            print('GENERIC_PLOT: created Figure+Canvas', flush=True)
    except Exception:
        # fallback to pyplot if imports fail
        fig = plt.figure(figsize=default_style["figsize"])
        canvas = None

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
        lines = sp.get("lines", [])  # 修正了这里的获取方式
        nlines = len(lines)
        colors_for_lines = get_n_colors(nlines)
        line_local_idx = 0
        for line in lines:
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

            # assign a generated color if caller didn't specify one
            if not isinstance(style, dict):
                style = {} if style is None else dict(style)
            if 'color' not in style and line_local_idx < len(colors_for_lines):
                style = dict(style)
                style['color'] = colors_for_lines[line_local_idx]

            ax.plot(x_data, y_data, label=label, **style)
            line_local_idx += 1

        # handle legend intelligently: if many labels, place legend outside
        handles, labels = ax.get_legend_handles_labels()
        nlabels = len(labels)
        if nlabels:
            legend_threshold = default_style.get("legend_threshold", 8)
            legend_pos = default_style.get("legend_position", "auto")
            if nlabels > legend_threshold:
                # choose number of columns to reduce legend height (or make a row)
                ncol = min(default_style.get("legend_ncol_max", 8),
                           max(1, math.ceil(nlabels / legend_threshold)))
                fontsize = default_style.get("legend_fontsize", 8)
                markerscale = default_style.get("legend_markerscale", 0.8)

                # Placement strategies
                if legend_pos in ("auto", "right"):
                    ax.legend(handles,
                              labels,
                              ncol=ncol,
                              bbox_to_anchor=(1.02, 1),
                              loc='upper left',
                              fontsize=fontsize,
                              markerscale=markerscale)
                    # make room on the right for the legend
                    try:
                        fig.subplots_adjust(right=0.78)
                    except Exception:
                        pass
                elif legend_pos == "top":
                    # place legend above the plot, centered
                    ax.legend(handles,
                              labels,
                              ncol=min(
                                  nlabels,
                                  default_style.get("legend_ncol_max",
                                                    nlabels)),
                              bbox_to_anchor=(0.5, 1.02),
                              loc='lower center',
                              fontsize=fontsize,
                              markerscale=markerscale)
                    try:
                        fig.subplots_adjust(top=0.82)
                    except Exception:
                        pass
                elif legend_pos == "bottom":
                    # place legend below the plot, centered
                    ax.legend(handles,
                              labels,
                              ncol=min(
                                  nlabels,
                                  default_style.get("legend_ncol_max",
                                                    nlabels)),
                              bbox_to_anchor=(0.5, -0.12),
                              loc='upper center',
                              fontsize=fontsize,
                              markerscale=markerscale)
                    try:
                        fig.subplots_adjust(bottom=0.18)
                    except Exception:
                        pass
                else:
                    # fallback to default (right)
                    ax.legend(handles,
                              labels,
                              ncol=ncol,
                              bbox_to_anchor=(1.02, 1),
                              loc='upper left',
                              fontsize=fontsize,
                              markerscale=markerscale)
                    try:
                        fig.subplots_adjust(right=0.78)
                    except Exception:
                        pass
            else:
                ax.legend()

        if default_style["grid"]:
            ax.grid(True)

    if default_style["tight_layout"]:
        try:
            fig.tight_layout()
        except Exception:
            try:
                plt.tight_layout()
            except Exception:
                pass

    if "save_path" in plot_spec:
        # Save to file when requested (non-blocking)
        try:
            # draw on Agg canvas if available
            if 'canvas' in locals() and getattr(locals()['canvas'], 'draw',
                                                None):
                try:
                    locals()['canvas'].draw()
                except Exception:
                    pass
            fig.savefig(plot_spec["save_path"],
                        dpi=default_style.get("dpi", 100),
                        bbox_inches='tight')
        except Exception:
            try:
                plt.savefig(plot_spec.get("save_path"),
                            dpi=default_style.get("dpi", 100),
                            bbox_inches='tight')
            except Exception:
                pass

    # IMPORTANT: do NOT call `plt.show()` here — that will open an
    # interactive window and block the Qt event loop when called from
    # a worker thread. Instead, free figure resources.
    try:
        # prefer to clear the figure; plt.close works with Figure objects
        plt.close(fig)
    except Exception:
        try:
            fig.clf()
        except Exception:
            pass

    # For testing or interactive inspection, optionally return the Figure
    if plot_style and isinstance(plot_style, dict) and plot_style.get(
            'return_figure', False):
        try:
            return fig
        except Exception:
            return None
