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
        styles: List[Dict[str, Any]] = []
        for idx, r in enumerate(rows):
            s: Dict[str, Any] = {}

            # apply explicit mappings when present
            if color_map is not None:
                c = color_map.get(r.get(color_key))
                if c is not None:
                    s['color'] = tuple(c) if isinstance(c, (list, np.ndarray)) else c

            if ls_map is not None:
                ls = ls_map.get(r.get(ls_key))
                if ls is not None:
                    s['linestyle'] = ls

            if marker_map is not None:
                mk = marker_map.get(r.get(marker_key))
                if mk is not None:
                    if isinstance(mk, dict):
                        s.update(mk)
                    else:
                        s['marker'] = mk

            styles.append(s)

        # Ensure every style has defaults for any missing role so plotting always
        # has a color, linestyle and marker value (unless pools are empty).
        for idx, s in enumerate(styles):
            if 'color' not in s and default_colors:
                s['color'] = tuple(default_colors[idx % len(default_colors)])
            if 'linestyle' not in s and default_lss:
                s['linestyle'] = default_lss[idx % len(default_lss)]
            if 'marker' not in s and default_markers:
                mk = default_markers[idx % len(default_markers)]
                if isinstance(mk, dict):
                    # merge marker dict if provided by pool
                    for k, v in mk.items():
                        if k not in s:
                            s[k] = v
                else:
                    s['marker'] = mk

        return styles
            'markeredgewidth': 1.2,
            'markersize': 6
        })
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


def generate_plot_style(
        group_dict: Dict[str, List[Any]] | List[Dict[str, Any]],
        options: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Generate per-curve plotting styles from grouping columns.

    This function accepts either:
      - a mapping of lists (``dict[str, List]``) where each list is index-aligned
        across keys (old behaviour), or
      - a list of row-dictionaries (``List[Dict[str, Any]]``), each providing
        the group values for a single curve (new behaviour).

    Missing values are treated as ``None``.

    Options are the same as before and support keys mapping style roles to
    grouping keys (e.g. ``{'color_key': 'mygroup'}``).
    """
    opts = options or {}

    # normalized option names (accept both 'color' and 'color_key')
    color_key = opts.get('color_key') or opts.get('color')
    ls_key = opts.get('linestyle_key') or opts.get('linestyle')
    marker_key = opts.get('marker_key') or opts.get('marker')

    # detect explicit mapping presence (user provided a role mapping key)
    explicit_color = color_key is not None or ('color_map' in opts)
    explicit_ls = ls_key is not None or ('linestyle_map' in opts)
    explicit_marker = marker_key is not None or ('marker_map' in opts)

    # normalize rows: accept list-of-dicts or dict-of-lists
    rows: List[Dict[str, Any]] = []
    if isinstance(group_dict, list):
        for row in group_dict:
            rows.append(dict(row) if row is not None else {})
    else:
        keys = list(group_dict.keys())
        if not keys:
            return []
        n = max((len(v) for v in group_dict.values()), default=0)
        for i in range(n):
            r: Dict[str, Any] = {}
            for k in keys:
                vals = group_dict.get(k, [])
                r[k] = vals[i] if i < len(vals) else None
            rows.append(r)

    if not rows:
        return []

    # helper to build mapping from unique group values -> style values
    def build_map_from_key(key_name, gen_fn):
        if key_name is None:
            return None
        seen: List[Any] = []
        for r in rows:
            v = r.get(key_name)
            if v not in seen:
                seen.append(v)
        vals = gen_fn(len(seen))
        return {k: v for k, v in zip(seen, vals)}

    # user-provided explicit maps override generated maps
    color_map = opts.get('color_map') or build_map_from_key(
        color_key, lambda m: get_n_colors(m))
    ls_map = opts.get('linestyle_map') or build_map_from_key(
        ls_key, lambda m: get_n_linestyles(m))
    marker_map = opts.get('marker_map') or build_map_from_key(
        marker_key, lambda m: get_n_markers(m, is_hollow='none'))

    # default pools used for disambiguation (do not apply eagerly when any explicit role exists)
    default_colors = get_n_colors(len(rows))
    default_lss = get_n_linestyles(len(rows))
    default_markers = get_n_markers(len(rows), is_hollow='none')

    styles: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        s: Dict[str, Any] = {}

        # apply explicit mappings only
        if color_map is not None:
            c = color_map.get(r.get(color_key))
            if c is not None:
                s['color'] = tuple(c) if isinstance(c,
                                                    (list, np.ndarray)) else c

        if ls_map is not None:
            ls = ls_map.get(r.get(ls_key))
            if ls is not None:
                s['linestyle'] = ls

        if marker_map is not None:
            mk = marker_map.get(r.get(marker_key))
            if mk is not None:
                # mk may be a dict of marker properties
                if isinstance(mk, dict):
                    s.update(mk)
                else:
                    s['marker'] = mk

        styles.append(s)

    # If no explicit roles were requested at all, fall back to legacy behavior:
    # assign each row a color and linestyle (and marker if desired) deterministically.
    if not (explicit_color or explicit_ls or explicit_marker):
        for idx, s in enumerate(styles):
            if 'color' not in s and default_colors:
                s['color'] = tuple(default_colors[idx % len(default_colors)])
            if 'linestyle' not in s and default_lss:
                s['linestyle'] = default_lss[idx % len(default_lss)]
        return styles

    # Otherwise only add styles that are necessary to disambiguate identical signatures.
    # Priority for adding roles: marker -> linestyle -> color
    from collections import defaultdict

    def signature_for(sdict, roles):
        return tuple(sdict.get(r) for r in roles)

    # roles considered so far (start with explicit roles only)
    current_roles = []
    if explicit_color and color_map is not None:
        current_roles.append('color')
    if explicit_ls and ls_map is not None:
        current_roles.append('linestyle')
    if explicit_marker and marker_map is not None:
        current_roles.append('marker')

    # If no explicit mapped roles were produced (e.g., user specified keys but maps empty),
    # start with whatever fields are present in styles
    if not current_roles:
        # include roles that appear in styles
        for role in ('color', 'linestyle', 'marker'):
            if any(role in s for s in styles):
                current_roles.append(role)

    # function that tries to add a role (marker/linestyle/color) to resolve collisions
    def try_add_role(role_name):
        if role_name == 'marker':
            pool = default_markers
            assign_fn = lambda i: pool[i % len(pool)] if pool else None
            key = 'marker'
        elif role_name == 'linestyle':
            pool = default_lss
            assign_fn = lambda i: pool[i % len(pool)] if pool else None
            key = 'linestyle'
        else:
            pool = default_colors
            assign_fn = lambda i: pool[i % len(pool)] if pool else None
            key = 'color'

        # build signature map under current_roles
        sig_map = defaultdict(list)
        for i, s in enumerate(styles):
            sig = signature_for(s, current_roles)
            sig_map[sig].append(i)

        changed = False
        for sig, idxs in list(sig_map.items()):
            if len(idxs) <= 1:
                continue
            # assign the role to each colliding item if not already present
            for j, ii in enumerate(idxs):
                if key in styles[ii]:
                    continue
                val = assign_fn(j)
                if val is None:
                    continue
                if key == 'marker':
                    # marker may need to be a dict (marker + face/edge)
                    if isinstance(val, dict):
                        styles[ii].update(val)
                    else:
                        styles[ii]['marker'] = val
                elif key == 'linestyle':
                    styles[ii]['linestyle'] = val
                else:
                    styles[ii]['color'] = tuple(val) if isinstance(
                        val, (list, np.ndarray)) else val
                changed = True
        if changed:
            # role was applied; include it in current_roles for subsequent rounds
            current_roles.append(key)
        return changed

    # Attempt disambiguation in priority order
    for role in ('marker', 'linestyle', 'color'):
        # don't add a role that the user explicitly requested to be absent
        if role == 'marker' and explicit_marker:
            continue
        if role == 'linestyle' and explicit_ls:
            continue
        if role == 'color' and explicit_color:
            continue
        changed = try_add_role(role)
        # if no collisions remain, stop early
        sigs = set(signature_for(s, current_roles) for s in styles)
        if len(sigs) == len(styles):
            break

    return styles


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

    # Prefer object-oriented Figure + Agg canvas (thread-safe in-process)
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    fig = Figure(figsize=default_style["figsize"],
                 dpi=default_style.get("dpi", 100))
    canvas = FigureCanvas(fig)
    if debug:
        print('GENERIC_PLOT: created Figure+Canvas', flush=True)

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
                x_raw = extract_f(x_param)
                y_raw = extract_f(y_param)
            except Exception as e:
                raise RuntimeError(
                    f"Data extraction failed for {x_param}/{y_param}: {e}")

            def _to_1d(seq):
                # handle (data, meta) tuples
                if isinstance(seq, tuple) and len(seq) == 2:
                    seq = seq[0]
                # pandas objects
                try:
                    import pandas as _pd
                    if isinstance(seq, (_pd.Series, _pd.DataFrame)):
                        arr = np.asarray(seq).ravel()
                        return arr
                except Exception:
                    pass
                arr = np.asarray(seq)
                if arr.ndim == 0:
                    # scalar
                    return np.atleast_1d(arr)
                return arr.ravel()

            x_data = _to_1d(x_raw)
            y_data = _to_1d(y_raw)

            if x_data.shape[0] != y_data.shape[0]:
                raise ValueError(
                    f"x/y length mismatch: {x_data.shape[0]} vs {y_data.shape[0]}"
                )

            # assign a generated color if caller didn't specify one
            if not isinstance(style, dict):
                style = {} if style is None else dict(style)

            # sanitize style: only allow known kwargs to pass to ax.plot
            allowed_keys = {
                'color', 'linewidth', 'linestyle', 'marker', 'markersize',
                'markerfacecolor', 'markeredgecolor', 'alpha'
            }
            safe_style = {k: v for k, v in style.items() if k in allowed_keys}

            # resolve generated color if not provided
            if 'color' not in safe_style and line_local_idx < len(
                    colors_for_lines):
                safe_style['color'] = tuple(colors_for_lines[line_local_idx])
            else:
                # if color provided as dict like {'color': (r,g,b)} or as list
                c = safe_style.get('color')
                if isinstance(c, dict) and 'color' in c:
                    safe_style['color'] = tuple(c['color'])
                if isinstance(c, (list, np.ndarray)):
                    safe_style['color'] = tuple(c)

            ax.plot(x_data, y_data, label=label, **safe_style)
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
            pass

    if "save_path" in plot_spec:
        # Save to file when requested (non-blocking)
        try:
            try:
                canvas.draw()
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
        # prefer to clear the Figure and close it
        fig.clf()
    except Exception:
        pass
    try:
        plt.close(fig)
    except Exception:
        pass

    # For testing or interactive inspection, optionally return the Figure
    if plot_style and isinstance(plot_style, dict) and plot_style.get(
            'return_figure', False):
        return fig
    return None
