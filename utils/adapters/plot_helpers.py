from pathlib import Path
from typing import Any, Dict, List

def save_plot_png_values(values: List[float], out_path: Path, cfg: Dict[str, Any]) -> Path:
    """Render values into a PNG. Prefer Agg; fallback to Pillow.
    This adapter keeps plotting separate from core pipeline utils.
    """
    try:
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
        fig = Figure(figsize=(cfg.get("fig_width", 4), cfg.get("fig_height", 3)), dpi=cfg.get("dpi", 100))
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
        draw.rectangle([margin, margin, margin + plot_w, margin + plot_h], outline=(0, 0, 0))
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
