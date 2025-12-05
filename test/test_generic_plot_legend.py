import os
import math
import pytest

from utils.adapters.plot_helpers import generic_plot


def test_generic_plot_legend_bottom():
    """Render a figure with many lines and assert the legend is below the axes.

    The test requests the Figure object back via `plot_style['return_figure']`
    so it can inspect the legend and axes bounding boxes using the Agg
    renderer. This avoids opening any interactive windows.
    """

    def extract_f(param):
        if param == 'x':
            return list(range(20))
        if isinstance(param, str) and param.startswith('y'):
            i = int(param[1:])
            return [j + i * 0.1 for j in range(20)]
        return []

    subplots = [{
        "pos": (1, 1, 1),
        "title":
        "many-lines test",
        "lines": [{
            "x": "x",
            "y": (f"y{i}", f"line{i}", {})
        } for i in range(12)]
    }]

    plot_spec = {"subplots": subplots}
    # force legend placement to bottom and return the figure for inspection
    fig = generic_plot(
        extract_f,
        plot_spec,
        plot_style={
            "legend_threshold": 1,
            "legend_position": "bottom",
            "return_figure": True
        },
    )

    assert fig is not None, "generic_plot did not return a Figure"

    # Ensure renderer is available
    canvas = getattr(fig, "canvas", None)
    if canvas is None:
        pytest.skip("No canvas available for this matplotlib backend")

    canvas.draw()
    renderer = canvas.get_renderer()

    # inspect first axis
    ax = fig.axes[0]
    legend = ax.get_legend()
    assert legend is not None, "legend not created"

    # bounding boxes in display coordinates
    legend_bb = legend.get_window_extent(renderer)
    ax_bb = ax.get_window_extent(renderer)

    # For bottom placement, legend's top (y1) should be below axes' bottom (y0)
    assert legend_bb.y1 <= ax_bb.y0 + 1e-6, (
        f"Legend not below axes: legend_bb={legend_bb}, ax_bb={ax_bb}")
