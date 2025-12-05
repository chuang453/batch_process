print('MATPLOTLIB AGG TEST START')
try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    fig = Figure(figsize=(4, 3))
    canvas = FigureCanvas(fig)
    ax = fig.add_subplot(111)
    ax.plot([1, 2, 3])
    canvas.draw()
    fig.savefig('debug_logs/agg_test.png')
    print('SAVED OK')
except Exception:
    import traceback
    traceback.print_exc()
    print('FAILED')
print('END')
