import runpy, traceback, sys

try:
    runpy.run_path(
        r'd:/git_reposity/batch_process/scripts/demo_plot_legend.py',
        run_name='__main__')
except Exception:
    traceback.print_exc()
    sys.exit(1)
