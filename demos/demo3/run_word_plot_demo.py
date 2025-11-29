"""Run the word-plot pipeline demo.

Select a root folder (e.g., demos/demo2/data) and use demos/word_plot_config.yaml.
Requires: matplotlib, python-docx.
"""
import sys
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from main import run_pipeline


def main():
    # default demo root; adjust if needed
    root = Path('demos/demo2/data')
    config = Path('demos/word_plot_config.yaml')

    print('Running word-plot demo...')
    ctx = run_pipeline(str(root), str(config))
    print('results:', len(ctx.results))
    for r in ctx.results[:10]:
        print(r)
    out_doc = Path('demos/output.docx')
    print('word doc exists:', out_doc.exists(), out_doc)
    img_dir = Path('demos/images')
    print('images dir exists:', img_dir.exists())


if __name__ == '__main__':
    main()
