"""Advanced demo runner.

This runs `main.run_pipeline` against demo_advanced/sample_tree using demo_advanced/config.yaml.
It prints a summary and shows where the generated summary.json and backups end up.
"""
import sys
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from main import run_pipeline


def main():
    demo_root = Path(__file__).parent / "sample_tree"
    config = Path(__file__).parent / "config.yaml"

    print("Running advanced demo pipeline")
    ctx = run_pipeline(str(demo_root), str(config))

    # print some information from context
    print("\nSummary: results count:", len(ctx.results))
    for r in ctx.results:
        print(r)

    summary_path = Path(__file__).parent / "summary.json"
    if summary_path.exists():
        print("\nGenerated summary found:", summary_path)
        print(summary_path.read_text(encoding='utf-8'))
    else:
        print("\nNo summary.json generated at expected location.")

    backups = Path(__file__).parent / 'backups'
    print("Backups directory exists:", backups.exists())


if __name__ == '__main__':
    main()
