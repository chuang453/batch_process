"""Run the demo config against demo/sample_tree and show result summary.

Usage:
    python demo/run_example.py

Output:
    Prints information and verifies that backups were created according to demo/config.yaml
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

    print("Running demo pipeline")
    ctx = run_pipeline(str(demo_root), str(config))

    # show a short summary
    print("\nResults summary (short):")
    for r in ctx.results:
        print(f" - {r.get('processor')} on {r.get('path')} -> {r.get('result')}")

    # verify backup file
    backup_path = Path(__file__).parent / "backups" / "file1.txt"
    if backup_path.exists():
        print("\nOK: backup exists:", backup_path)
    else:
        print("\nWARNING: expected backup not found:", backup_path)


if __name__ == '__main__':
    main()
