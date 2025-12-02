"""Pytest conftest to ensure repo root is on sys.path during test collection.

This helps VS Code / pytest discovery find local packages when the workspace
root is the repository root and tests import the inner package.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    # insert at front so tests import local package first
    sys.path.insert(0, ROOT_STR)
