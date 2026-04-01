---
mode: 'agent'
description: 'Use when fixing a bug in batch_process with impact-based test execution.'
---
Fix a bug with minimal blast radius and explicit verification.

Workflow:
1. Reproduce or identify the failing path.
2. Inspect changed files with `git diff --name-only HEAD`.
3. Select tests using impact map:
   - `git diff --name-only HEAD | python harness/test_selector.py`
4. Run selected tests first. If no tests are selected, run a nearby suite manually.
5. If testmon metadata exists, run:
   - `pytest --testmon -q`
6. For broad refactors or uncertain impact, run:
   - `pytest -q`
7. Report:
   - root cause
   - fix summary
   - exact tests run and outcomes
