---
mode: 'agent'
description: 'Use when adding or updating tests for batch_process modules and processors.'
---
Add focused, maintainable tests that match repository conventions.

Guidelines:
1. Put tests in `test/` and prefer existing naming patterns (`test_<module>.py`).
2. Reuse fixtures/helpers from `test/pytest_helpers.py` where possible.
3. Prefer deterministic assertions over broad snapshots.
4. Cover behavior at module boundaries (engine, pipeline, loader, processors).
5. Keep tests small and independent.

Verification:
1. Run the specific test file(s) you changed with `pytest -q <file>`.
2. Run affected tests from the impact map:
   - `git diff --name-only HEAD | python harness/test_selector.py`
3. If needed, run full regression:
   - `pytest -q`

Output format:
- list added/updated tests
- list assertions/behaviors covered
- exact commands run and pass/fail
