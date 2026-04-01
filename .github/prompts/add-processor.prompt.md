---
mode: 'agent'
description: 'Use when adding or modifying a @processor implementation and its tests in batch_process.'
---
Implement or update a processor safely in this repository.

Workflow:
1. Read `decorators/processor.py`, `core/engine.py`, and the target file under `processors/`.
2. Keep existing processor names stable. If behavior changes, prefer adding an alias over renaming.
3. Return structured results (dict or `context.add_result`) so recorders can persist useful output.
4. Add or update tests in `test/`.
5. Run targeted verification first:
   - `pytest test/test_file_ops.py test/test_validate.py -q`
6. If the change touches pipeline/recorders, also run affected suites from `harness/test_selector.py`.
7. Report:
   - changed files
   - behavior impact
   - exact tests run and pass/fail
