Short, focused reference for AI coding agents working on this repo.

Overview
- This project is a recursive batch-processing framework (GUI + CLI) that applies named "processors" to files and directories based on a YAML/JSON config.
- Key components: `core/engine.py` (processing engine), `decorators/processor.py` (processor registration & ProcessingContext), `config/loader.py` (config & plugin loader), `processors/` and `plugins/` (built-in and external processors), `cli/app.py` and `main_window.py` (entrypoints).

What to know (high-impact facts)
- Processors are discovered and registered via decorators in `decorators/processor.py`:
  - @processor → registers a file-or-dir handler (PROCESSORS)
  - @pre_processor → registers a global/dir pre-run hook (PRE_PROCESSORS)
  - @post_processor → registers a global/summary hook (POST_PROCESSORS)
- The engine (BatchProcessor) applies processors found in the config map. Patterns in config (glob-like) map to lists of `processors`, `pre_processors`, and `post_processors`.
- Config keys that influence global hooks: `pre_process`, `post_process`, `config_pre`, `config_post` (see `core/engine.py` for exact behavior).
- Plugin discovery: `config/loader.py::load_plugins()` dynamically imports every `plugins/*.py` and registers functions into AVAILABLE_PROCESSORS.

Common tasks & examples
- List available processors (CLI):
  - python -m batch_processor.cli --processors
```instructions
Short, focused reference for AI coding agents working on this repo.

Overview
- Purpose: recursive batch-processing framework (GUI + CLI) that runs named "processors" over files/dirs using YAML/JSON config.
- Core modules:
  - `core/engine.py` — traversal, rule matching, sequencing, progress and simulate API (`BatchProcessor`).
  - `decorators/processor.py` — registration decorators (`@processor`, `@pre_processor`, `@post_processor`), `ProcessingContext`, `retry`, and `get_all_processors()`.
  - `config/loader.py` — YAML/JSON load/save, `load_plugins()` dynamic import, `generate_template()`.
  - `processors/` and `plugins/` — built-in and external processors (see `processors/builtin_recorders.py`).
  - `cli/app.py`, `main.py`, `main_window.py` — CLI and GUI entry points.

Big picture
- Config maps glob-like patterns (relative to `root_path`) to rule dicts that may include `pre_processors`, `processors` (inline), `post_processors`, `config`, and `priority`.
- `BatchProcessor` builds three phases per-matching-path (`pre`, `inline`, `post`), sorts each phase by priority (descending), and executes in this order. Directory processing: pre/inline → recurse children → post.
- Global hooks: `pre_process` and `post_process` with `config_pre`/`config_post` are run once each (see `BatchProcessor.run`).

Key APIs & conventions (practical)
- Register a processor with `@processor(name=..., priority=..., source=..., metadata={...})`. Example implementations: `processors/file_ops.py`.
- `@pre_processor` and `@post_processor` register lifecycle hooks. Global pre/post often accept `(context, **cfg)`.
- Processor signature: `(path: Path, context: ProcessingContext, **kwargs)`.
- `ProcessingContext` (see `decorators/processor.py`):
  - `.data`, `.shared`, `.metadata` (dicts) for storing run state
  - `.set_data`/`.get_data`/`.setdefault_data` and corresponding `.set_shared` / `.get_shared`
  - `.add_result(result)` to append structured results (used by recorders)
- Use `decorators.retry` to wrap unstable processors before `@processor`.

Pattern matching & priorities
- Patterns are matched against POSIX-style relative paths to `root_path`. Use `.` to match root.
- Directory patterns end with `/` and match directories only; `**/` matches directories at any depth. File patterns like `**/*.txt` match files.
- Within each phase (`pre`, `inline`, `post`) processors are sorted by the `priority` integer (higher runs first). The engine does not deduplicate — repeated names are invoked repeatedly.

Built-in recorders
- Prefer explicit recorders for stable persistence: `record_to_shared`, `persist_history_sqlite`, `persist_history_jsonl` live in `processors/builtin_recorders.py` and use an async SQLite writer.
- Engine can inject recorders automatically when `enable_builtin_recorders` is set in top-level config (see `core/engine.py`).

CLI & quick commands
- List processors: `python -m batch_process.cli --processors`
- Generate template: `python -m batch_process.cli --generate-template config.yaml`
- Run CLI: `python -m batch_process.cli <root> -c config.yaml`
- Quick validate demo: `python test/run_validate_demo.py` (creates temp tree, runs `backup_file`, verifies backup)

Files to read when changing behavior
- `decorators/processor.py` — registration, `ProcessingContext`, `retry`, `get_all_processors()`
- `core/engine.py` — matching (`_match_rule`), counting, sequencing, `_execute_processor_list_with_progress`, `simulate()`
- `config/loader.py` — YAML handling, `load_plugins()` autoloader, `generate_template()`
- `processors/builtin_recorders.py` — recorder APIs and async SQLite writer

Testing
- Run unit tests:
  - PowerShell:
    ```powershell
    pip install -r requirements.txt  # if present
    pytest -q
    ```
- Run the standalone validator:
  - `python test/run_validate_demo.py`

AI agent notes
- Read `core/engine.py` and `decorators/processor.py` before changes — they define the central control flow.
- When adding processors, include `metadata` in the decorator for GUI visibility and return structured dicts or call `context.add_result()` so recorders can persist meaningful entries.
- Do not rename processor names used in configs; add aliases if necessary.
- Use `load_plugins()` to load modules under `plugins/` during CLI runs and tests.

If you'd like, I can expand with: (a) a minimal example processor file, (b) a sample config demonstrating recorder injection, or (c) a checklist for adding GUI signal tests.

```

Verification workflow (harness)
- After any code change, run selective verification first:
  - `pytest --testmon -q`
- If test selection needs to be explicit from changed paths, use:
  - `git diff --name-only HEAD | python harness/test_selector.py`
  - Then run the emitted tests with pytest.
- When `--testmon` data is missing (first run) or broad refactors occur, run full regression:
  - `pytest -q`

Impact map (changed module -> likely tests)
- `core/engine.py` -> `test/test_validate.py`, `test/test_file_ops.py`, `test/test_main_routing.py`
- `core/data_stage.py` -> `test/test_data_stage.py`, `test/test_transform.py`
- `core/pipeline.py` -> `test/test_pipeline.py`, `test/test_pipeline_helpers.py`, `test/test_pipeline_simulate_compat.py`
- `decorators/processor.py` -> `test/test_builtin_recorders.py`, `test/test_processor_metadata.py`, `test/test_validate.py`
- `config/loader.py` -> `test/test_config_loader_pipeline.py`, `test/test_validate.py`
- `processors/` -> `test/test_file_ops.py`, `test/test_builtin_recorders.py`
- `widgets/batch_thread.py` -> `test/test_pipeline_worker_signals.py`

Required response format after edits
- Summarize changed files and behavior impact.
- List exactly which tests were executed.
- Report pass/fail and any skipped validation.
