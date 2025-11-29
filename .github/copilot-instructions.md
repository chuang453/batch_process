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
- Run CLI pipeline:
  - python -m batch_processor.cli <root> -c config.yaml
- Generate a config template (CLI helper):
  - python -m batch_processor.cli --generate-template config.yaml

Important internal API & patterns
- ProcessingContext (decorators/processor.py): passed to processors and used for shared data, results, and metadata. Use context.data, context.shared and context.add_result to record results.
- Processors receive signature (path: Path, context: ProcessingContext, **kwargs). Use @processor(name="x", priority=60, source=SCRIPT_DIR, metadata={...}) to register and document behaviour.
- Priority ordering: engine sorts processors by `priority` (descending). The engine does not deduplicate names — same processor may be invoked multiple times if config lists it repeatedly.
- Error handling: engine wraps calls and records failures in context.results; processors should return structured dicts for downstream aggregation.
- Retry decorator exists (decorators.retry) for making network/unstable processors resilient — often used before @processor.

Where to look when editing or adding processors
- Add new built-in processors under `processors/` with `@processor(...)`.
- For project-specific or experimental processors place them in `plugins/` and ensure `load_plugins()` is called before running (see `main.py` and `cli/app.py`).
- To expose a processor to the GUI table use the decorator metadata fields (name, source, metadata) — GUI consumes `get_all_processors()`.

Validation & examples
- There is a quick standalone validator you can run locally without pytest: `test/run_validate_demo.py` — it creates a temp tree, runs `backup_file`, and verifies the backup.
- A pytest-style unit test is provided at `test/test_validate.py` for automation; note some environments (Windows consoles) may require UTF-8 stdout and some versions of wcmatch expose underscore-prefixed constants (_PATHNAME/_GLOBSTAR) — the tests include small compatibility guards.

Developer notes for AI contributors
- Prefer to read `core/engine.py` and `decorators/processor.py` before changing behavior — these contain the canonical control flow and conventions.
- When writing quick fixes or tests, use `config/generate_template()` and small temporary directories under `test/` (there are examples in `test/test1` and `test/blade_load_extract`).
- Keep changes backward-compatible with decorator registration — avoid renaming registered processor names without adding aliases.

If anything here is unclear or you need a different level of detail (examples, flow diagrams, or test harness), tell me what to expand.
