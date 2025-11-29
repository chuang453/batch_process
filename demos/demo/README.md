Demo: End-to-end example for the batch_processor project

This demo shows a minimal, runnable example of the project's pipeline using the built-in processors.

Structure
- demo/sample_tree/        (small directory tree with .txt files)
- demo/config.yaml        (rules telling the engine which processors to run)

What the demo exercises
- backup_file: copies *.txt files into a backup folder keeping directory structure
- rename_file: renames files by prefixing processed_
- set_path_name_dict: loads a simple mapping file for directory label metadata

Quick steps (from repo root)
1) Run the standalone demo script (creates its own temporary tree and runs backup):

   python test/run_validate_demo.py

2) Use the CLI against `demo/sample_tree` with the demo config:

   python -m batch_processor.cli demo/sample_tree -c demo/config.yaml

3) Inspect `demo/sample_tree` and `demo/backups` to verify changes (backup created, files renamed when config says so).

Notes
- The engine uses glob-like config patterns (supports `**` and trailing `/` to indicate directories). See `core/engine.py` for matching behavior.
- Plugins are loaded automatically by `config/loader.py::load_plugins()` if you place module files in `plugins/`.

If you'd like, I can add a GitHub Action to run this demo as part of CI and verify results automatically.
