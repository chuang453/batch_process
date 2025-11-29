Advanced demo for batch_processor

This demo shows a slightly more complex pipeline:

- Pre-processor: set_path_name_dict at the root (will read demo_advanced/sample_tree/_dict.txt to populate labels)
- File processors: count_lines (from plugins/advanced_plugin.py), backup_file (built-in), and rename_file in a specific subfolder
- Post-processor: generate_summary (from plugins/advanced_plugin.py) writes demo_advanced/summary.json

Run locally:

    python demo_advanced/run_advanced.py

Or use the CLI (plugins are auto-loaded by main.run_pipeline which calls load_plugins()):

    python -m batch_processor.cli demo_advanced/sample_tree -c demo_advanced/config.yaml

What to expect
- demo_advanced/backups: backed up copies of text files
- demo_advanced/summary.json: a short JSON summary with files counted

This demo exercises plugin discovery, pre/main/post processors, priorities, and config driven processing.
