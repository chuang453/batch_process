Demo: extract_closure_demo

This demo shows how to use a closure-based `extract_f` with the plotting
processor and how to consume extractor metadata downstream.

Flow
- The demo creates a shared `pandas.DataFrame` and defines a closure
  extractor that filters the DataFrame by `group` and returns `(df, meta)`.
- It calls the processor `plot_from_spec(target, context, ..., extract_f=extractor)`.
- The plotting implementation collects extractor metadata and the
  processor stores a mapping under `context.data['plot_extract_meta'][str(target)]`.
- The demo then calls `write_plot_extract_summary(target, context)` to
  write the mapping as JSON under the demo output directory.

Files produced
- `demos/demo_extract_output/extract_demo.png` — plot image
- `demos/demo_extract_output/plot_extract_meta_demo_target.json` — summary JSON

See `demos/demo_extract_closure_demo.py` for runnable code.
