from decorators.processor import processor, post_processor
from pathlib import Path

@processor(name="count_lines", priority=70, source=__file__, metadata={
    "name": "Count Lines",
    "author": "demo",
    "version": "0.1",
    "description": "Count lines in text files and store in context.data['analysis']",
})
def count_lines(path: Path, context, **kwargs):
    if not path.is_file():
        return {"skipped": True}
    try:
        text = path.read_text(encoding='utf-8', errors='ignore')
        lines = text.splitlines()
        n = len(lines)
        # store a per-file analysis entry under context.data['analysis']
        analysis = context.setdefault_data(['analysis', str(path)], {})
        analysis.update({"count_lines": n})
        return {"file": str(path), "count_lines": n}
    except Exception as e:
        return {"file": str(path), "error": str(e)}


@post_processor(name="generate_summary", priority=50, source=__file__, metadata={
    "name": "Generate Summary",
    "author": "demo",
    "version": "0.1",
    "description": "Collect results from the run and write a small summary file in the root",
})
def generate_summary(context, **kwargs):
    root = context.root_path or Path('.')
    report = {
        "total_results": len(context.results),
        "files_counted": [],
    }
    for r in context.results:
        if r.get('processor') == 'count_lines' and isinstance(r.get('result'), dict):
            report['files_counted'].append({r.get('path'): r['result'].get('count_lines')})

    out = root / kwargs.get('out_file', 'summary.json')
    try:
        import json
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        return {"report": str(out), "summary": report}
    except Exception as e:
        return {"error": str(e)}
