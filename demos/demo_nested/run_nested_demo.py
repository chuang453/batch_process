"""Run the nested folder Word+data extraction demo.
Generates a report docx with per-folder tables, plots, and summary paragraphs.
Also prints a merged DataFrame of file stats.
"""
from pathlib import Path
import yaml
from decorators.processor import ProcessingContext
from core.engine import BatchProcessor
from utils.exporters import results_to_dataframe

# Ensure plugin module is imported so processors register
from demos.demo_nested.plugins import nested_word_pipeline  # noqa: F401


def load_yaml(path: Path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def main():
    demo_root = Path(__file__).parent / 'sample_tree'
    config_path = Path(__file__).parent / 'config.yaml'
    config = load_yaml(config_path)

    context = ProcessingContext()
    engine = BatchProcessor(config=config)
    context = engine.run(demo_root, context=context)

    # Build DataFrame of raw processor results (only file processed entries)
    rows = []
    for r in context.results:
        if r.get('processor') == 'process_data_file' and 'result' in r:
            stat = r['result']
            rows.append(stat)
    if rows:
        df = results_to_dataframe(rows, engine='pandas')
        print('\nMerged file stats DataFrame:')
        print(df)
    else:
        print('No file stats collected.')

    print('\nReport saved at:', context.shared.get('pipeline', {}).get('outputs', {}).get('doc_path', 'see set_output storage'))

if __name__ == '__main__':
    main()
