import pandas as pd

from core.pipeline import Pipeline
from decorators.processor import ProcessingContext, processor, transform


@processor(name='collect_txt_to_main_df_for_test')
def collect_txt_to_main_df_for_test(path, context, **kwargs):
    rows = context.get_main('df_rows', [])
    rows.append({'name': path.name, 'size': path.stat().st_size})
    context.set_main('df_rows', rows)
    context.set_main('df', pd.DataFrame(rows))
    return {'count': len(rows)}


@transform(name='filter_size_positive_for_test')
def filter_size_positive_for_test(df, context, **kwargs):
    return df[df['size'] > 0].copy()


def test_pipeline_walk_then_data(tmp_path):
    (tmp_path / 'a.txt').write_text('aa', encoding='utf-8')
    (tmp_path / 'b.txt').write_text('bb', encoding='utf-8')

    stages = [
        {
            'name': 'collect',
            'type': 'walk',
            'root': str(tmp_path),
            'config': {
                '**/*.txt': {
                    'processors': ['collect_txt_to_main_df_for_test']
                }
            }
        },
        {
            'name': 'analyze',
            'type': 'data',
            'source': 'df',
            'steps': [
                {
                    'run': ['filter_size_positive_for_test']
                },
                {
                    'group_by': 'name',
                    'collect': True,
                    'steps': [
                        {
                            'head': 1
                        },
                    ],
                },
            ],
        },
    ]

    ctx = Pipeline(stages=stages).run(root_path=tmp_path)
    out = ctx.get_main('df')

    assert isinstance(out, pd.DataFrame)
    assert len(out) == 2


def test_pipeline_simulate_contains_stage(tmp_path):
    stages = [{
        'name': 'collect',
        'type': 'walk',
        'root': str(tmp_path),
        'config': {
            '**/*.txt': {
                'processors': ['collect_txt_to_main_df_for_test']
            }
        }
    }]
    sim = Pipeline(stages=stages, context=ProcessingContext()).simulate(
        root_path=tmp_path)

    assert 'total_steps' in sim
    if sim['steps']:
        assert sim['steps'][0].get('stage') == 'collect'
