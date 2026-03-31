import pandas as pd

from core.data_stage import DataStage
from decorators.processor import ProcessingContext, TRANSFORMS, get_all_processors, transform


@transform(name='t_add_one_for_test')
def t_add_one_for_test(df, context, **kwargs):
    out = df.copy()
    out['v'] = out['v'] + 1
    return out


@transform(name='t_none_for_test')
def t_none_for_test(df, context, **kwargs):
    return None


def test_transform_registered():
    assert 't_add_one_for_test' in TRANSFORMS


def test_transform_chain_and_none_defense():
    ctx = ProcessingContext()
    ds = DataStage(ctx)
    df = pd.DataFrame({'v': [1, 2]})

    out = ds.run_steps(df, [{'run': ['t_add_one_for_test', 't_none_for_test']}])

    assert list(out['v']) == [2, 3]


def test_get_all_processors_includes_transform():
    all_items = get_all_processors()
    kinds = {item['kind'] for item in all_items}
    assert 'transform' in kinds
