from pathlib import Path
import pandas as pd
from processors.plotting import plot_from_spec
from decorators.processor import ProcessingContext


def test_duplicate_series_labels_produces_listed_meta(tmp_path):
    # shared df
    df = pd.DataFrame({
        'x': list(range(8)),
        'y': [i * 2 for i in range(8)],
        'grp': ['A', 'A', 'B', 'B', 'A', 'B', 'A', 'B']
    })

    def make_extractor(shared):

        def extract(series, data, target):
            # always return (df, meta) with label included
            grp = series.get('group')
            if grp:
                subset = shared[shared['grp'] == grp]
                return subset, {
                    'cache_key': f'group_{grp}',
                    'rows': len(subset)
                }
            return shared, {'cache_key': 'full', 'rows': len(shared)}

        return extract

    extractor = make_extractor(df)

    # create a spec with two subplots using the same label
    spec = {
        'layout': {
            'rows': 1,
            'cols': 2
        },
        'subplots': [
            {
                'row': 0,
                'col': 0,
                'series': [{
                    'x': 'x',
                    'y': 'y',
                    'group': 'A',
                    'label': 'dup'
                }]
            },
            {
                'row': 0,
                'col': 1,
                'series': [{
                    'x': 'x',
                    'y': 'y',
                    'group': 'B',
                    'label': 'dup'
                }]
            },
        ],
        'save': {
            'filename': 'dup_labels.png'
        }
    }

    ctx = ProcessingContext()
    res = plot_from_spec(Path('t'),
                         ctx,
                         data=None,
                         spec=spec,
                         out_dir=str(tmp_path),
                         extract_f=extractor)
    assert res.get('status') == 'success'

    meta = ctx.get_data(['plot_extract_meta', str(Path('t'))])
    assert isinstance(meta, dict)
    # key 'dup' should exist and contain two meta entries
    assert 'dup' in meta
    assert isinstance(meta['dup'], list) and len(meta['dup']) == 2


def test_extractor_exception_is_recorded_in_warnings(tmp_path):
    # extractor that raises for series with label 'bad'
    def extractor(series, data, target):
        if series.get('label') == 'bad':
            raise RuntimeError('simulated extractor failure')
        # return simple inline df and meta
        return {'x': [0, 1], 'y': [0, 1]}, {'cache_key': 'inline', 'rows': 2}

    spec = {
        'layout': {
            'rows': 1,
            'cols': 2
        },
        'subplots': [
            {
                'row': 0,
                'col': 0,
                'series': [{
                    'x': 'x',
                    'y': 'y',
                    'label': 'good'
                }]
            },
            {
                'row': 0,
                'col': 1,
                'series': [{
                    'x': 'x',
                    'y': 'y',
                    'label': 'bad'
                }]
            },
        ],
        'save': {
            'filename': 'extractor_exc.png'
        }
    }

    ctx = ProcessingContext()
    res = plot_from_spec(Path('t'),
                         ctx,
                         data=None,
                         spec=spec,
                         out_dir=str(tmp_path),
                         extract_f=extractor)
    # function should not raise; should record a warning about extractor failure
    assert res.get('status') == 'success'
    warnings = res.get('warnings', []) or []
    assert any('extractor failed' in w or 'extractor' in w for w in warnings)

    # meta should contain only the successful 'good' series
    meta = ctx.get_data(['plot_extract_meta', str(Path('t'))])
    # may be present and should include 'good' but not 'bad'
    if meta:
        assert 'good' in meta
        assert 'bad' not in meta
