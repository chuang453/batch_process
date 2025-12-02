import pandas as pd
from pathlib import Path
from processors._impl.plotting_impl import plot_from_spec_impl


def test_plot_from_spec_impl_with_closure_extractor(tmp_path):
    # prepare a shared DataFrame with groups
    df_all = pd.DataFrame({
        'x': list(range(10)),
        'y': [i * 2 for i in range(10)],
        'group': ['A'] * 5 + ['B'] * 5
    })

    def make_extractor(df_shared: pd.DataFrame):

        def extract(series, data, target):
            # allow per-series inline data to override
            if series.get('data') is not None:
                return pd.DataFrame(series['data'])
            grp = series.get('group')
            if grp:
                return df_shared[df_shared['group'] == grp]
            return df_shared

        return extract

    extractor = make_extractor(df_all)

    spec = {
        'layout': {
            'rows': 1,
            'cols': 1
        },
        'subplots': [{
            'series': [{
                'x': 'x',
                'y': 'y',
                'label': 'grouped'
            }]
        }],
        'save': {
            'filename': 'extract_closure.png'
        }
    }

    out = plot_from_spec_impl(Path('t'),
                              data=None,
                              spec=spec,
                              out_dir=str(tmp_path),
                              extract_f=extractor)
    assert out.get('status') == 'success'
    fig_path = Path(out.get('figure_path'))
    assert fig_path.exists()


def test_plot_from_spec_impl_with_extractor_returning_none(tmp_path):
    # extractor that returns None for every series
    def extractor_none(series, data, target):
        return None

    spec = {
        'layout': {
            'rows': 1,
            'cols': 1
        },
        'subplots': [{
            'series': [{
                'x': 'x',
                'y': 'y',
            }]
        }],
        'save': {
            'filename': 'extract_none.png'
        }
    }

    out = plot_from_spec_impl(Path('t'),
                              data=None,
                              spec=spec,
                              out_dir=str(tmp_path),
                              extract_f=extractor_none)
    # function should complete and save an (empty) figure, but include warnings
    assert out.get('status') == 'success'
    assert 'warnings' in out
    assert any('no data available' in w for w in out.get('warnings', []))
    fig_path = Path(out.get('figure_path'))
    assert fig_path.exists()
