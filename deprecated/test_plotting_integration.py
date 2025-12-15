from pathlib import Path
import pandas as pd
from processors.plotting import plot_from_spec
from decorators.processor import ProcessingContext


def test_plot_from_spec_processor_forwards_extractor(tmp_path):
    # shared dataframe
    df_all = pd.DataFrame({
        'x': list(range(6)),
        'y': [i * 3 for i in range(6)],
        'grp': ['A', 'A', 'B', 'B', 'A', 'B']
    })

    def make_extractor(shared_df):

        def extract(series, data, target):
            if series.get('data') is not None:
                df_inline = pd.DataFrame(series['data'])
                return df_inline, {"source": "inline", "rows": len(df_inline)}
            grp = series.get('group') or series.get('grp')
            if grp:
                subset = shared_df[shared_df['grp'] == grp]
                return subset, {
                    "cache_key": f"group_{grp}",
                    "rows": len(subset)
                }
            return shared_df, {"cache_key": "full", "rows": len(shared_df)}

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
                'group': 'A',
                'label': 'A series'
            }]
        }],
        'save': {
            'filename': 'integration_plot.png'
        }
    }

    ctx = ProcessingContext()
    out_dir = str(tmp_path)

    res = plot_from_spec(Path('t'),
                         ctx,
                         data=None,
                         spec=spec,
                         out_dir=out_dir,
                         extract_f=extractor)
    assert res.get('status') == 'success'
    # context should record the figure path in data['plots']
    plots = ctx.get_data(['plots'], [])
    assert isinstance(
        plots, list) and plots, 'plots entry must be present in context data'
    assert Path(plots[0]).exists()
    # results should include a record of the processor call
    assert any(r.get('processor') == 'plot_from_spec' for r in ctx.results)
    # plot_extract_meta should be persisted into context.data under ['plot_extract_meta', str(target)]
    meta = ctx.get_data(['plot_extract_meta', str(Path('t'))])
    assert meta is not None, 'plot_extract_meta must be present in ProcessingContext.data'
    # meta should be a dict keyed by series label (values are lists of meta dicts)
    assert isinstance(
        meta, dict) and meta, 'plot_extract_meta should be a non-empty dict'
    # our extractor used label 'A series' so that key should exist
    assert 'A series' in meta
    assert isinstance(
        meta['A series'],
        list) and meta['A series'][0].get('cache_key') in ('group_A', 'full')
