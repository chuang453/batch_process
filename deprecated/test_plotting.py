from pathlib import Path
import pandas as pd
from processors._impl import plotting_impl as impl


def test_prepare_and_plot_impl(tmp_path):
    # Create an in-memory DataFrame and plot it
    df = pd.DataFrame({
        'time': [0, 1, 2, 3],
        'val1': [0, 1, 4, 9],
    })

    spec = {
        'title':
        'T',
        'layout': {
            'rows': 1,
            'cols': 1
        },
        'subplots': [{
            'title':
            'S',
            'series': [{
                'x': 'time',
                'y': 'val1',
                'style': '-o',
                'label': 'v1'
            }]
        }],
        'save': {
            'filename': 'out_test_plot.png',
            'dpi': 80
        }
    }

    res = impl.plot_from_spec_impl(tmp_path,
                                   data=df,
                                   spec=spec,
                                   out_dir=str(tmp_path / 'plots'))
    assert res.get('status') == 'success'
    figpath = Path(res.get('figure_path'))
    assert figpath.exists()

    # Now create a sqlite DB, prepare data via impl, and plot
    import sqlite3
    dbp = tmp_path / 'd.db'
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    cur.execute('CREATE TABLE measurements (ts INTEGER, v REAL)')
    cur.executemany('INSERT INTO measurements VALUES (?,?)', [(0, 1.0),
                                                              (1, 2.0)])
    conn.commit()
    conn.close()

    db_url = f"sqlite:///{str(dbp)}"
    q = 'SELECT ts as time, v FROM measurements'
    prep = impl.prepare_plot_data_impl(tmp_path,
                                       db_url=db_url,
                                       query=q,
                                       cache_key='ck1')
    assert prep.get('status') == 'cached'
    assert prep.get('rows') == 2

    df2 = prep.get('df')
    assert df2 is not None

    res2 = impl.plot_from_spec_impl(tmp_path,
                                    data=df2,
                                    spec={
                                        'layout': {
                                            'rows': 1,
                                            'cols': 1
                                        },
                                        'subplots': [{
                                            'series': [{
                                                'x': 'time',
                                                'y': 'v'
                                            }]
                                        }],
                                        'save': {
                                            'filename': 'sql_plot.png'
                                        }
                                    },
                                    out_dir=str(tmp_path / 'plots2'))
    assert res2.get('status') == 'success'
    assert Path(res2.get('figure_path')).exists()
    # Test explicit subplot placement with row/col and rowspan/colspan
    spec_span = {
        'title':
        'Span Test',
        'layout': {
            'rows': 2,
            'cols': 2
        },
        'subplots': [{
            'row': 0,
            'col': 0,
            'rowspan': 2,
            'colspan': 1,
            'title': 'Left tall',
            'series': [{
                'x': 'time',
                'y': 'val1',
                'label': 'left'
            }]
        }, {
            'row': 0,
            'col': 1,
            'title': 'Right top',
            'series': [{
                'x': 'time',
                'y': 'val1',
                'label': 'rt'
            }]
        }, {
            'row': 1,
            'col': 1,
            'title': 'Right bottom',
            'series': [{
                'x': 'time',
                'y': 'val1',
                'label': 'rb'
            }]
        }],
        'save': {
            'filename': 'span_plot.png'
        }
    }

    res3 = impl.plot_from_spec_impl(tmp_path,
                                    data=df,
                                    spec=spec_span,
                                    out_dir=str(tmp_path / 'plots3'))
    assert res3.get('status') == 'success'
    assert Path(res3.get('figure_path')).exists()
    q = 'SELECT ts as time, v FROM measurements'
    prep = impl.prepare_plot_data_impl(tmp_path,
                                       db_url=db_url,
                                       query=q,
                                       cache_key='ck1')
    assert prep.get('status') == 'cached'
    assert prep.get('rows') == 2

    df2 = prep.get('df')
    assert df2 is not None

    res2 = impl.plot_from_spec_impl(tmp_path,
                                    data=df2,
                                    spec={
                                        'layout': {
                                            'rows': 1,
                                            'cols': 1
                                        },
                                        'subplots': [{
                                            'series': [{
                                                'x': 'time',
                                                'y': 'v'
                                            }]
                                        }],
                                        'save': {
                                            'filename': 'sql_plot.png'
                                        }
                                    },
                                    out_dir=str(tmp_path / 'plots2'))
    assert res2.get('status') == 'success'
    assert Path(res2.get('figure_path')).exists()
