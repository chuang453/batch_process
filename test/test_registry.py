from stage2_platform.contracts import RunManifest, SeriesManifest
from stage2_platform.registry import LineageTracker, RunJournal


def test_run_journal_append_and_read(tmp_path):
    p = tmp_path / 'journal.jsonl'
    journal = RunJournal(p)

    rec = SeriesManifest(series_name='s1', input_key='df', output_key='out', status='success')
    run = RunManifest(run_id='r1', project_name='p', status='done', series_records=[rec])
    journal.append(run)

    rows = journal.read_all()
    assert len(rows) == 1
    assert rows[0]['run_id'] == 'r1'


def test_lineage_tracker_basic():
    lin = LineageTracker()
    lin.add('df', 's1', 'out')
    lin.add('df', 's2', 'out2')

    out = lin.get_by_output('out')
    assert len(out) == 1
    assert out[0]['series_name'] == 's1'
