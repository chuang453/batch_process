from pathlib import Path

import pandas as pd

from core.pipeline import Pipeline
from decorators.processor import ProcessingContext, processor, transform


@processor(name='collect_rows_for_signal_test')
def collect_rows_for_signal_test(path: Path, context: ProcessingContext, **kwargs):
    rows = context.get_main('rows', [])
    rows.append({'name': path.name, 'size': path.stat().st_size})
    context.set_main('rows', rows)
    context.set_main('df', pd.DataFrame(rows))
    return {'rows': len(rows)}


@transform(name='signal_test_transform')
def signal_test_transform(df, context, **kwargs):
    out = df.copy()
    out['size2'] = out['size'] * 2
    return out


class _DummySignal:

    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args)


class _DummyWorker:

    def __init__(self):
        self.step_started = _DummySignal()
        self.step_finished = _DummySignal()

    def thread(self):
        return None


def test_pipeline_worker_signals_use_global_step_offsets(tmp_path):
    (tmp_path / 'a.txt').write_text('a', encoding='utf-8')
    (tmp_path / 'b.txt').write_text('bb', encoding='utf-8')

    stages = [
        {
            'name': 'walk_stage',
            'type': 'walk',
            'root': str(tmp_path),
            'config': {
                '**/*.txt': {
                    'processors': ['collect_rows_for_signal_test']
                }
            },
        },
        {
            'name': 'data_stage',
            'type': 'data',
            'source': 'df',
            'steps': [
                {
                    'run': ['signal_test_transform']
                },
                {
                    'head': 2
                },
            ],
        },
    ]

    worker = _DummyWorker()
    progress_events = []

    p = Pipeline(stages=stages, context=ProcessingContext())
    p.set_worker(worker)
    p.set_progress_callback(lambda c, t, s: progress_events.append((c, t, s)))
    p.run(root_path=tmp_path)

    assert worker.step_started.calls
    assert worker.step_finished.calls

    started_steps = [int(args[0]) for args in worker.step_started.calls]
    finished_steps = [int(args[0]) for args in worker.step_finished.calls]

    assert started_steps == sorted(started_steps)
    assert finished_steps == sorted(finished_steps)
    assert max(started_steps) <= max(finished_steps)

    # total in progress callback should be global total steps
    totals = {int(t) for _, t, _ in progress_events}
    assert len(totals) == 1
    total = list(totals)[0]
    assert total >= 4
