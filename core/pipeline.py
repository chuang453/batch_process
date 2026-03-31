from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from core.data_stage import DataStage
from core.engine import BatchProcessor
from decorators.processor import ProcessingContext


class Pipeline:

    def __init__(self,
                 stages: List[Dict],
                 context: ProcessingContext = None):
        self.stages = stages or []
        self.context = context or ProcessingContext()
        self.worker = None
        self.progress_callback = None

    def set_worker(self, worker):
        self.worker = worker

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def set_config(self, config: Dict):
        """Compatibility API with BatchProcessor: accept full config dict."""
        if isinstance(config, dict) and isinstance(config.get('pipeline'), list):
            self.stages = config.get('pipeline', [])

    def set_processors(self, pre=None, main=None, post=None):
        """Compatibility no-op: Pipeline does not use processor registries directly."""
        return self

    class _SignalProxy:

        def __init__(self, emit_fn):
            self._emit_fn = emit_fn

        def emit(self, *args):
            self._emit_fn(*args)

    class _WorkerProxy:

        def __init__(self, worker, offset: int):
            self._worker = worker
            self._offset = offset
            self.step_started = Pipeline._SignalProxy(self._emit_started)
            self.step_finished = Pipeline._SignalProxy(self._emit_finished)

        def _emit_started(self, step_idx: int):
            if hasattr(self._worker, 'step_started'):
                self._worker.step_started.emit(int(step_idx) + self._offset)

        def _emit_finished(self, step_idx: int, success: bool, message: str):
            if hasattr(self._worker, 'step_finished'):
                self._worker.step_finished.emit(int(step_idx) + self._offset,
                                                bool(success), message)

        def thread(self):
            if hasattr(self._worker, 'thread'):
                return self._worker.thread()
            return None

    def _stage_step_counts(self, root_default: Path) -> List[int]:
        counts: List[int] = []
        for stage in self.stages:
            stage_type = stage.get('type')
            if stage_type == 'walk':
                bp = BatchProcessor(config=stage.get('config', {}) or {})
                run_root = Path(stage.get('root', root_default))
                sim = bp.simulate(run_root, sequence=True)
                counts.append(int(sim.get('total_steps', 0)))
            elif stage_type == 'data':
                ds = DataStage(self.context)
                counts.append(len(ds.simulate_steps(None, stage.get('steps', []) or [])))
            else:
                counts.append(1)
        return counts

    def run(self,
            root_path: str | Path = None,
            context: ProcessingContext = None) -> ProcessingContext:
        if context is not None:
            self.context = context
        root_default = Path(root_path) if root_path is not None else Path('.')

        stage_counts = self._stage_step_counts(root_default)
        total_steps = sum(stage_counts)
        stage_offset = 0

        for idx, stage in enumerate(self.stages):
            stage_type = stage.get('type')
            stage_name = stage.get('name', stage_type or 'unknown')
            stage_steps = stage_counts[idx] if idx < len(stage_counts) else 0

            def stage_progress(current: int, _total: int, status: str):
                if self.progress_callback:
                    self.progress_callback(stage_offset + int(current),
                                           max(total_steps, 1),
                                           f"[{stage_name}] {status}")

            if stage_type == 'walk':
                walk_cfg = stage.get('config', {}) or {}
                bp = BatchProcessor(config=walk_cfg)
                if self.worker is not None:
                    bp.set_worker(self._WorkerProxy(self.worker, stage_offset))
                bp.set_progress_callback(stage_progress)
                run_root = Path(stage.get('root', root_default))
                bp.run(run_root, self.context)
                stage_offset += stage_steps
                continue

            if stage_type == 'data':
                source_key = stage.get('source', 'df')
                df = self.context.get_main(source_key)
                if df is None:
                    self.context.add_result({
                        'stage': stage_name,
                        'status': 'skipped',
                        'reason': f'main["{source_key}"] is None',
                    })
                    stage_offset += stage_steps
                    continue

                ds = DataStage(self.context)
                if self.worker is not None:
                    ds.set_worker(self._WorkerProxy(self.worker, stage_offset))
                ds.set_progress_callback(stage_progress)
                out_df = ds.run_steps(df, stage.get('steps', []) or [])
                self.context.set_main(source_key, out_df)
                stage_offset += stage_steps
                continue

            self.context.add_result({
                'stage': stage_name,
                'status': 'skipped',
                'reason': f'unknown stage type: {stage_type}',
            })
            stage_offset += stage_steps

        return self.context

    def simulate(self,
                 root_path: str | Path = None,
                 max_items: int | None = None,
                 pattern_filter: str | None = None,
                 sequence: bool | None = None) -> Any:
        root_default = Path(root_path) if root_path is not None else Path('.')
        steps = []
        actions = []
        legacy_dict_mode = sequence is None and max_items is None and pattern_filter is None
        if sequence is None:
            sequence = False

        for stage in self.stages:
            stage_type = stage.get('type')
            stage_name = stage.get('name', stage_type or 'unknown')

            if stage_type == 'walk':
                bp = BatchProcessor(config=stage.get('config', {}) or {})
                run_root = Path(stage.get('root', root_default))
                if sequence:
                    sim = bp.simulate(run_root, sequence=True)
                    for item in sim.get('steps', []):
                        item['stage'] = stage_name
                        steps.append(item)
                else:
                    stage_actions = bp.simulate(run_root,
                                                max_items=max_items,
                                                pattern_filter=pattern_filter,
                                                sequence=False)
                    for item in stage_actions:
                        item['stage'] = stage_name
                        actions.append(item)
                continue

            if stage_type == 'data':
                ds = DataStage(self.context)
                data_steps = ds.simulate_steps(None, stage.get('steps', []) or [])
                if sequence:
                    base_step = len(steps)
                    for idx, item in enumerate(data_steps, start=1):
                        steps.append({
                            'step': base_step + idx,
                            'phase': item.get('step_type', 'data'),
                            'path': f'.pipeline/{stage_name}',
                            'is_dir': True,
                            'proc_name': item.get('step_type', 'data'),
                            'config': item.get('detail', {}),
                            'stage': stage_name,
                        })
                else:
                    proc_names = []
                    for item in data_steps:
                        detail = item.get('detail', {})
                        if isinstance(detail, dict) and 'run' in detail:
                            proc_names.extend([str(x) for x in (detail.get('run') or [])])
                        else:
                            proc_names.append(item.get('step_type', 'data'))
                    actions.append({
                        'path': f'.pipeline/{stage_name}',
                        'is_dir': True,
                        'pre_processors': [],
                        'processors': [{'name': p, 'config': {}} for p in proc_names],
                        'post_processors': [],
                        'stage': stage_name,
                    })
                continue

            if sequence:
                steps.append({
                    'step': len(steps) + 1,
                    'phase': 'unknown-stage',
                    'path': f'.pipeline/{stage_name}',
                    'is_dir': True,
                    'proc_name': 'unknown-stage',
                    'config': stage,
                    'stage': stage_name,
                })
            else:
                actions.append({
                    'path': f'.pipeline/{stage_name}',
                    'is_dir': True,
                    'pre_processors': [],
                    'processors': [{'name': 'unknown-stage', 'config': stage}],
                    'post_processors': [],
                    'stage': stage_name,
                })

        if sequence:
            return {'total_steps': len(steps), 'steps': steps}
        if legacy_dict_mode:
            # Backward-compatible default shape used by earlier tests/callers.
            seq = self.simulate(root_path=root_path, sequence=True)
            return seq
        if max_items is not None:
            return actions[:max_items]
        return actions
