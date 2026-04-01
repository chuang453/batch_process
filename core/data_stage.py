from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List

import pandas as pd

from decorators.processor import ProcessingContext, TRANSFORMS
from stage2_platform.contracts.step_descriptor import StepEvent, StepResult
from stage2_platform.execution.builtin_ops import BUILTIN_OPS
from stage2_platform.execution.step_parser import parse_steps
from utils.adapters.df_helpers import prepend_dict_columns, split_dataframe_by_groups


class DataStage:
    """Execute data stage steps: builtin DataFrame ops, transforms and nested group_by."""

    def __init__(self, context: ProcessingContext):
        self.context = context
        self.progress_callback = None
        self.worker = None
        self._transforms = TRANSFORMS

    def set_worker(self, worker):
        self.worker = worker

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def _call_progress(self, current: int, total: int, status: str):
        if self.progress_callback:
            try:
                self.progress_callback(current, total, status)
            except Exception:
                pass

    def _record_pipe(self, name: str, payload: Dict[str, Any], phase: str = 'data'):
        if not isinstance(payload, dict):
            return
        self.context.pipe[name] = payload
        self.context.pipe_log.append({
            'path': '.',
            'proc_name': name,
            'phase': phase,
            'keys': list(payload.keys()),
            'ts': datetime.now().isoformat(sep=' ', timespec='seconds')
        })

    def _emit_event(self, observer: Callable[[StepEvent], None] | None, event: StepEvent):
        if observer is not None:
            try:
                observer(event)
            except Exception:
                pass

        # Compatibility bridge: keep existing worker signal behavior.
        if self.worker is not None and hasattr(self.worker, 'step_started') and event.kind == 'started':
            try:
                self.worker.step_started.emit(int(str(event.step_id).split('.')[-1]))
            except Exception:
                pass
        if self.worker is not None and hasattr(self.worker, 'step_finished') and event.kind in ('finished', 'failed'):
            try:
                success = event.kind == 'finished'
                self.worker.step_finished.emit(int(str(event.step_id).split('.')[-1]), success, event.error)
            except Exception:
                pass

    def run_steps(self,
                  df: Any,
                  steps: List[dict],
                  on_error: str = 'continue',
                  observer: Callable[[StepEvent], None] | None = None,
                  return_result: bool = False) -> Any:
        out = df
        descriptors = parse_steps(steps or [])
        total = len(descriptors)
        failed_count = 0

        for idx, desc in enumerate(descriptors, start=1):
            step_name = desc.op_name or desc.op_type
            self._emit_event(observer, StepEvent(desc.step_id, step_name, 'started', ''))
            try:
                if desc.op_type == 'group_by':
                    out = self._run_group_by(out,
                                             desc.detail,
                                             on_error=on_error,
                                             observer=observer,
                                             parent_step=desc.step_id,
                                             exec_frame={})
                elif desc.op_type == 'transform':
                    cfg = desc.detail.get('config', {}) if isinstance(desc.detail, dict) else {}
                    out = self._run_transform_chain(out,
                                                    list(desc.params or []),
                                                    cfg,
                                                    on_error=on_error)
                else:
                    out = self._run_builtin_op(out, desc)

                self._call_progress(idx, total, f"data step {idx}/{total}: {step_name}")
                self._emit_event(observer, StepEvent(desc.step_id, step_name, 'finished', ''))
            except Exception as e:
                failed_count += 1
                self.context.add_result({
                    'processor': step_name,
                    'status': 'failed',
                    'error': str(e),
                })
                self._emit_event(observer, StepEvent(desc.step_id, step_name, 'failed', str(e)))

                policy = str(on_error or 'continue').lower()
                if policy == 'abort':
                    raise
                if policy == 'skip_remaining':
                    break

        result = StepResult(
            df=out,
            success=failed_count == 0,
            error='' if failed_count == 0 else f'{failed_count} step(s) failed',
            step_id=descriptors[-1].step_id if descriptors else '',
            op_name=descriptors[-1].op_name if descriptors else '',
        )
        if return_result:
            return result
        return result.df

    def simulate_steps(self, df: Any, steps: List[dict],
                        _prefix: str = '', _level: int = 0) -> List[dict]:
        """Produce a flat list describing each step for preview purposes.

        For ``group_by`` steps the sub-steps are recursively expanded with
        dotted numbering (e.g. ``3.1``, ``3.2``) so the UI can render them
        with indentation.
        """
        records: List[dict] = []
        parsed = parse_steps(steps or [], prefix=_prefix, level=_level, flatten=True)
        for desc in parsed:
            step_type = 'builtin_op' if desc.op_type == 'builtin' else desc.op_type
            records.append({
                'step': desc.step_id,
                'step_type': step_type,
                'op_name': desc.op_name,
                'level': desc.level,
                'detail': desc.detail,
            })
        return records

    def _run_builtin_op(self, df: Any, desc) -> Any:
        if not isinstance(df, pd.DataFrame):
            self.context.add_result({
                'processor': desc.op_name or 'builtin_op',
                'status': 'skipped',
                'reason': 'input is not DataFrame',
                'detail': desc.detail,
            })
            return df

        op = BUILTIN_OPS.get(desc.op_name)
        if op is None:
            self.context.add_result({
                'processor': desc.op_name or 'builtin_op',
                'status': 'skipped',
                'reason': 'unknown op',
                'detail': desc.detail,
            })
            return df

        try:
            return op(df, desc.params)
        except Exception as e:
            self.context.add_result({
                'processor': desc.op_name or 'builtin_op',
                'status': 'failed',
                'error': str(e),
                'detail': desc.detail,
            })
            raise

    def _run_transform_chain(self,
                             df: Any,
                             func_names: List[str],
                             config: Dict[str, Any],
                             on_error: str = 'continue') -> Any:
        out = df
        for fname in func_names or []:
            func = self._transforms.get(fname)
            if func is None:
                self.context.add_result({
                    'processor': fname,
                    'status': 'not_found',
                })
                continue

            try:
                result = func(out, self.context, **(config or {}))
                if result is None:
                    self.context.add_result({
                        'processor': fname,
                        'status': 'warning',
                        'message': f'{fname} returned None, keeping previous value',
                    })
                    continue
                out = result
                if isinstance(out, pd.DataFrame):
                    self._record_pipe(
                        fname,
                        {
                            'rows': int(len(out)),
                            'cols': list(out.columns),
                        },
                        phase='transform',
                    )
            except Exception as e:
                self.context.add_result({
                    'processor': fname,
                    'status': 'failed',
                    'error': str(e),
                })
                if str(on_error or 'continue').lower() == 'abort':
                    raise
        return out

    def _normalize_group_cols(self, cols) -> List[str]:
        if isinstance(cols, str):
            return [cols]
        return list(cols or [])

    def _run_group_by(self,
                      df: Any,
                      step: Dict[str, Any],
                      on_error: str = 'continue',
                      observer: Callable[[StepEvent], None] | None = None,
                      parent_step: str = '',
                      exec_frame: Dict[str, Any] | None = None) -> Any:
        if not isinstance(df, pd.DataFrame):
            self.context.add_result({
                'processor': 'group_by',
                'status': 'skipped',
                'reason': 'input is not DataFrame',
            })
            return df

        group_cols = self._normalize_group_cols(step.get('group_by', []))
        sub_steps = step.get('steps', []) or []
        collect = bool(step.get('collect', False))

        try:
            groups = split_dataframe_by_groups(df, group_cols)
        except Exception as e:
            self.context.add_result({
                'processor': 'group_by',
                'status': 'failed',
                'error': str(e),
                'group_by': group_cols,
            })
            return df

        frame = dict(exec_frame or {})
        frame['group_cols'] = group_cols
        frame['group_key'] = None

        collected: List[pd.DataFrame] = []
        total = len(groups)
        for idx, (group_key, group_df) in enumerate(groups, start=1):
            if self.worker and hasattr(self.worker, 'thread'):
                thr = self.worker.thread()
                if thr and thr.isInterruptionRequested():
                    break

            frame['group_key'] = group_key
            self._record_pipe('_group', {
                'key': group_key,
                'rows': int(len(group_df)) if hasattr(group_df, '__len__') else None,
                'cols': list(group_df.columns) if isinstance(group_df, pd.DataFrame) else None,
            }, phase='group')
            self._call_progress(idx, max(total, 1), f'group {idx}/{total}')

            result = self.run_steps(group_df,
                                    sub_steps,
                                    on_error=on_error,
                                    observer=observer,
                                    return_result=True)
            if collect and isinstance(result.df, pd.DataFrame):
                collected.append(prepend_dict_columns(result.df, group_key))

        if collect and collected:
            return pd.concat(collected, ignore_index=True)
        return df
