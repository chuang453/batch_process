from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from decorators.processor import ProcessingContext, TRANSFORMS
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

    def run_steps(self, df: Any, steps: List[dict]) -> Any:
        out = df
        total = len(steps or [])
        for idx, step in enumerate(steps or [], start=1):
            step_name = 'group_by' if 'group_by' in step else 'transform' if 'run' in step else 'builtin_op'
            try:
                if self.worker is not None and hasattr(self.worker, 'step_started'):
                    try:
                        self.worker.step_started.emit(idx)
                    except Exception:
                        pass

                if 'group_by' in step:
                    out = self._run_group_by(out, step)
                elif 'run' in step:
                    cfg = step.get('config', {}) or {}
                    out = self._run_transform_chain(out, step.get('run', []), cfg)
                else:
                    out = self._run_builtin_op(out, step)

                self._call_progress(idx, total, f"data step {idx}/{total}: {step_name}")
                if self.worker is not None and hasattr(self.worker, 'step_finished'):
                    try:
                        self.worker.step_finished.emit(idx, True, '')
                    except Exception:
                        pass
            except Exception as e:
                self.context.add_result({
                    'processor': step_name,
                    'status': 'failed',
                    'error': str(e),
                })
                if self.worker is not None and hasattr(self.worker, 'step_finished'):
                    try:
                        self.worker.step_finished.emit(idx, False, str(e))
                    except Exception:
                        pass
        return out

    def simulate_steps(self, df: Any, steps: List[dict]) -> List[dict]:
        records: List[dict] = []
        for idx, step in enumerate(steps or [], start=1):
            step_type = 'builtin_op'
            if 'group_by' in step:
                step_type = 'group_by'
            elif 'run' in step:
                step_type = 'transform'
            records.append({'step': idx, 'step_type': step_type, 'detail': step})
        return records

    def _run_builtin_op(self, df: Any, step: Dict[str, Any]) -> Any:
        if not isinstance(df, pd.DataFrame):
            self.context.add_result({
                'processor': 'builtin_op',
                'status': 'skipped',
                'reason': 'input is not DataFrame',
                'detail': step,
            })
            return df

        try:
            if 'rename' in step:
                return df.rename(columns=step['rename'])
            if 'dropna' in step:
                params = step['dropna']
                if isinstance(params, dict):
                    return df.dropna(**params)
                return df.dropna()
            if 'filter' in step:
                return df.query(step['filter'])
            if 'select' in step:
                return df[list(step['select'])]
            if 'sort' in step:
                params = step['sort']
                if isinstance(params, dict):
                    return df.sort_values(**params)
                return df.sort_values(by=params)
            if 'fillna' in step:
                return df.fillna(step['fillna'])
            if 'eval' in step:
                return df.eval(step['eval'])
            if 'astype' in step:
                return df.astype(step['astype'])
            if 'drop' in step:
                cols = step['drop']
                if isinstance(cols, dict):
                    return df.drop(**cols)
                return df.drop(columns=list(cols))
            if 'head' in step:
                return df.head(int(step['head']))
            if 'tail' in step:
                return df.tail(int(step['tail']))

            self.context.add_result({
                'processor': 'builtin_op',
                'status': 'skipped',
                'reason': 'unknown op',
                'detail': step,
            })
            return df
        except Exception as e:
            self.context.add_result({
                'processor': 'builtin_op',
                'status': 'failed',
                'error': str(e),
                'detail': step,
            })
            return df

    def _run_transform_chain(self, df: Any, func_names: List[str], config: Dict[str, Any]) -> Any:
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
        return out

    def _normalize_group_cols(self, cols) -> List[str]:
        if isinstance(cols, str):
            return [cols]
        return list(cols or [])

    def _run_group_by(self, df: Any, step: Dict[str, Any]) -> Any:
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

        loop_cols_stack = self.context.setdefault_metadata(['runtime_info', 'loop_cols'], [])
        loop_vars_stack = self.context.setdefault_metadata(['runtime_info', 'loop_vars'], [])
        loop_cols_stack.append(group_cols)
        loop_vars_stack.append(None)

        collected: List[pd.DataFrame] = []
        try:
            total = len(groups)
            for idx, (group_key, group_df) in enumerate(groups, start=1):
                if self.worker and hasattr(self.worker, 'thread'):
                    thr = self.worker.thread()
                    if thr and thr.isInterruptionRequested():
                        break

                loop_vars_stack[-1] = group_key
                self._record_pipe('_group', {
                    'key': group_key,
                    'rows': int(len(group_df)) if hasattr(group_df, '__len__') else None,
                    'cols': list(group_df.columns) if isinstance(group_df, pd.DataFrame) else None,
                }, phase='group')

                result_df = self.run_steps(group_df, sub_steps)
                if collect and isinstance(result_df, pd.DataFrame):
                    collected.append(prepend_dict_columns(result_df, group_key))
        finally:
            loop_cols_stack.pop()
            loop_vars_stack.pop()

        if collect and collected:
            return pd.concat(collected, ignore_index=True)
        return df
