# core.py - BatchProcessor with pre/post per-path and accurate progress

from pathlib import Path
from datetime import datetime
import fnmatch
from wcmatch import glob
import traceback
from typing import Dict, List, Tuple, Any, Optional
from decorators.processor import ProcessingContext, PROCESSORS, PRE_PROCESSORS, POST_PROCESSORS
import copy

try:
    from qtpy.QtCore import Qt
except Exception:
    Qt = None


class BatchProcessor:

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.root_path: Optional[Path] = None
        self.progress_callback = None
        self.worker = None
        # 默认使用全局注册表
        self._pre_processors = PRE_PROCESSORS
        self._processors = PROCESSORS
        self._post_processors = POST_PROCESSORS
        # current execution status (most recent status string)
        self.current_status: Optional[str] = None
        # default status log file (can be overridden via `set_status_log`)
        self.status_log_path: Path = Path.cwd() / 'debug_logs' / 'status.log'
        # Whether to deepcopy per-processor config before invoking processors.
        # Default True to avoid accidental mutations of shared config objects
        # by processor implementations.
        self.deepcopy_processor_config: bool = True

    def set_config(self, config: Dict):
        self.config = config

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def _call_progress(self, current: int, total: int, status: str):
        # update in-memory status
        try:
            self.current_status = status
        except Exception:
            pass

        # emit to any UI / external callback
        if self.progress_callback:
            try:
                self.progress_callback(current, total, status)
            except Exception:
                # ensure progress logging doesn't interrupt processing
                pass

        # persist a short status line to the status log for external monitoring
        try:
            log_path = self.status_log_path
            log_dir = log_path.parent
            if not log_dir.exists():
                log_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().isoformat(sep=' ', timespec='seconds')
            with open(log_path, 'a', encoding='utf-8') as fh:
                fh.write(f"{ts} | {current}/{total} | {status}\n")
        except Exception:
            # non-fatal: don't raise from logging failures
            pass

    def set_worker(self, worker):
        self.worker = worker

    def set_status_log(self, path: str | Path):
        """Override the status log file path. Path may be a directory or file.

        Examples:
          - `processor.set_status_log('C:/temp/status.log')`
          - `processor.set_status_log('debug_logs/status.log')`
        """
        p = Path(path)
        # if a directory given, use `status.log` inside it
        if p.exists() and p.is_dir():
            p = p / 'status.log'
        # ensure parent exists (created on first write as well)
        self.status_log_path = p

    def set_isolate_processor_inputs(self, enabled: bool):
        """Enable or disable deepcopying processor `config` when calling.

        When True (default), the engine will pass a deep-copied `config`
        dict to each processor invocation to prevent processors from
        mutating shared configuration objects. Set to False for performance
        when you are certain processors will not modify their input config.
        """
        self.deepcopy_processor_config = bool(enabled)

    def get_current_status(self) -> Optional[str]:
        return self.current_status

    def _is_cancelled(self) -> bool:
        if self.worker and self.worker.thread():
            return self.worker.thread().isInterruptionRequested()
        return False

    def set_processors(self, pre=None, main=None, post=None):
        """允许外部覆盖处理器集合"""
        if pre is not None:
            self._pre_processors = pre
        if main is not None:
            self._processors = main
        if post is not None:
            self._post_processors = post

    def _record_pipe_result(self,
                            context: ProcessingContext,
                            proc_name: str,
                            result: Any,
                            phase: str,
                            path: Optional[Path] = None):
        if not isinstance(result, dict):
            return
        context.pipe[proc_name] = result
        context.pipe_log.append({
            'path': str(path) if path is not None else '.',
            'proc_name': proc_name,
            'phase': phase,
            'keys': list(result.keys()),
            'ts': datetime.now().isoformat(sep=' ', timespec='seconds')
        })

    def _record_result(self, context: ProcessingContext, proc_name: str,
                       result: Any):
        if not isinstance(result, dict):
            return
        if 'processor' in result:
            context.add_result(result)
            return
        context.add_result({'processor': proc_name, 'result': result, **result})

    # ==================== PUBLIC API ====================
    def run(self,
            root_path: str | Path,
            context: ProcessingContext = None) -> ProcessingContext:
        context = context or ProcessingContext()
        root = Path(root_path)
        if not root.exists():
            raise FileNotFoundError(f"路径不存在: {root}")

        context.root_path = root
        self.root_path = root
        print(f"🔍 开始处理: {root}")

        # 获取全局钩子
        global_pre_name, config_pre = self._get_pre_config()
        global_post_name, config_post = self._get_post_config()

        # 精确统计总步数
        total_processor_calls = self._count_total_processor_calls(root)
        total_steps = ((1 if global_pre_name else 0) + total_processor_calls +
                       (1 if global_post_name else 0))
        print(f"📊 总操作数: {total_steps}")

        current_step = 0

        # === 全局 pre_process ===
        if global_pre_name:
            current_step += 1
            self._call_progress(current_step, total_steps,
                                f"🚀 全局初始化: {global_pre_name}")
            print(f'🚀 执行全局初始化...（{global_pre_name}）')
            if self._is_cancelled():
                return context
            try:
                if global_pre_name in self._pre_processors:
                    # Call the global pre-processor. Recording of results
                    # moved to optional built-in processors (e.g. record_to_shared)
                    result = self._pre_processors[global_pre_name](
                        context, **config_pre)
                    self._record_result(context, global_pre_name, result)
                    self._record_pipe_result(context,
                                             global_pre_name,
                                             result,
                                             phase='global-pre',
                                             path=root)
                    print('✅ 全局初始化完成!')
                else:
                    print(f"⚠️ 未注册的全局初始化函数: {global_pre_name}")
            except Exception as e:
                print(f"❌ 全局初始化失败: {e}\n{traceback.format_exc()}")
                # 不中断，继续处理

        # === 递归处理所有路径 ===
        step_counter = [current_step]  # mutable reference
        self._process_path_recursive(root, context, step_counter, total_steps)

        # === 全局 post_process ===
        if not self._is_cancelled() and global_post_name:
            step_counter[0] += 1
            self._call_progress(step_counter[0], total_steps,
                                f"🏁 全局收尾: {global_post_name}")
            print(f"🏁 执行全局最终处理: {global_post_name}")
            try:
                if global_post_name in self._post_processors:
                    # Call the global post-processor. Post-run recording
                    # should be performed by configured post-processors.
                    result = self._post_processors[global_post_name](
                        context, **config_post)
                    self._record_result(context, global_post_name, result)
                    self._record_pipe_result(context,
                                             global_post_name,
                                             result,
                                             phase='global-post',
                                             path=root)
            except Exception as e:
                print(f"❌ 全局最终处理失败: {e}\n{traceback.format_exc()}")

        return context

    def simulate(self,
                 root_path: str | Path,
                 max_items: int | None = None,
                 pattern_filter: str | None = None,
                 sequence: bool = False) -> Any:
        """Produce a dry-run plan for the given root_path.

        Returns a list of action dicts describing which processors would
        run for each file/dir. This does NOT execute any processors.

        Parameters:
          - root_path: path to simulate
          - max_items: optional cap on number of entries returned
          - pattern_filter: optional substring to filter returned paths
        """
        root = Path(root_path)
        if not root.exists():
            raise FileNotFoundError(f"路径不存在: {root}")

        self.root_path = root

        actions: List[Dict] = []

        # If sequence mode requested, we'll build a linear step list
        if sequence:
            steps: List[Dict] = []
            step_counter = 0
            # include global pre/post config names
            global_pre_name, config_pre = self._get_pre_config()
            global_post_name, config_post = self._get_post_config()

        def _walk(p: Path):
            nonlocal actions, step_counter, steps
            is_dir = p.is_dir()
            rules = self._get_processors_for_path(p, is_dir)

            # build action record
            rel = None
            try:
                rel = p.relative_to(self.root_path).as_posix()
            except Exception:
                rel = str(p)

            action = {
                "path":
                rel if rel != "" else ".",
                "is_dir":
                is_dir,
                "pre_processors": [{
                    "name": n,
                    "config": c
                } for n, c in rules.get("pre", [])],
                "processors": [{
                    "name": n,
                    "config": c
                } for n, c in rules.get("inline", [])],
                "post_processors": [{
                    "name": n,
                    "config": c
                } for n, c in rules.get("post", [])],
            }

            # optional filter
            passed_filter = True
            if pattern_filter:
                passed_filter = (pattern_filter in action["path"])

            if sequence and passed_filter:
                # append pre processors
                for proc_name, cfg in rules.get('pre', []):
                    step_counter += 1
                    steps.append({
                        'step': step_counter,
                        'phase': 'pre',
                        'path': action['path'],
                        'is_dir': is_dir,
                        'proc_name': proc_name,
                        'config': cfg,
                    })
                # append inline processors
                for proc_name, cfg in rules.get('inline', []):
                    step_counter += 1
                    steps.append({
                        'step': step_counter,
                        'phase': 'inline',
                        'path': action['path'],
                        'is_dir': is_dir,
                        'proc_name': proc_name,
                        'config': cfg,
                    })

            if not sequence:
                if passed_filter:
                    actions.append(action)

            # early stop
            if max_items is not None and len(actions) >= max_items:
                return

            if is_dir:
                try:
                    for child in sorted(p.iterdir()):
                        if max_items is not None and len(actions) >= max_items:
                            return
                        _walk(child)
                except (PermissionError, OSError):
                    pass

            if sequence and passed_filter:
                # after children, append post processors for this path
                for proc_name, cfg in rules.get('post', []):
                    step_counter += 1
                    steps.append({
                        'step': step_counter,
                        'phase': 'post',
                        'path': action['path'],
                        'is_dir': is_dir,
                        'proc_name': proc_name,
                        'config': cfg,
                    })

        # if sequence mode, include global pre, walk, then global post
        if sequence:
            if global_pre_name:
                step_counter += 1
                steps.append({
                    'step': step_counter,
                    'phase': 'global-pre',
                    'path': '.',
                    'is_dir': True,
                    'proc_name': global_pre_name,
                    'config': config_pre,
                })

            _walk(root)

            if global_post_name:
                step_counter += 1
                steps.append({
                    'step': step_counter,
                    'phase': 'global-post',
                    'path': '.',
                    'is_dir': True,
                    'proc_name': global_post_name,
                    'config': config_post,
                })

            return {'total_steps': step_counter, 'steps': steps}

        else:
            _walk(root)
            return actions

    # ==================== PRIVATE HELPERS ====================

    def _count_total_processor_calls(self, root: Path) -> int:
        """遍历整棵树，统计所有 pre + post 处理器调用次数"""
        total = 0

        def _walk(p: Path):
            nonlocal total
            is_dir = p.is_dir()
            rules = self._get_processors_for_path(p, is_dir)
            total += len(rules.get("pre", [])) + len(rules.get(
                "inline", [])) + len(rules.get("post", []))
            if is_dir:
                try:
                    for child in sorted(p.iterdir()):
                        _walk(child)
                except (PermissionError, OSError):
                    pass  # skip inaccessible dirs

        _walk(root)
        return total

    def _process_path_recursive(self, path: Path, context: ProcessingContext,
                                step_counter: List[int],
                                total_steps: int) -> None:
        is_dir = path.is_dir()
        rules = self._get_processors_for_path(path, is_dir)
        pre_procs = rules.get("pre", []) + rules.get("inline", [])
        post_procs = rules.get("post", [])

        # Pre-visit
        if pre_procs:
            self._execute_processor_list_with_progress(pre_procs, path,
                                                       context, is_dir, "pre",
                                                       step_counter,
                                                       total_steps)

        # Recurse into children (if dir)
        if is_dir:
            try:
                children = sorted(path.iterdir())
            except (PermissionError, OSError):
                children = []
            for child in children:
                if self._is_cancelled():
                    return
                self._process_path_recursive(child, context, step_counter,
                                             total_steps)

        # Post-visit
        if post_procs:
            self._execute_processor_list_with_progress(post_procs, path,
                                                       context, is_dir, "post",
                                                       step_counter,
                                                       total_steps)

    def _get_processors_for_path(
            self, path: Path,
            is_dir: bool) -> Dict[str, List[Tuple[str, Dict]]]:

        # 收集所有候选规则（带优先级）
        candidates = {"pre": [], "post": [], "inline": []}

        for pattern, rule in self.config.items():
            if pattern in ("pre_process", "post_process", "config_pre",
                           "config_post"):
                continue
            if not isinstance(rule, dict):
                continue

            if self._match_rule(path, pattern, is_dir):
                config = rule.get("config", {})
                priority = rule.get("priority", 0)

                #      must_execute = rule.get("must_execute", False)must_execute

                def add_to_list(lst, procs):
                    for p in procs:
                        lst.append((p, config, priority))

                if "processors" in rule:
                    add_to_list(candidates["inline"], rule["processors"])
                if "pre_processors" in rule:
                    add_to_list(candidates["pre"], rule["pre_processors"])
                if "post_processors" in rule:
                    add_to_list(candidates["post"], rule["post_processors"])

        # 对每类处理器按优先级排序，返回最终列表（不去重）
        result = {}  # phase -> list of (name, config)
        for phase in ["pre", "inline", "post"]:
            procs = candidates[phase]
            if not procs:
                result[phase] = []
                continue

            sorted_procs = sorted(candidates[phase], key=lambda x: -x[2])
            result[phase] = [(name, cfg) for name, cfg, _ in sorted_procs]

        # Optionally inject built-in recorders when enabled in top-level config
        try:
            if self.config.get('enable_builtin_recorders'):
                br = self.config.get('builtin_recorders', {}) or {}
                rec_name = br.get('record', 'record_to_shared')
                persist_name = br.get('persist', 'persist_history_sqlite')

                # inline recorder (per-file/per-path quick record)
                if rec_name and rec_name in self._processors:
                    names = [n for n, _ in result.get('inline', [])]
                    if rec_name not in names:
                        result.setdefault('inline', []).append((rec_name, {}))

                # post-run persistence
                if persist_name and persist_name in self._processors:
                    names = [n for n, _ in result.get('post', [])]
                    if persist_name not in names:
                        result.setdefault('post', []).append(
                            (persist_name, {}))
        except Exception:
            # non-fatal: misconfiguration should not break rule matching
            pass

        return result

    def _match_rule(self, path: Path, pattern: str, is_dir: bool) -> bool:
        try:
            rel_path = path.relative_to(self.root_path).as_posix()
        except ValueError:
            return False

        if pattern == ".":
            return str(path) == str(self.root_path)

        # === 模式以 / 结尾 → 匹配目录本身（支持 *, ?, **, [...]）===
        if pattern.endswith('/'):
            if not is_dir:
                return False
            pattern_base = pattern.rstrip('/')
            # 允许 ** 出现在目录匹配中！
            return glob.globmatch(
                rel_path,
                pattern_base,
                flags=glob.GLOBSTAR  #
            )

        # === 普通模式 → 匹配文件或目录（也支持 **）===
        else:
            return glob.globmatch(rel_path, pattern, flags=glob.GLOBSTAR)

    def _execute_processor_list_with_progress(
            self, procs: List[Tuple[str, Dict]], path: Path,
            context: ProcessingContext, is_dir: bool, phase: str,
            step_counter: List[int], total_steps: int):
        rel_path = path.relative_to(
            self.root_path) if path != self.root_path else Path(".")
        parts = list(rel_path.parts) if rel_path != Path(".") else ["."]
        parts_key = [p + '/' for p in parts[:-1]] + [parts[-1]]

        metadata_info = [[], [], [], None, [], []]
        context.set_metadata(parts_key, metadata_info)

        for proc_name, config in procs:
            if self._is_cancelled():
                break

            step_counter[0] += 1
            step_idx = step_counter[0]
            item_type = "📁目录" if is_dir else "📄文件"
            status = f"{item_type} {path.name} → {proc_name} ({phase})"
            # emit per-step started event if worker provided
            try:
                if hasattr(self, 'worker') and getattr(
                        self, 'worker') is not None and hasattr(
                            self.worker, 'step_started'):
                    try:
                        self.worker.step_started.emit(step_idx)
                    except Exception:
                        pass
            except Exception:
                pass

            self._call_progress(step_idx, total_steps, status)
            print(status)

            metadata_info[0].append(proc_name)
            # snapshot config for metadata and call to avoid accidental
            # mutations by processors. Use deepcopy when enabled.
            if self.deepcopy_processor_config:
                cfg_snapshot = copy.deepcopy(config)
            else:
                cfg_snapshot = config
            metadata_info[1].append(cfg_snapshot)

            if proc_name in self._processors:
                try:
                    # call processor with isolated config if enabled
                    cfg_for_call = cfg_snapshot
                    result = self._processors[proc_name](path, context,
                                                         **(cfg_for_call
                                                            or {}))
                    self._record_result(context, proc_name, result)
                    self._record_pipe_result(context,
                                             proc_name,
                                             result,
                                             phase=phase,
                                             path=path)
                    metadata_info[2].append('succeed')
                    # emit per-step finished(success)
                    try:
                        if hasattr(self, 'worker') and getattr(
                                self, 'worker') is not None and hasattr(
                                    self.worker, 'step_finished'):
                            try:
                                self.worker.step_finished.emit(
                                    step_idx, True, '')
                            except Exception:
                                pass
                    except Exception:
                        pass
                except Exception as e:
                    error_msg = f"{proc_name}: {e}"
                    print(
                        f"❌ 处理失败 [{proc_name} on {path}]: {e}\n{traceback.format_exc()}"
                    )
                    metadata_info[2].append('failed')
                    metadata_info[4].append(error_msg)
                    # emit per-step finished(failed)
                    try:
                        if hasattr(self, 'worker') and getattr(
                                self, 'worker') is not None and hasattr(
                                    self.worker, 'step_finished'):
                            try:
                                self.worker.step_finished.emit(
                                    step_idx, False, error_msg)
                            except Exception:
                                pass
                    except Exception:
                        pass

            else:
                warn_msg = f"{proc_name}: 未注册处理器"
                print(f"⚠️ {warn_msg}")
                metadata_info[2].append('failed')
                metadata_info[4].append(warn_msg)
                # emit per-step finished(failed)
                try:
                    if hasattr(self, 'worker') and getattr(
                            self, 'worker') is not None and hasattr(
                                self.worker, 'step_finished'):
                        try:
                            self.worker.step_finished.emit(
                                step_idx, False, warn_msg)
                        except Exception:
                            pass
                except Exception:
                    pass

    def _get_pre_config(self):
        func_name = self.config.get('pre_process')
        config = self.config.get('config_pre', {})
        return func_name, config

    def _get_post_config(self):
        func_name = self.config.get('post_process')
        config = self.config.get('config_post', {})
        return func_name, config


# 示例：获取当前启用的处理器

    def get_enabled_processors(self):
        enabled = {}
        for row in range(self.plugin_table.rowCount()):
            cb = self.plugin_table.item(row, 1)
            if cb.checkState() == Qt.Checked and hasattr(cb, 'plugin_func'):
                func = cb.plugin_func
                enabled[func.processor_name] = func
        return enabled
