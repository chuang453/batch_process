##

# worker.py
from qtpy.QtCore import QObject, Signal, Slot
import pandas as pd


class WriteStream:

    def __init__(self, write_func, max_buffer=4096):
        self.write_func = write_func
        self._buf = ""
        self.max_buffer = max_buffer

    def write(self, text):
        if not text:
            return
        # accumulate and emit on newline or when buffer grows too large
        self._buf += text
        while True:
            if "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                if line.strip():
                    try:
                        self.write_func(line)
                    except Exception:
                        pass
                # continue loop to handle multiple newlines
                continue
            # no newline: flush if buffer too large
            if len(self._buf) >= self.max_buffer:
                chunk = self._buf
                self._buf = ""
                if chunk.strip():
                    try:
                        self.write_func(chunk)
                    except Exception:
                        pass
            break

    def flush(self):
        if self._buf and self._buf.strip():
            try:
                self.write_func(self._buf)
            except Exception:
                pass
        self._buf = ""


class BatchWorker(QObject):
    finished = Signal(object)  # emit context
    log = Signal(str)  # emit log message
    progress = Signal(int, int, str)  # current, total, status
    # per-step signals: step index (int) started, and finished with success flag and message
    step_started = Signal(int)
    step_finished = Signal(int, bool, str)

    def __init__(self, processor, root_path, context):
        super().__init__()
        self.processor = processor
        self.root_path = root_path
        self.context = context

    @Slot()  # 明确标记为槽函数
    def run(self):
        try:
            import sys
            old_stdout = sys.stdout
            sys.stdout = WriteStream(lambda s: self.log.emit(s))

            def progress_callback(current, total, status="处理中"):
                self.progress.emit(current, total, status)

        # ✅ 将当前线程传给 processor，用于检查中断

            self.processor.set_worker(self)
            self.processor.set_progress_callback(progress_callback)

            # 执行批处理
            self.processor.run(self.root_path, self.context)

            self.finished.emit(self.context)

        except Exception as e:
            if self.thread().isInterruptionRequested():
                self.log.emit("🛑 批处理已被用户取消")
            else:
                self.log.emit(f"❌ 执行失败: {e}")
            self.finished.emit(self.context)
        finally:
            sys.stdout = sys.__stdout__


class DataStageWorker(QObject):
    finished = Signal(object, bool, str)  # out_df, success, message
    log = Signal(str)
    progress = Signal(int, int, str)
    step_started = Signal(int)
    step_finished = Signal(int, bool, str)

    def __init__(self, context, input_df, steps):
        super().__init__()
        self.context = context
        self.input_df = input_df
        self.steps = steps or []

    @Slot()
    def run(self):
        try:
            from core.data_stage import DataStage

            ds = DataStage(self.context)
            ds.set_worker(self)
            ds.set_progress_callback(
                lambda current, total, status='处理中': self.progress.emit(
                    int(current), int(total), str(status)))

            out_df = ds.run_steps(self.input_df, self.steps)
            self.finished.emit(out_df, True, '')
        except Exception as e:
            self.log.emit(f"❌ Stage2执行失败: {e}")
            self.finished.emit(None, False, str(e))


class Stage1ProfileWorker(QObject):
    finished = Signal(int, object, object, str)  # request_id, cache_key, result, error
    progress = Signal(int, int, str)
    log = Signal(str)

    def __init__(self, request_id, key, df):
        super().__init__()
        self.request_id = int(request_id)
        self.key = str(key)
        self.df = df

    def _check_cancelled(self):
        thread = self.thread()
        return thread is not None and thread.isInterruptionRequested()

    @Slot()
    def run(self):
        try:
            df = self.df
            cache_key = (self.key, id(df), df.shape)
            profile_rows = len(df)
            profile_cols = len(df.columns)
            max_profile_rows = 2000
            use_sample_profile = profile_rows > 20000 or profile_cols > 200
            profile_df = df.head(min(max_profile_rows, profile_rows)) if use_sample_profile else df

            self.progress.emit(1, 3, '生成列信息')
            if self._check_cancelled():
                self.finished.emit(self.request_id, cache_key, None, 'cancelled')
                return

            lines = []
            if use_sample_profile:
                lines.append(
                    f"大数据集优化: 列统计与 describe 基于前 {len(profile_df)} 行样本生成，完整表大小 {df.shape}。")

            col_df = pd.DataFrame({
                'column': df.columns.astype(str),
                'dtype': [str(x) for x in df.dtypes],
                'nulls': [int(profile_df[c].isna().sum()) for c in df.columns],
                'unique': [int(profile_df[c].nunique(dropna=True)) for c in df.columns],
            })
            columns_text = col_df.to_string(index=False, max_rows=100)
            if lines:
                columns_text = '\n'.join(lines + [columns_text])

            self.progress.emit(2, 3, '生成统计摘要')
            if self._check_cancelled():
                self.finished.emit(self.request_id, cache_key, None, 'cancelled')
                return

            try:
                desc = profile_df.describe(include='all', datetime_is_numeric=True)
                describe_text = desc.to_string(max_rows=120)
                if use_sample_profile:
                    describe_text = (
                        f"大数据集优化: describe 基于前 {len(profile_df)} 行样本。\n\n"
                        f"{describe_text}")
            except Exception:
                describe_text = 'describe() 不可用或数据列为空。'

            self.progress.emit(3, 3, '完成')
            if self._check_cancelled():
                self.finished.emit(self.request_id, cache_key, None, 'cancelled')
                return

            result = {
                'columns_text': columns_text,
                'describe_text': describe_text,
            }
            self.finished.emit(self.request_id, cache_key, result, '')
        except Exception as e:
            self.log.emit(f"❌ Stage1摘要生成失败: {e}")
            self.finished.emit(self.request_id, None, None, str(e))
