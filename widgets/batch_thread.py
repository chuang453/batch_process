##

# worker.py
from qtpy.QtCore import QObject, Signal, Slot


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
