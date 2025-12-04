##

# worker.py
from qtpy.QtCore import QObject, Signal, Slot


class WriteStream:

    def __init__(self, write_func):
        self.write_func = write_func

    def write(self, text):
        if text.strip():
            self.write_func(text)

    def flush(self):
        pass


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
