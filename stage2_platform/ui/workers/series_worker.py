from __future__ import annotations

from qtpy.QtCore import QObject, Signal, Slot

from stage2_platform.api import Stage2Service


class SeriesWorker(QObject):
    series_started = Signal(str)
    step_progress = Signal(str, str)
    series_finished = Signal(str, str)
    log = Signal(str)

    def __init__(self, service: Stage2Service, stage_name: str, series_name: str):
        super().__init__()
        self.service = service
        self.stage_name = stage_name
        self.series_name = series_name

    @Slot()
    def run(self):
        try:
            self.series_started.emit(self.series_name)
            record = self.service.run_series(self.stage_name, self.series_name)
            self.series_finished.emit(record.series_name, record.status)
        except Exception as exc:
            self.log.emit(str(exc))
