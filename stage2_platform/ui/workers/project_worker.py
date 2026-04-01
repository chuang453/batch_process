from __future__ import annotations

from qtpy.QtCore import QObject, Signal, Slot

from stage2_platform.api import Stage2Service


class ProjectWorker(QObject):
    run_started = Signal(str)
    series_started = Signal(str)
    step_progress = Signal(str, str, str)
    series_finished = Signal(str, str, str)
    run_finished = Signal(object)
    log = Signal(str)

    def __init__(self, service: Stage2Service):
        super().__init__()
        self.service = service

    @Slot()
    def run(self):
        try:
            def observer(kind, payload):
                if kind == 'run_started':
                    self.run_started.emit(str(payload.get('run_id', '')))
                elif kind == 'series_started':
                    self.series_started.emit(str(payload.get('series_name', '')))
                elif kind == 'step_event':
                    self.step_progress.emit(str(payload.get('series_name', '')),
                                            str(payload.get('step_id', '')),
                                            str(payload.get('kind', '')))
                elif kind == 'series_finished':
                    manifest = payload.get('manifest')
                    self.series_finished.emit(str(getattr(manifest, 'series_name', '')),
                                              str(getattr(manifest, 'status', '')),
                                              str(getattr(manifest, 'error', '')))
                elif kind == 'run_finished':
                    self.run_finished.emit(payload.get('manifest'))

            self.service.runner.run(self.service.project, self.service.context, observer=observer)
        except Exception as exc:
            self.log.emit(str(exc))
