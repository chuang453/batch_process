from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path

import yaml
from qtpy.QtCore import QThread, Qt
from qtpy.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from stage2_platform.api import Stage2Service
from stage2_platform.ui.panels import DesignerPanel, InputsPanel, MonitorPanel, OutputsPanel
from stage2_platform.ui.project_editor import clone_project, serialize_project_for_disk
from stage2_platform.ui.workers import ProjectWorker


def _jsonable(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


class Stage2WorkspaceWindow(QWidget):

    def __init__(self, initial_project: dict | None = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Stage 2 Workspace")
        self.resize(1380, 900)
        self.service = Stage2Service()
        self._project_thread = None
        self._project_worker = None
        self._loaded_path = None
        self._build_ui()
        self._new_project(initial_project or None)

    def _build_ui(self):
        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        btn_new = QPushButton("New")
        btn_new.clicked.connect(lambda: self._new_project())
        toolbar.addWidget(btn_new)

        btn_load = QPushButton("Load")
        btn_load.clicked.connect(self._load_project)
        toolbar.addWidget(btn_load)

        btn_save = QPushButton("Save")
        btn_save.clicked.connect(self._save_project)
        toolbar.addWidget(btn_save)

        btn_save_as = QPushButton("Save As")
        btn_save_as.clicked.connect(lambda: self._save_project(save_as=True))
        toolbar.addWidget(btn_save_as)

        btn_apply = QPushButton("Apply Edits")
        btn_apply.clicked.connect(self._apply_editor_to_service)
        toolbar.addWidget(btn_apply)

        btn_validate = QPushButton("Validate")
        btn_validate.clicked.connect(self._validate_project)
        toolbar.addWidget(btn_validate)

        btn_simulate = QPushButton("Simulate")
        btn_simulate.clicked.connect(self._simulate_project)
        toolbar.addWidget(btn_simulate)

        btn_run = QPushButton("Run Project")
        btn_run.clicked.connect(self._run_project)
        toolbar.addWidget(btn_run)

        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.service.cancel_run)
        toolbar.addWidget(btn_cancel)

        toolbar.addWidget(QLabel("Project Name"))
        self.project_name_edit = QLineEdit()
        self.project_name_edit.setPlaceholderText("stage2_project")
        self.project_name_edit.setMinimumWidth(220)
        toolbar.addWidget(self.project_name_edit)

        self.path_label = QLabel("No project loaded")
        toolbar.addWidget(self.path_label)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self.inputs_panel = InputsPanel()
        self.designer_panel = DesignerPanel()
        self.monitor_panel = MonitorPanel()
        self.outputs_panel = OutputsPanel()
        self.simulate_text = QTextEdit()
        self.simulate_text.setReadOnly(True)
        self.simulate_text.setPlaceholderText("Simulation and manifest output")
        self.manifest_text = QTextEdit()
        self.manifest_text.setReadOnly(True)
        self.manifest_text.setPlaceholderText("Run manifest output")

        tabs = QTabWidget()
        tabs.addTab(self._build_project_tab(), "Project")
        tabs.addTab(self._build_run_tab(), "Run")
        tabs.addTab(self._build_outputs_tab(), "Outputs")
        layout.addWidget(tabs)

    def _build_project_tab(self) -> QWidget:
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        splitter = QSplitter()
        splitter.addWidget(self.inputs_panel)
        splitter.addWidget(self.designer_panel)
        splitter.setSizes([520, 820])
        tab_layout.addWidget(splitter)
        return tab

    def _build_run_tab(self) -> QWidget:
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        splitter = QSplitter()
        splitter.setOrientation(Qt.Vertical)
        splitter.addWidget(self.monitor_panel)
        splitter.addWidget(self.simulate_text)
        splitter.setSizes([600, 700])
        tab_layout.addWidget(splitter)
        return tab

    def _build_outputs_tab(self) -> QWidget:
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        tab_layout.addWidget(self.outputs_panel)
        tab_layout.addWidget(self.manifest_text)
        return tab

    def _new_project(self, project: dict | None = None):
        self._loaded_path = None
        base = clone_project(project)
        self.service.set_project(base)
        self.service.context = self.service._prepare_context(self.service.project)
        self.monitor_panel.clear()
        self._refresh_from_service()
        self.path_label.setText("Unsaved project")

    def _refresh_from_service(self):
        project = self.service.project or {}
        self.project_name_edit.setText(str(project.get("name", "stage2_project")))
        self.inputs_panel.set_project(project)
        self.designer_panel.set_project(project)
        self.outputs_panel.set_catalog(dict(self.service.context.catalog.items()))
        self.manifest_text.clear()
        status = self.service.get_run_status()
        self.monitor_panel.set_records(status.series_records if status else [])
        if status is not None:
            self.manifest_text.setPlainText(json.dumps(_jsonable(status), ensure_ascii=False, indent=2))
        if self._loaded_path:
            self.path_label.setText(str(self._loaded_path))

    def _apply_editor_to_service(self):
        project = {
            "name": self.project_name_edit.text().strip() or "stage2_project",
            "inputs": self.inputs_panel.get_inputs(),
            "stages": self.designer_panel.get_stages(),
        }
        self.service.set_project(project)
        self.service.context = self.service._prepare_context(self.service.project)
        self.outputs_panel.set_catalog(dict(self.service.context.catalog.items()))
        self.monitor_panel.append_log("Applied project edits to service context.")
        return self.service.project

    def _load_project(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load Stage2 Project", "", "Project Files (*.yaml *.yml *.json)")
        if not path:
            return
        try:
            self._loaded_path = Path(path)
            self.service.load_project(path)
            self.service.context = self.service._prepare_context(self.service.project)
            self.monitor_panel.clear()
            self._refresh_from_service()
        except Exception as exc:
            QMessageBox.critical(self, "Load failed", str(exc))

    def _save_project(self, save_as: bool = False):
        try:
            project = self._apply_editor_to_service()
            path = self._loaded_path
            if save_as or path is None:
                selected, _ = QFileDialog.getSaveFileName(
                    self,
                    "Save Stage2 Project",
                    str(path or Path.cwd() / "stage2_project.yaml"),
                    "Project Files (*.yaml *.yml *.json)",
                )
                if not selected:
                    return
                path = Path(selected)

            serializable = serialize_project_for_disk(project)
            if path.suffix.lower() == ".json":
                text = json.dumps(serializable, ensure_ascii=False, indent=2)
            else:
                text = yaml.safe_dump(serializable, sort_keys=False, allow_unicode=True)
            path.write_text(text, encoding="utf-8")
            self._loaded_path = path
            self.path_label.setText(str(path))
            self.monitor_panel.append_log(f"Saved project: {path}")
        except Exception as exc:
            QMessageBox.critical(self, "Save failed", str(exc))

    def _validate_project(self):
        try:
            self._apply_editor_to_service()
            errors = self.service.validate_project()
            if errors:
                QMessageBox.warning(self, "Validation", "\n".join(errors))
            else:
                QMessageBox.information(self, "Validation", "Project is valid.")
        except Exception as exc:
            QMessageBox.critical(self, "Validation failed", str(exc))

    def _simulate_project(self):
        try:
            self._apply_editor_to_service()
            sim = self.service.simulate()
            self.simulate_text.setPlainText(json.dumps(_jsonable(sim), ensure_ascii=False, indent=2))
        except Exception as exc:
            QMessageBox.critical(self, "Simulate failed", str(exc))

    def _run_project(self):
        if self._project_thread is not None and self._project_thread.isRunning():
            QMessageBox.warning(self, "Run", "Project is already running.")
            return
        try:
            self._apply_editor_to_service()
        except Exception as exc:
            QMessageBox.critical(self, "Run", str(exc))
            return

        if not self.service.project:
            QMessageBox.warning(self, "Run", "Load or set a project first.")
            return

        self.monitor_panel.clear()
        self._project_worker = ProjectWorker(self.service)
        self._project_thread = QThread()
        self._project_worker.moveToThread(self._project_thread)
        self._project_thread.started.connect(self._project_worker.run)
        self._project_worker.run_started.connect(lambda run_id: self.monitor_panel.append_log(f"Run started: {run_id}"))
        self._project_worker.series_started.connect(lambda name: self.monitor_panel.append_log(f"Series started: {name}"))
        self._project_worker.step_progress.connect(
            lambda series, step, kind: self.monitor_panel.append_log(f"{series} {step} {kind}")
        )
        self._project_worker.series_finished.connect(
            lambda name, status, error: self.monitor_panel.append_log(f"Series finished: {name} [{status}] {error}")
        )
        self._project_worker.log.connect(self.monitor_panel.append_log)
        self._project_worker.run_finished.connect(self._on_run_finished)
        self._project_worker.run_finished.connect(self._project_thread.quit)
        self._project_worker.run_finished.connect(self._project_worker.deleteLater)
        self._project_thread.finished.connect(self._project_thread.deleteLater)
        self._project_thread.start()

    def _on_run_finished(self, manifest):
        self.monitor_panel.set_records(getattr(manifest, "series_records", []))
        self.outputs_panel.set_catalog(dict(self.service.context.catalog.items()))
        manifest_json = json.dumps(_jsonable(manifest), ensure_ascii=False, indent=2)
        self.simulate_text.setPlainText(manifest_json)
        self.manifest_text.setPlainText(manifest_json)
        self.monitor_panel.append_log("Run finished.")
        self._project_worker = None
        self._project_thread = None