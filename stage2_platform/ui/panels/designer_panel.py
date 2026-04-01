from __future__ import annotations

from qtpy.QtWidgets import (
    QCheckBox,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from stage2_platform.ui.project_editor import clone_project, dump_json, parse_list_text


class DesignerPanel(QGroupBox):

    def __init__(self, parent=None):
        super().__init__("Pipeline / Series Designer", parent)
        self._project = {"stages": []}
        self._selected_stage = None
        self._selected_series = None
        self._building = False
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        splitter = QSplitter()

        stage_widget = QWidget()
        stage_layout = QVBoxLayout(stage_widget)
        stage_toolbar = QHBoxLayout()
        btn_add_stage = QPushButton("Add Stage")
        btn_add_stage.clicked.connect(self.add_stage)
        stage_toolbar.addWidget(btn_add_stage)
        btn_remove_stage = QPushButton("Remove Stage")
        btn_remove_stage.clicked.connect(self.remove_selected_stage)
        stage_toolbar.addWidget(btn_remove_stage)
        stage_toolbar.addStretch()
        stage_layout.addLayout(stage_toolbar)

        self.stage_table = QTableWidget(0, 2)
        self.stage_table.setHorizontalHeaderLabels(["Stage", "Source"])
        self.stage_table.verticalHeader().setVisible(False)
        self.stage_table.itemSelectionChanged.connect(self._on_stage_selected)
        stage_layout.addWidget(self.stage_table)
        splitter.addWidget(stage_widget)

        series_widget = QWidget()
        series_layout = QVBoxLayout(series_widget)
        series_toolbar = QHBoxLayout()
        btn_add_series = QPushButton("Add Series")
        btn_add_series.clicked.connect(self.add_series)
        series_toolbar.addWidget(btn_add_series)
        btn_remove_series = QPushButton("Remove Series")
        btn_remove_series.clicked.connect(self.remove_selected_series)
        series_toolbar.addWidget(btn_remove_series)
        series_toolbar.addStretch()
        series_layout.addLayout(series_toolbar)

        self.series_table = QTableWidget(0, 4)
        self.series_table.setHorizontalHeaderLabels(["Series", "Input", "Output", "Enabled"])
        self.series_table.verticalHeader().setVisible(False)
        self.series_table.itemSelectionChanged.connect(self._on_series_selected)
        series_layout.addWidget(self.series_table)

        detail_widget = QWidget()
        detail_layout = QVBoxLayout(detail_widget)
        detail_layout.addWidget(QLabel("Selected Series Steps (JSON/YAML list)"))
        self.steps_edit = QTextEdit()
        self.steps_edit.setPlaceholderText('[{"head": 10}, {"dropna": {"subset": ["a"]}}]')
        detail_layout.addWidget(self.steps_edit)

        option_row = QHBoxLayout()
        self.output_policy_combo = QComboBox()
        self.output_policy_combo.addItems(["overwrite", "append", "error"])
        option_row.addWidget(QLabel("Output Policy"))
        option_row.addWidget(self.output_policy_combo)
        self.enabled_check = QCheckBox("Enabled")
        self.enabled_check.setChecked(True)
        option_row.addWidget(self.enabled_check)
        self.continue_on_error_check = QCheckBox("Continue On Error")
        self.continue_on_error_check.setChecked(True)
        option_row.addWidget(self.continue_on_error_check)
        option_row.addStretch()
        detail_layout.addLayout(option_row)
        series_layout.addWidget(detail_widget)
        splitter.addWidget(series_widget)

        splitter.setSizes([300, 700])
        layout.addWidget(splitter)

    def set_project(self, project: dict | None) -> None:
        self._project = clone_project(project)
        self._project.setdefault("stages", [])
        self._selected_stage = None
        self._selected_series = None
        self._populate_stage_table()

    def get_stages(self) -> list[dict]:
        self._sync_editor_to_project()
        return clone_project({"stages": self._project.get("stages", [])}).get("stages", [])

    def add_stage(self) -> None:
        self._sync_editor_to_project()
        stages = self._project.setdefault("stages", [])
        stage_index = len(stages) + 1
        stages.append({
            "name": f"stage_{stage_index}",
            "type": "data",
            "source": "df",
            "series": [self._default_series(1, "df")],
        })
        self._populate_stage_table(select_stage=len(stages) - 1)

    def remove_selected_stage(self) -> None:
        self._sync_editor_to_project()
        row = self.stage_table.currentRow()
        if row < 0:
            return
        stages = self._project.get("stages", [])
        if row < len(stages):
            del stages[row]
        next_row = min(row, max(len(stages) - 1, 0)) if stages else None
        self._populate_stage_table(select_stage=next_row)

    def add_series(self) -> None:
        self._sync_editor_to_project()
        stage_idx = self.stage_table.currentRow()
        stages = self._project.get("stages", [])
        if stage_idx < 0 or stage_idx >= len(stages):
            return
        stage = stages[stage_idx]
        series = stage.setdefault("series", [])
        series.append(self._default_series(len(series) + 1, stage.get("source", "df")))
        self._populate_series_table(stage_idx, select_series=len(series) - 1)

    def remove_selected_series(self) -> None:
        self._sync_editor_to_project()
        stage_idx = self.stage_table.currentRow()
        series_idx = self.series_table.currentRow()
        stages = self._project.get("stages", [])
        if stage_idx < 0 or stage_idx >= len(stages):
            return
        series = stages[stage_idx].setdefault("series", [])
        if series_idx < 0 or series_idx >= len(series):
            return
        del series[series_idx]
        next_row = min(series_idx, max(len(series) - 1, 0)) if series else None
        self._populate_series_table(stage_idx, select_series=next_row)

    def _populate_stage_table(self, select_stage: int | None = 0) -> None:
        self._building = True
        stages = self._project.get("stages", [])
        self.stage_table.setRowCount(len(stages))
        for row, stage in enumerate(stages):
            self.stage_table.setItem(row, 0, QTableWidgetItem(str(stage.get("name", ""))))
            self.stage_table.setItem(row, 1, QTableWidgetItem(str(stage.get("source", "df"))))
        self._building = False

        if stages and select_stage is not None:
            self.stage_table.selectRow(max(0, min(select_stage, len(stages) - 1)))
        else:
            self.series_table.setRowCount(0)
            self.steps_edit.clear()
            self._selected_stage = None
            self._selected_series = None

    def _populate_series_table(self, stage_idx: int, select_series: int | None = 0) -> None:
        self._building = True
        stages = self._project.get("stages", [])
        if stage_idx < 0 or stage_idx >= len(stages):
            self.series_table.setRowCount(0)
            self.steps_edit.clear()
            self._building = False
            return

        stage = stages[stage_idx]
        series_rows = stage.get("series", []) or []
        self.series_table.setRowCount(len(series_rows))
        for row, item in enumerate(series_rows):
            self.series_table.setItem(row, 0, QTableWidgetItem(str(item.get("name", ""))))
            self.series_table.setItem(row, 1, QTableWidgetItem(str(item.get("input_key", stage.get("source", "df")))))
            self.series_table.setItem(row, 2, QTableWidgetItem(str(item.get("output_key", ""))))
            self.series_table.setItem(row, 3, QTableWidgetItem("yes" if item.get("enabled", True) else "no"))
        self._building = False

        if series_rows and select_series is not None:
            self.series_table.selectRow(max(0, min(select_series, len(series_rows) - 1)))
        else:
            self.steps_edit.clear()
            self._selected_series = None

    def _populate_series_editor(self, stage_idx: int, series_idx: int) -> None:
        stage = self._project.get("stages", [])[stage_idx]
        series = stage.get("series", [])[series_idx]
        self.steps_edit.setPlainText(dump_json(series.get("steps", [])))
        self.enabled_check.setChecked(bool(series.get("enabled", True)))
        self.continue_on_error_check.setChecked(bool(series.get("continue_on_error", True)))
        policy = str(series.get("output_policy", "overwrite"))
        idx = self.output_policy_combo.findText(policy)
        self.output_policy_combo.setCurrentIndex(idx if idx >= 0 else 0)

    def _sync_editor_to_project(self) -> None:
        if self._building:
            return
        self._sync_stage_table_to_project()
        stage_idx = self._selected_stage
        if stage_idx is None:
            return
        self._sync_series_table_to_project(stage_idx)
        series_idx = self._selected_series
        if series_idx is not None:
            self._sync_series_editor_to_project(stage_idx, series_idx)

    def _sync_stage_table_to_project(self) -> None:
        stages = self._project.setdefault("stages", [])
        updated = []
        for row in range(self.stage_table.rowCount()):
            current = dict(stages[row]) if row < len(stages) else {}
            current["name"] = self._table_text(self.stage_table, row, 0) or f"stage_{row + 1}"
            current["type"] = "data"
            current["source"] = self._table_text(self.stage_table, row, 1) or "df"
            current.setdefault("series", [])
            updated.append(current)
        self._project["stages"] = updated

    def _sync_series_table_to_project(self, stage_idx: int) -> None:
        stages = self._project.get("stages", [])
        if stage_idx >= len(stages):
            return
        stage = stages[stage_idx]
        series = stage.setdefault("series", [])
        updated = []
        for row in range(self.series_table.rowCount()):
            current = dict(series[row]) if row < len(series) else self._default_series(row + 1, stage.get("source", "df"))
            current["name"] = self._table_text(self.series_table, row, 0) or f"series_{row + 1}"
            current["input_key"] = self._table_text(self.series_table, row, 1) or stage.get("source", "df")
            current["output_key"] = self._table_text(self.series_table, row, 2)
            enabled_text = self._table_text(self.series_table, row, 3).strip().lower()
            current["enabled"] = enabled_text not in {"false", "0", "no", "off"}
            current.setdefault("steps", [])
            current.setdefault("continue_on_error", True)
            current.setdefault("output_policy", "overwrite")
            updated.append(current)
        stage["series"] = updated

    def _sync_series_editor_to_project(self, stage_idx: int, series_idx: int) -> None:
        stages = self._project.get("stages", [])
        if stage_idx >= len(stages):
            return
        series = stages[stage_idx].setdefault("series", [])
        if series_idx >= len(series):
            return
        current = series[series_idx]
        current["steps"] = parse_list_text(self.steps_edit.toPlainText(), "series.steps")
        current["enabled"] = self.enabled_check.isChecked()
        current["continue_on_error"] = self.continue_on_error_check.isChecked()
        current["output_policy"] = self.output_policy_combo.currentText()

    def _on_stage_selected(self) -> None:
        if self._building:
            return
        self._sync_editor_to_project()
        row = self.stage_table.currentRow()
        self._selected_stage = row if row >= 0 else None
        if self._selected_stage is None:
            self.series_table.setRowCount(0)
            self.steps_edit.clear()
            return
        self._populate_series_table(self._selected_stage, select_series=0)

    def _on_series_selected(self) -> None:
        if self._building:
            return
        if self._selected_stage is not None and self._selected_series is not None:
            self._sync_series_editor_to_project(self._selected_stage, self._selected_series)
        row = self.series_table.currentRow()
        self._selected_series = row if row >= 0 else None
        if self._selected_stage is None or self._selected_series is None:
            self.steps_edit.clear()
            return
        self._populate_series_editor(self._selected_stage, self._selected_series)

    def _table_text(self, table: QTableWidget, row: int, col: int) -> str:
        item = table.item(row, col)
        return item.text() if item is not None else ""

    def _default_series(self, index: int, source_key: str) -> dict:
        return {
            "name": f"series_{index}",
            "input_key": source_key,
            "output_key": "",
            "steps": [],
            "enabled": True,
            "continue_on_error": True,
            "output_policy": "overwrite",
        }