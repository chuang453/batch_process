from __future__ import annotations

import copy
import json

from qtpy.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from stage2_platform.ui.project_editor import get_source_param_template


class InputsPanel(QGroupBox):

    def __init__(self, parent=None):
        super().__init__("Dataset Inputs", parent)
        self._inputs = []
        self._selected_row = -1
        self._syncing = False
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        btn_add = QPushButton("Add Input")
        btn_add.clicked.connect(self.add_input)
        toolbar.addWidget(btn_add)
        btn_remove = QPushButton("Remove Input")
        btn_remove.clicked.connect(self.remove_selected_input)
        toolbar.addWidget(btn_remove)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self.table = QTableWidget(0, 2)
        self.table.setHorizontalHeaderLabels(["Name", "Source Type"])
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self._on_input_selected)
        self.table.itemChanged.connect(self._on_input_changed)
        layout.addWidget(self.table)

        layout.addWidget(QLabel("Source Params (key / value)"))
        params_toolbar = QHBoxLayout()
        btn_template = QPushButton("Load Source Template")
        btn_template.clicked.connect(self._apply_source_template)
        params_toolbar.addWidget(btn_template)
        btn_add_param = QPushButton("Add Param")
        btn_add_param.clicked.connect(self._add_param)
        params_toolbar.addWidget(btn_add_param)
        btn_remove_param = QPushButton("Remove Param")
        btn_remove_param.clicked.connect(self._remove_param)
        params_toolbar.addWidget(btn_remove_param)
        params_toolbar.addStretch()
        layout.addLayout(params_toolbar)

        self.params_table = QTableWidget(0, 2)
        self.params_table.setHorizontalHeaderLabels(["Key", "Value"])
        self.params_table.verticalHeader().setVisible(False)
        layout.addWidget(self.params_table)

    def set_project(self, project: dict | None) -> None:
        self._inputs = []
        for idx, item in enumerate((project or {}).get("inputs", []) or [], start=1):
            current = dict(item or {})
            current["name"] = str(current.get("name", f"input_{idx}"))
            current["source_type"] = str(current.get("source_type", "file"))
            params = current.get("source_params")
            current["source_params"] = params if isinstance(params, dict) else {}
            self._inputs.append(current)

        self._selected_row = -1
        self._render_inputs_table(select_row=0 if self._inputs else None)

    def get_inputs(self) -> list[dict]:
        self._commit_params_editor()
        rows = []
        for idx, item in enumerate(self._inputs, start=1):
            name = str(item.get("name", "")).strip()
            source_type = str(item.get("source_type", "")).strip()
            params = item.get("source_params") or {}
            if not any((name, source_type, params)):
                continue
            rows.append({
                "name": name or f"input_{idx}",
                "source_type": source_type or "file",
                "source_params": copy.deepcopy(params),
            })
        return rows

    def add_input(self) -> None:
        self._commit_params_editor()
        row = len(self._inputs)
        self._inputs.append({
            "name": f"input_{row + 1}",
            "source_type": "file",
            "source_params": get_source_param_template("file"),
        })
        self._render_inputs_table(select_row=row)

    def remove_selected_input(self) -> None:
        self._commit_params_editor()
        row = self.table.currentRow()
        if row < 0 or row >= len(self._inputs):
            return
        del self._inputs[row]
        next_row = min(row, len(self._inputs) - 1) if self._inputs else None
        self._render_inputs_table(select_row=next_row)

    def _render_inputs_table(self, select_row: int | None = None) -> None:
        self._syncing = True
        self.table.setRowCount(len(self._inputs))
        for row, item in enumerate(self._inputs):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("name", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("source_type", "file"))))
        self._syncing = False

        if select_row is not None and self._inputs:
            self.table.selectRow(max(0, min(select_row, len(self._inputs) - 1)))
        else:
            self.params_table.setRowCount(0)
            self._selected_row = -1

    def _on_input_selected(self) -> None:
        if self._syncing:
            return
        self._commit_params_editor()
        row = self.table.currentRow()
        self._selected_row = row
        self._load_params_editor(row)

    def _on_input_changed(self, item: QTableWidgetItem) -> None:
        if self._syncing:
            return
        row = item.row()
        col = item.column()
        if row < 0 or row >= len(self._inputs):
            return

        current = self._inputs[row]
        text = item.text().strip()
        if col == 0:
            current["name"] = text
        elif col == 1:
            prev_type = str(current.get("source_type", "file"))
            current["source_type"] = text or "file"
            if not current.get("source_params"):
                current["source_params"] = get_source_param_template(current["source_type"])
            elif prev_type != current["source_type"] and row == self._selected_row:
                self._load_params_editor(row)

    def _apply_source_template(self) -> None:
        row = self.table.currentRow()
        if row < 0 or row >= len(self._inputs):
            return
        source_type = self._inputs[row].get("source_type", "file")
        self._inputs[row]["source_params"] = get_source_param_template(str(source_type))
        self._load_params_editor(row)

    def _load_params_editor(self, row: int) -> None:
        self._syncing = True
        if row < 0 or row >= len(self._inputs):
            self.params_table.setRowCount(0)
            self._syncing = False
            return

        params = self._inputs[row].get("source_params") or {}
        keys = list(params.keys())
        self.params_table.setRowCount(len(keys))
        for idx, key in enumerate(keys):
            self.params_table.setItem(idx, 0, QTableWidgetItem(str(key)))
            self.params_table.setItem(idx, 1, QTableWidgetItem(self._format_value(params.get(key))))
        self._syncing = False

    def _commit_params_editor(self) -> None:
        row = self._selected_row
        if row < 0 or row >= len(self._inputs):
            return
        params = {}
        for prow in range(self.params_table.rowCount()):
            key = self._table_text(self.params_table, prow, 0).strip()
            if not key:
                continue
            params[key] = self._parse_value(self._table_text(self.params_table, prow, 1))
        self._inputs[row]["source_params"] = params

    def _add_param(self) -> None:
        row = self.params_table.rowCount()
        self.params_table.insertRow(row)
        self.params_table.setItem(row, 0, QTableWidgetItem("key"))
        self.params_table.setItem(row, 1, QTableWidgetItem(""))

    def _remove_param(self) -> None:
        row = self.params_table.currentRow()
        if row >= 0:
            self.params_table.removeRow(row)

    def _table_text(self, table: QTableWidget, row: int, col: int) -> str:
        item = table.item(row, col)
        return item.text() if item is not None else ""

    def _parse_value(self, text: str):
        raw = str(text or "").strip()
        if raw == "":
            return ""
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def _format_value(self, value) -> str:
        if isinstance(value, (dict, list, bool, int, float)) or value is None:
            return json.dumps(value, ensure_ascii=False)
        return str(value)