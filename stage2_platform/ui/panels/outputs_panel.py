from __future__ import annotations

from pathlib import Path

import pandas as pd
from qtpy.QtWidgets import (
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)


class OutputsPanel(QGroupBox):

    def __init__(self, parent=None):
        super().__init__("Outputs & Lineage", parent)
        self._catalog = {}
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Datasets"))
        self.table = QTableWidget(0, 2)
        self.table.setHorizontalHeaderLabels(["Output Key", "Shape"])
        self.table.verticalHeader().setVisible(False)
        self.table.itemSelectionChanged.connect(self._on_dataset_selected)
        layout.addWidget(self.table)

        control_row = QHBoxLayout()
        control_row.addWidget(QLabel("Preview Rows"))
        self.preview_rows = QSpinBox()
        self.preview_rows.setMinimum(1)
        self.preview_rows.setMaximum(5000)
        self.preview_rows.setValue(50)
        self.preview_rows.valueChanged.connect(self._refresh_preview)
        control_row.addWidget(self.preview_rows)

        btn_export_csv = QPushButton("Export CSV")
        btn_export_csv.clicked.connect(lambda: self._export_selected("csv"))
        control_row.addWidget(btn_export_csv)
        btn_export_parquet = QPushButton("Export Parquet")
        btn_export_parquet.clicked.connect(lambda: self._export_selected("parquet"))
        control_row.addWidget(btn_export_parquet)
        control_row.addStretch()
        layout.addLayout(control_row)

        self.preview_hint = QLabel("Select an output dataset to preview.")
        self.preview_hint.setWordWrap(True)
        layout.addWidget(self.preview_hint)

        self.preview_table = QTableWidget(0, 0)
        self.preview_table.verticalHeader().setVisible(False)
        layout.addWidget(self.preview_table)

    def set_catalog(self, catalog) -> None:
        self._catalog = dict(catalog or {})
        rows = []
        for key, value in self._catalog.items():
            shape = getattr(value, "shape", "")
            rows.append((key, shape))

        self.table.setRowCount(len(rows))
        for row, (key, shape) in enumerate(rows):
            self.table.setItem(row, 0, QTableWidgetItem(str(key)))
            self.table.setItem(row, 1, QTableWidgetItem(str(shape)))

        if rows:
            self.table.selectRow(0)
            self._refresh_preview()
        else:
            self.preview_table.clear()
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(0)
            self.preview_hint.setText("No output dataset available yet.")

    def _on_dataset_selected(self) -> None:
        self._refresh_preview()

    def _refresh_preview(self) -> None:
        key = self._selected_key()
        if not key:
            self.preview_table.clear()
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(0)
            self.preview_hint.setText("Select an output dataset to preview.")
            return

        value = self._catalog.get(key)
        if not isinstance(value, pd.DataFrame):
            self.preview_table.clear()
            self.preview_table.setRowCount(0)
            self.preview_table.setColumnCount(0)
            self.preview_hint.setText(f"{key} is not a DataFrame and cannot be previewed.")
            return

        n_rows = int(self.preview_rows.value())
        preview = value.head(n_rows)
        self.preview_hint.setText(f"Preview: {key} | shape={value.shape} | showing top {len(preview)} rows")
        self.preview_table.setRowCount(len(preview))
        self.preview_table.setColumnCount(len(preview.columns))
        self.preview_table.setHorizontalHeaderLabels([str(c) for c in preview.columns])
        for ridx, row in enumerate(preview.itertuples(index=False, name=None)):
            for cidx, cell in enumerate(row):
                text = "" if pd.isna(cell) else str(cell)
                self.preview_table.setItem(ridx, cidx, QTableWidgetItem(text))

    def _export_selected(self, fmt: str) -> None:
        key = self._selected_key()
        if not key:
            QMessageBox.warning(self, "Export", "Select an output dataset first.")
            return

        value = self._catalog.get(key)
        if not isinstance(value, pd.DataFrame):
            QMessageBox.warning(self, "Export", f"{key} is not a DataFrame.")
            return

        if fmt == "csv":
            filters = "CSV Files (*.csv)"
            default_name = f"{key}.csv"
        else:
            filters = "Parquet Files (*.parquet)"
            default_name = f"{key}.parquet"

        path, _ = QFileDialog.getSaveFileName(self, f"Export {fmt.upper()}", str(Path.cwd() / default_name), filters)
        if not path:
            return

        try:
            if fmt == "csv":
                value.to_csv(path, index=False)
            else:
                value.to_parquet(path, index=False)
            QMessageBox.information(self, "Export", f"Exported to:\n{path}")
        except Exception as exc:
            QMessageBox.critical(self, "Export failed", str(exc))

    def _selected_key(self) -> str:
        row = self.table.currentRow()
        if row < 0:
            return ""
        item = self.table.item(row, 0)
        return item.text() if item is not None else ""