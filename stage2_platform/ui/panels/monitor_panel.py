from __future__ import annotations

from qtpy.QtWidgets import QGroupBox, QTableWidget, QTableWidgetItem, QTextEdit, QVBoxLayout


class MonitorPanel(QGroupBox):

    def __init__(self, parent=None):
        super().__init__("Run Monitor", parent)
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Series", "Status", "Error"])
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(self.log)

    def set_records(self, records) -> None:
        rows = list(records or [])
        self.table.setRowCount(len(rows))
        for row, rec in enumerate(rows):
            self.table.setItem(row, 0, QTableWidgetItem(str(getattr(rec, "series_name", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(getattr(rec, "status", ""))))
            self.table.setItem(row, 2, QTableWidgetItem(str(getattr(rec, "error", ""))))

    def append_log(self, text: str) -> None:
        self.log.append(str(text))

    def clear(self) -> None:
        self.table.setRowCount(0)
        self.log.clear()
