# FileStructureWidget.py

from qtpy.QtWidgets import QWidget, QVBoxLayout, QTreeView
from qtpy.QtGui import QStandardItemModel, QStandardItem
from qtpy.QtCore import Qt

class FileStructureWidget(QWidget):
    """
    可视化嵌套字典结构，值为列表，每个元素显示为一列。
    路径只显示局部名称（不拼接父路径）。
    """

    def __init__(self, data=None, column_names=None, parent=None):
        super().__init__(parent)
        self.column_names = column_names
        self.init_ui()
        if data is not None:
            self.set_data(data)

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(False)
        self.tree_view.setAlternatingRowColors(True)
        self.tree_view.setSelectionMode(QTreeView.SingleSelection)

        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)

        layout.addWidget(self.tree_view)
        self.setLayout(layout)

    def set_data(self, data):
        """设置数据并重建树"""
        if not data:
            return

        # 推断列数
        sample_value = self._get_sample_value(data)
        if not isinstance(sample_value, list):
            raise ValueError("所有值必须是列表")

        num_columns = len(sample_value)
        self.num_columns = num_columns

        # 设置列名
        if self.column_names and len(self.column_names) == num_columns:
            headers = ["名称"] + self.column_names
        else:
            headers = ["名称"] + [f"字段{i}" for i in range(num_columns)]

        self.model.clear()
        self.model.setHorizontalHeaderLabels(headers)

        root_item = self.model.invisibleRootItem()
        self._build_tree(data, root_item)

    def _get_sample_value(self, data):
        """找一个非字典的列表作为样本"""
        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, list):
                    return value
                elif isinstance(value, dict):
                    sample = self._get_sample_value(value)
                    if sample is not None:
                        return sample
        return None

    def _build_tree(self, data, parent_item):
        if not isinstance(data, dict):
            return
    
        for name, value in data.items():
            if name.endswith('/'):
                # 这是一个文件夹容器，例如 "folder/"
                folder_key = name  # "folder/"
                display_name = name.rstrip('/')  # 显示为 "folder"
    
                # 查找对应的属性值：键为 "folder"（无 /）
                attr_key = display_name  # "folder"
                attr_value = data.get(attr_key, None)
    
                # 创建文件夹项
                name_item = QStandardItem(display_name)
                name_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                items = [name_item]
    
                # 填充属性列
                if isinstance(attr_value, list) and len(attr_value) == self.num_columns:
                    for v in attr_value:
                        item = QStandardItem(str(v))
                        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                        items.append(item)
                else:
                    # 如果没有属性或格式不对，留空
                    for _ in range(self.num_columns):
                        items.append(QStandardItem(""))
    
                parent_item.appendRow(items)
    
                # 递归处理子结构（只有 value 是字典时才递归）
                if isinstance(value, dict):
                    self._build_tree(value, items[0])  # 使用当前行作为父节点
    
            elif not name.endswith('/'):
                # 普通项，且不是文件夹的属性（因为对应的 "name/" 不存在）
                # 或者是被 "name/" 使用过的属性，但不应单独显示
                # 所以我们只在 "name/" 存在时使用它，否则作为普通项显示？
                
                # 根据你的设计，如果 "name/" 存在，则 "name" 是属性，不单独显示
                # 如果 "name/" 不存在，则 "name" 是普通项
                if (name + '/') in data:
                    continue  # 跳过，因为它已被作为文件夹的属性使用
                else:
                    # 独立普通项
                    name_item = QStandardItem(name)
                    name_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                    items = [name_item]
    
                    if isinstance(value, list) and len(value) == self.num_columns:
                        for v in value:
                            item = QStandardItem(str(v))
                            item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                            items.append(item)
                    else:
                        for _ in range(self.num_columns):
                            items.append(QStandardItem(""))
    
                    parent_item.appendRow(items)