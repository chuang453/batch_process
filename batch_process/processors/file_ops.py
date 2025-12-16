'''
处理文件或文件夹的一些处理函数

使用context.data数据，此文件中的所有函数使用context.data['file_ops']字典写入数据

'''
import re
from pathlib import Path
import shutil
from typing import Dict, Any
from core.engine import ProcessingContext
from decorators.processor import processor

SCRIPT_DIR = Path(__file__).parent.resolve()  ##此脚本的路径


@processor(name="backup_file",
           priority=60,
           source=SCRIPT_DIR,
           metadata={
               "name": "备份",
               "author": "guancc",
               "version": "1.0",
               "description": "备份文件到指定目录",
               "supported_types": [""],
               "tags": [""]
           })
def backup_file(file_path: Path, context,
                **kwargs):  #, backup_dir: str = "/backup"
    """
    备份文件到指定目录
    """
    root_dir = context.root_path or '.'
    backup_dir = kwargs.get("backup_dir", "./backup")  ##相对路径

    if not file_path.is_file():
        return {f"skipped: {file_path} is not a file"}

    backup_root = Path(backup_dir)
    # 保持目录结构
    rel_path = file_path.relative_to(root_dir)
    backup_path = backup_root / rel_path

    try:
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, backup_path)
        print(f"  📦 备份: {file_path} → {backup_path}")
        return {
            "action": "backup",
            "from": str(file_path),
            "to": str(backup_path)
        }
    except Exception as e:
        print(f"  ❌ 备份失败 {file_path}: {e}")
        return {"error": str(e), "path": str(file_path)}


@processor(name="backup_file1",
           priority=60,
           source=SCRIPT_DIR,
           metadata={
               "name": "备份文件",
               "author": "guancc",
               "version": "1.0",
               "description":
               "备份文件到 .bak,data['file_ops']['renamed']中存储备份的文件列表",
               "supported_types": [""],
               "tags": [""]
           })
#@processor("backup_file1")
def backup_file1(file: Path, context: ProcessingContext,
                 **kwargs) -> Dict[str, Any]:
    """备份文件到 .bak"""
    backup_path = file.with_suffix(file.suffix + ".bak")
    try:
        shutil.copy2(file, backup_path)
        # 全局共享备份列表
        #        context.shared.setdefault("backups", []).append(str(backup_path))
        file_op_data = context.data.setdefault("file_ops", {})  ##file_op的数据
        file_op_data.setdefault("backups", []).append(str(backup_path))
        return {
            "file": str(file),
            "backup": str(backup_path),
            "processor": "backup_file",
            "status": "success"
        }
    except Exception as e:
        return {
            "file": str(file),
            "processor": "backup_file",
            "status": "error",
            "error": str(e)
        }


@processor(name="rename_file",
           priority=60,
           source=SCRIPT_DIR,
           metadata={
               "name": "重命名",
               "author": "guancc",
               "version": "1.0",
               "description": "重命名文件, data['file_ops']['renamed']中存储修改信息列表",
               "supported_types": [""],
               "tags": [""]
           })
#@processor("rename_file")
def rename_file(file: Path, context: ProcessingContext,
                **kwargs) -> Dict[str, Any]:
    """重命名文件（示例：添加前缀）"""
    new_name = file.parent / f"processed_{file.name}"
    try:
        file.rename(new_name)
        #     context.shared.setdefault("renamed", []).append({
        #         "from": str(file),
        #        "to": str(new_name)
        #    })
        file_op_data = context.data.setdefault("file_ops", {})  ##file_op的数据
        file_op_data.setdefault("renamed", []).append({
            "from": str(file),
            "to": str(new_name)
        })
        return {
            "file": str(file),
            "new_name": str(new_name),
            "processor": "rename_file",
            "status": "success"
        }
    except Exception as e:
        return {
            "file": str(file),
            "processor": "rename_file",
            "status": "error",
            "error": str(e)
        }


@processor(name="delete_file",
           priority=60,
           source=SCRIPT_DIR,
           metadata={
               "name": "删除文件",
               "author": "guancc",
               "version": "1.0",
               "description": "删除文件, data['file_ops']['deleted']中存储删除的文件名",
               "supported_types": [""],
               "tags": [""]
           })
def delete_file(file: Path, context: ProcessingContext,
                **kwargs) -> Dict[str, Any]:
    """删除文件（谨慎使用）"""
    try:
        file.unlink()
        #    context.shared.setdefault("deleted", []).append(str(file))
        file_op_data = context.data.setdefault("file_ops", {})  ##file_op的数据
        file_op_data.setdefault("deleted", []).append(str(file))
        return {
            "file": str(file),
            "processor": "delete_file",
            "status": "deleted"
        }
    except Exception as e:
        return {
            "file": str(file),
            "processor": "delete_file",
            "status": "error",
            "error": str(e)
        }


## 为所有文件夹或文件名对应一个新名称。由字典_dict给定{路径名: 新名称}，并在context.data['labels']中为其内各文件夹和文件的添加别名
##     context.data['labels'][path] 对应path的别名， 是一个列表 [name1, name2,...] ，namei对应其各父级path的别名
## 这个_dict由文件夹内的文件_dict.txt(默认名)指定。_dict.txt可用参数字典config['_dict_file']指定
## _dict.txt内有2列数据，第一列为键、第二列为值。键和值之间可由任何空格、制表位、逗号隔开
##  若要在其它处理函数中引用这个字典，可用data["file_ops"]["path_name_dict"][str(path)] 存储文件夹path内所有文件的对应字典
##
@processor(name="set_path_name_dict",
           priority=60,
           source=SCRIPT_DIR,
           metadata={
               "name": "set_path_name_dict",
               "author": "guancc",
               "version": "1.0",
               "description": "从文件夹下读取此文件夹下所有文件的名称字典",
               "supported_types": [""],
               "tags": [""]
           })
def set_path_name_dict(path: Path, context: ProcessingContext, **kwargs):
    """读取目录下的名称映射文件并在 `context` 中注册 `labels` 与 `categories`、'category_label_map'。
    功能概述:
    - 从目录 `path` 中读取一个名称字典文件（默认 `_dict.txt`，可通过 `_dict_file` 参数指定），
        将文件中每行的 "键 值" 对解析为 {basename: label} 并存入 `context.data['file_ops']['path_name_dict'][str(path)]`。
    - 为目录内每个项 `pathi` 在 `context.data['labels'][str(pathi)]` 追加对应别名（若字典无对应项则回退为文件名）。
    - 搜索以 `category_suffix`（默认 `.cate`）结尾的文件来发现目录类别；如果找到则把类别名追加到
        `context.data['categories'][str(pathi)]`。
    参数:
    - `path` (Path): 目标目录；若不是目录函数直接返回。
    - `context` (ProcessingContext): 处理上下文，函数使用 `context.setdefault_data` / `context.set_data`
        或 `context.data` 保存结果：
            - 字典存储位置: `context.data['file_ops']['path_name_dict'][str(path)]`
            - 标签位置: `context.data['labels'][str(pathi)]`（为列表，包含父目录前缀 + 本级别别名）
            - 类别位置: `context.data['categories'][str(pathi)]`
            - 类别到标签映射: `context.data['category_label_map']`，键为类别名，值为该类别下所有条目的标签列表。
    - 可选 `kwargs`:
            - `_dict_file` (str): 字典文件名，默认 `_dict.txt`。
            - `category_suffix` (str): 类别文件后缀，默认 `.cate`。
    关于缺失或格式不正确的字典文件:
    - 如果字典文件不存在，函数不会抛错；`path_name_dict` 保持为空或已有值，随后为每个 `pathi`
        使用 `all_dict.get(pathi.name, pathi.name)` 回退到原始文件名作为标签。
    - 如果字典文件存在但某行格式不正确（少于两列或键为空），该行会被跳过并打印警告，不会抛出异常。
    返回值:
    - 成功时返回 `{"file": str(path), "processor": "set_path_name_dict", "status": "success"}`。
    示例:
            set_path_name_dict(Path('data/project'), context, _dict_file='_names.txt')
    """
    if not path.is_dir():  ##非文件夹，跳过
        return

    all_dict = context.setdefault_data(
        ["file_ops", "path_name_dict", str(path)], {})
    _dict_file = kwargs.get('_dict_file', '_dict.txt')  ##字典文件名
    dict_file = path / _dict_file

    ##文件存在，则读取
    if dict_file.is_file() and not all_dict:  ## all_dict为空字典时
        separator_pattern = r'\s*,\s*|\s+'
        config = {}
        with open(dict_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                # 跳过空行和注释
                if not line or line.startswith('#'):
                    continue

                # 使用正则分割，最多分割成两部分（防止值中包含分隔符）
                parts = re.split(separator_pattern, line, maxsplit=1)

                if len(parts) < 2:
                    print(f"⚠️  第 {line_num} 行格式错误（缺少值）: {line}")
                    continue

                key, value = parts[0].strip(), parts[1].strip()
                if not key:
                    print(f"⚠️  第 {line_num} 行键为空: {line}")
                    continue

                config[key] = value
        all_dict.update(config)

    ##为其内文件添加别名
    path_label = context.get_data(['labels', str(path)], [])
    for pathi in path.iterdir():
        context.set_data(['labels', str(pathi)],
                         path_label + [all_dict.get(pathi.name, pathi.name)])


##文件夹的category名

    path_cate = context.get_data(['categories', str(path)], [])
    _suffix = kwargs.get('category_suffix', '.cate')
    cate_name = [pathi.stem for pathi in path.glob('*' + _suffix)]
    if cate_name:
        for pathi in path.iterdir():
            context.set_data(['categories', str(pathi)],
                             path_cate + [cate_name[0]])

        # build category -> labels mapping at context.data['category_label_map']
        # ensure context.data exists and is dict-like
        cd = getattr(context, 'data', None)
        if cd is None or not isinstance(cd, dict):
            # try to set an attribute-safe dict
            try:
                context.data = {}
                cd = context.data
            except Exception:
                cd = {}
        cat = cate_name[0]
        cat_map = cd.setdefault('category_label_map', {})
        for pathi in path.iterdir():
            lbl = all_dict.get(pathi.name, pathi.name)
            cat_map.setdefault(cat, []).append(lbl)
        # stored in-place under context.data['category_label_map']

    return {
        "file": str(path),
        "processor": "set_path_name_dict",
        "status": "success"
    }
