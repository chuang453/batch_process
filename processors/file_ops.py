'''
处理文件或文件夹的一些处理函数

使用context.data数据，此文件中的所有函数使用context.data['file_ops']字典写入数据

'''
import re
from pathlib import Path
import shutil
from typing import Dict, Any, Tuple, List
from core.engine import ProcessingContext
from decorators.processor import processor
SCRIPT_DIR = Path(__file__).parent.resolve()   ##此脚本的路径

@processor(name="backup_file", priority=60, source = SCRIPT_DIR, metadata={
    "name": "备份",
    "author": "guancc",
    "version": "1.0",
    "description": "备份文件到指定目录",
    "supported_types": [""],
    "tags": [""]
})
def backup_file(file_path: Path, context, **kwargs):  #, backup_dir: str = "/backup"
    """
    备份文件到指定目录
    """
    root_dir = context.root_path or '.'
    backup_dir = kwargs.get("backup_dir", "./backup")    ##相对路径
    
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
        return {
            "error": str(e),
            "path": str(file_path)
        }


@processor(name="backup_file1", priority=60, source = SCRIPT_DIR, metadata={
    "name": "备份文件",
    "author": "guancc",
    "version": "1.0",
    "description": "备份文件到 .bak,data['file_ops']['renamed']中存储备份的文件列表",
    "supported_types": [""],
    "tags": [""]
})
#@processor("backup_file1")
def backup_file1(file: Path, context: ProcessingContext, **kwargs) -> Dict[str, Any]:
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



@processor(name="rename_file", priority=60, source = SCRIPT_DIR, metadata={
    "name": "重命名",
    "author": "guancc",
    "version": "1.0",
    "description": "重命名文件, data['file_ops']['renamed']中存储修改信息列表",
    "supported_types": [""],
    "tags": [""]
})
#@processor("rename_file")
def rename_file(file: Path, context: ProcessingContext, **kwargs) -> Dict[str, Any]:
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

@processor(name="delete_file", priority=60, source = SCRIPT_DIR, metadata={
    "name": "删除文件",
    "author": "guancc",
    "version": "1.0",
    "description": "删除文件, data['file_ops']['deleted']中存储删除的文件名",
    "supported_types": [""],
    "tags": [""]
})
def delete_file(file: Path, context: ProcessingContext, **kwargs) -> Dict[str, Any]:
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
@processor(name="set_path_name_dict", priority=60, source = SCRIPT_DIR, metadata={
    "name": "set_path_name_dict",
    "author": "guancc",
    "version": "1.0",
    "description": "从文件夹下读取此文件夹下所有文件的名称字典",
    "supported_types": [""],
    "tags": [""]
})
def set_path_name_dict(path: Path, context: ProcessingContext, **kwargs):
    if not path.is_dir():    ##非文件夹，跳过
        return {
            "file": str(path),
            "processor": "set_path_name_dict",
            "status": "skipped",
            "reason": "not a directory"
        }

    # 参数
    _dict_file = kwargs.get('_dict_file', '_dict.txt')
    force = bool(kwargs.get('force', False))
    category_suffix = kwargs.get('category_suffix', '.cate')

    all_dict = context.setdefault_data(["file_ops", "path_name_dict", str(path)], {})
    dict_file = path / _dict_file

    # 解析器：更鲁棒地解析键值对文件，返回字典和警告列表
    def _parse_dict_file(p: Path, sep_pattern: str = r'\s*,\s*|\s+') -> Tuple[Dict[str, str], List[str]]:
        cfg: Dict[str, str] = {}
        warnings: List[str] = []
        if not p.is_file():
            return cfg, warnings
        with open(p, 'r', encoding=kwargs.get('encoding', 'utf-8')) as f:
            for i, raw in enumerate(f, 1):
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue
                parts = re.split(sep_pattern, line, maxsplit=1)
                if len(parts) < 2:
                    warnings.append(f"line {i}: missing value\n  {line}")
                    continue
                key, value = parts[0].strip(), parts[1].strip()
                if not key:
                    warnings.append(f"line {i}: empty key\n  {line}")
                    continue
                # 如果重复键，记录警告并覆盖（保持最后一条生效）
                if key in cfg:
                    warnings.append(f"line {i}: duplicate key '{key}', overwritten")
                cfg[key] = value
        return cfg, warnings

    # 只有在文件存在并且未解析过时，或者强制重载时才解析
    parse_warnings: List[str] = []
    parsed: Dict[str, str] = {}
    if dict_file.is_file() and (force or not all_dict):
        parsed, parse_warnings = _parse_dict_file(dict_file)
        if parsed:
            all_dict.update(parsed)

    # 为目录内的每个子项设置标签（列表形式）
    path_label = context.get_data(['labels', str(path)], []) or []
    labels_added = 0
    for pathi in sorted(path.iterdir()):
        # 仅对文件和目录设置标签
        name = pathi.name
        label_value = all_dict.get(name, name)
        new_label = path_label + [label_value]
        context.set_data(['labels', str(pathi)], new_label)
        labels_added += 1

    # 收集 category 文件（支持多个 .cate 文件），保留发现顺序
    path_cate = context.get_data(['categories', str(path)], []) or []
    cate_list = [p.stem for p in sorted(path.glob(f'*{category_suffix}'))]
    for pathi in sorted(path.iterdir()):
        context.set_data(['categories', str(pathi)], path_cate + cate_list)

    return {
        "file": str(path),
        "processor": "set_path_name_dict",
        "status": "success",
        "entries_parsed": len(parsed),
        "warnings": parse_warnings,
        "labels_added": labels_added,
        "categories": cate_list
    }

