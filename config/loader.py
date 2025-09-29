# config.py
import json
import yaml
from pathlib import Path
from typing import Dict, Any
from ruamel.yaml import YAML

# 创建 YAML 实例（配置统一格式）
_yaml = YAML()
_yaml.default_flow_style = False
_yaml.allow_unicode = True
_yaml.indent(mapping=2, sequence=4, offset=2)
_yaml.preserve_quotes = True
_yaml.sort_keys = False

def to_plain_dict(data):
    """递归地将 CommentedMap / OrderedDict 转为普通 dict"""
    if isinstance(data, dict):
        return {k: to_plain_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [to_plain_dict(item) for item in item]
    else:
        return data

def _yaml_load(config: str | Path) -> Dict[str, Any]:
    return _yaml.load(config)

def load_config(config_path: str | Path) -> Dict[str, Any]:
    """
    加载配置文件，支持 .yaml, .yml, .json
    :param config_path: 配置文件路径
    :return: 配置字典
    :raises: FileNotFoundError, ValueError
    """
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")

    suffix = path.suffix.lower()

    if suffix in (".yaml", ".yml"):
        with open(path, 'r', encoding='utf-8') as f:
            try:
                data = _yaml.load(f)
                return data or {}
            except Exception as e:
                raise ValueError(f"YAML 解析错误: {e}")

    elif suffix == ".json":
        with open(path, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except Exception as e:
                raise ValueError(f"JSON 解析错误: {e}")

    else:
        raise ValueError(f"不支持的配置文件格式: {suffix}，仅支持 .yaml/.yml/.json")


def save_config(config: Dict[str, Any], config_path: str | Path) -> None:
    """
    保存配置到文件，使用 ruamel.yaml 格式化输出
    :param config: 配置字典
    :param config_path: 保存路径
    :raises: ValueError, IOError
    """
    path = Path(config_path)
    suffix = path.suffix.lower()

    if not isinstance(config, dict):
        raise ValueError("配置必须是一个字典对象")

    if suffix in (".yaml", ".yml"):
        # 使用 ruamel.yaml 格式化写入
        with open(path, 'w', encoding='utf-8') as f:
            _yaml.dump(config, f)
    elif suffix == ".json":
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    else:
        raise ValueError(f"不支持的保存格式: {suffix}")


def format_config_yaml(config: Dict[str, Any]) -> str:
    """
    将配置字典格式化为 YAML 字符串（用于显示）
    :param config: 配置字典
    :return: 格式化后的 YAML 字符串
    """
    from io import StringIO
    buffer = StringIO()
    _yaml.dump(config, buffer)
    return buffer.getvalue()






#def load_config(config_path: str | Path) -> Dict[str, Any]:
#    """自动根据扩展名加载 JSON 或 YAML"""
#    path = Path(config_path)
#    if path.suffix.lower() == ".yaml" or path.suffix.lower() == ".yml":
#        with open(path, 'r', encoding='utf-8') as f:
#            
#            return yaml.safe_load(f)
#    else:
#        with open(path, 'r', encoding='utf-8') as f:
#            return json.load(f)


# 加载plugins中的处理函数，自动加载plugins/*.py
import importlib.util
def load_plugins(plugin_dir: str = "plugins"):
    """自动加载 plugins/ 目录下的所有 Python 文件"""
    for file in Path(plugin_dir).glob("*.py"):
        if file.name == "__init__.py":
            continue
        spec = importlib.util.spec_from_file_location(file.stem, file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            print(f"✅ 加载插件: {file.name}")
        except Exception as e:
            print(f"❌ 加载失败 {file.name}: {e}")



def generate_template(output_path: str | Path):
    """生成配置模板"""
    template = {
        "folderA": "process_text",
        "folderB": {
            "subfolder": "process_csv"
        },
        "*.txt": "backup",
        "**/*.log": "analyze_log"
    }
    path = Path(output_path)
    if path.suffix.lower() == ".yaml" or path.suffix.lower() == ".yml":
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(template, f, indent=2, allow_unicode=True, sort_keys=False)
    else:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(template, f, indent=2, ensure_ascii=False)
    print(f"✅ 模板已生成: {path}")