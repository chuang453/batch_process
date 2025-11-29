# decorators.py
from typing import Callable, Dict, Any, List, Union, Tuple, Iterable
from dataclasses import dataclass,field
from pathlib import Path



## 设置字典datadict的键和值，keys可为列表，此时datadict为嵌套字典
#  键存在，则原地修改，否则创建新键
def set_dict_data(datadict: Dict[Any, Any], keys: Any, value: Any) -> None:
    """
    设置嵌套字典的值。
    keys 可以是字符串（单层）、列表（多层）。
    若路径不存在则创建嵌套字典。
    原地修改。
    """
    if not isinstance(keys, list):
        datadict[keys] = value
#        return self.shared.get(key, default)
    if len(keys) == 0:
        raise ValueError("Keys list cannot be empty")

    shared = datadict
    # 遍历前 n-1 层，确保路径存在
    for key in keys[:-1]:
        if key not in shared:
            shared[key] = {}  # 只有不存在才创建
        if not isinstance(shared[key], dict):
            raise TypeError(f"Cannot set nested key '{key}' because it is not a dict")
        shared = shared[key]
    # 设置最终的值
    shared[keys[-1]] = value

## 根据键取得atadict的值，keys可为列表，此时datadict为嵌套字典
def get_dict_data(datadict: Dict[Any, Any], keys: Any, default: Any = None) -> Any:
    """
    获取嵌套字典的值。
    keys 可以是字符串、列表。
    如果路径不存在或类型不匹配，返回 default。
    """
    if not isinstance(keys, list):
        return datadict.get(keys, default)
    
    if len(keys) == 0:
        return default

    shared = datadict
    try:
        for key in keys:
            if not isinstance(shared, dict) or key not in shared:
                raise KeyError(key) 
            shared = shared[key]
        return shared
    except (KeyError, TypeError):
        return default


## 类似字典的setdefault方法,只不过支持嵌套字典
## 当datadict中存在 keys对应的值，则返回该值;若不存在，则会逐级创建空字典，直至最后一级设置为default
def setdefault_dict_data(datadict: Dict, keys: Any, default = None):
    """
    类似 dict.setdefault，但支持嵌套路径。
    如果路径已存在且有值，则返回该值；
    否则逐级创建字典，并在最终键上设置 default 并返回。
    """
    if not isinstance(keys, list):
        return datadict.setdefault(keys, default)

    if len(keys) == 0:
        return default
    
    shared = datadict
    for key in keys[:-1]:
        if key not in shared:
            shared[key] = {}
        if not isinstance(shared[key], dict):
            raise TypeError(f"Cannot set nested key '{key}' because it is not a dict")
        shared = shared[key]

    return shared.setdefault(keys[-1], default)


# 上下文对象：函数间传递数据的“背包”
@dataclass
class ProcessingContext:
    root_path = None     ##批处理的根目录 Path对象
    meta_colnames: List[str] = field(default_factory=lambda: ['处理函数','输入变量','执行情况','执行顺序','警告信息','错误信息'])
    data: Dict[str, Any] = field(default_factory=dict)  # 存储任意数据
    results: List[Any] = field(default_factory=list)    # 收集处理结果
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元信息
    shared: Dict[str, Any] = field(default_factory=dict)    # 全局共享数据

    def clear(self):
        self.data.clear()
        self.results.clear()
        self.metadata.clear()
        self.shared.clear()

    def set_data(self, keys: Any, value: Any):
        set_dict_data(self.data, keys, value)

    def get_data(self, keys: Any, default = None):
        return get_dict_data(self.data, keys, default)

    def setdefault_data(self, keys: Any, default = None):
        return setdefault_dict_data(self.data, keys, default)
    
    #
    def add_result(self, result: Any):
        self.results.append(result)

    def update_metadata(self, **kwargs):
        self.metadata.update(kwargs)

    def set_metadata(self, keys: Any, value: Any):
        set_dict_data(self.metadata, keys, value)

    def get_metadata(self, keys: Any, default = None):
        return get_dict_data(self.metadata, keys, default)

    def setdefault_metadata(self, keys: Any, default = None):
        return setdefault_dict_data(self.metadata, keys, default)

    ##设置共享数据，这里的keys是嵌套字典
    # ['key1', 'key2', 'key3']
    def set_shared(self, keys: Any, value: Any):
        set_dict_data(self.shared, keys, value)

    ## 从shared中取值，keys为list时，
    def get_shared(self, keys: Any, default=None):
        return get_dict_data(self.shared, keys, default)

    def setdefault_shared(self, keys: Any, default = None):
        return setdefault_dict_data(self.shared, keys, default)

    # 扩展：删除共享命名空间或具体键
    def delete_shared(self, keys: Any):
        if not isinstance(keys, list):
            self.shared.pop(keys, None)
            return
        # 逐级定位父字典
        if len(keys) == 0:
            return
        parent = self.shared
        for k in keys[:-1]:
            if not isinstance(parent, dict) or k not in parent:
                return
            parent = parent.get(k)
        if isinstance(parent, dict):
            parent.pop(keys[-1], None)

    # 扩展：列出某命名空间下所有键（返回扁平路径列表）
    def list_shared_namespace(self, prefix: List[str] = None) -> List[List[str]]:
        ns = self.shared
        if prefix:
            ns = get_dict_data(self.shared, prefix, {})
        paths: List[List[str]] = []

        def walk(node, base: List[str]):
            if isinstance(node, dict):
                for k, v in node.items():
                    walk(v, base + [str(k)])
            else:
                paths.append(base)

        walk(ns, prefix or [])
        return paths



# 全局处理器注册表
PROCESSORS: Dict[str, Callable[[Path, ProcessingContext], Any]] = {}
# 新增：前处理器和后处理器注册表
PRE_PROCESSORS: Dict[str, Callable[[ProcessingContext], Any]] = {}
POST_PROCESSORS: Dict[str, Callable[[ProcessingContext], Any]] = {}

# 示例处理器函数（供配置引用）
def process_text(folder): print(f"📄 处理文本: {folder}")
def process_csv(folder): print(f"📊 处理 CSV: {folder}")
def backup(file): print(f"💾 备份: {file}")
def analyze_log(file): print(f"🔍 分析日志: {file}")

# 函数名 → 函数对象
AVAILABLE_PROCESSORS = {
    "process_text": process_text,
    "process_csv": process_text,
    "backup": backup,
    "analyze_log": analyze_log,
}




def _set_processor_attributes(
    func: Callable,
    name: str,            ##名称
    kind: str,            ##'pre', 'file', 'post'
    priority: int = 50,   ##优先级
    must_excute: bool = False,   ##是否必须执行，为True时，一定会执行，但会根据priority排序
    source: str = '未知',  ##处理器来源， 即所在路径
    type_hint: str = "file",    ##处理文件类型如 'file', 'dir', 'image' 等（可选
    metadata: Dict[str, Any] = None   
):
    """
    统一设置函数的插件元数据
    """
    func.processor_name = name
    func.processor_kind = kind           # 'pre', 'file', 'post'
    func.processor_priority = priority   # 整数，用于排序
    func.processor_must_excute = must_excute   # 整数，用于排序
    func.processor_source = source       # 处理器来源， 即所在路径
    func.processor_type = type_hint      # 如 'file', 'dir', 'image' 等（可选）
    func.metadata = metadata or {}
    return func

###注册的函数名优先名为 name, 优先级priority
## 所有用此装饰器的处理函数都会被注册到PROCESSORS中   kind: str = "file",
def processor(name: str = None,  priority: int = 50, must_excute: bool = False, source = '未知', type_hint: str = 'file' , metadata: dict = None):
    """
    装饰器：注册一个文件/目录处理器

    示例：
        @processor(name="resize_images", priority=60, kind="image", metadata={
            "name": "图像缩放",
            "author": "Alice",
            "version": "1.0",
            "description": "将图片统一缩放到指定尺寸",
            "supported_types": ["jpg", "png"],
            "tags": ["image", "resize"]
        })
        def resize_images(file_path, context, **kwargs):
            ...
    """
    def decorator(func):
        proc_name = name or func.__name__
        func.reload_info = ''
        if proc_name in PROCESSORS:
            func.reload_info = f'处理器{proc_name}已存在，将重载'
   #         raise ValueError(f"处理器已存在: {proc_name}")
        func = _set_processor_attributes(func, proc_name, 'file', priority, must_excute, source, type_hint, metadata)
        func.called_path = []     ##调用的path列表
        PROCESSORS[proc_name] = func
        AVAILABLE_PROCESSORS[proc_name] = func 
        return func
    return decorator

# 使用时只需 @processor
#@processor(name="backup_file", type="file", priority=10)
#def backup_file(file_path, context, backup_dir="./backup/"):
    # ..., kind: str = "pre"


def pre_processor(name: str = None,  priority: int = 50, source = '未知', metadata: dict = None):
    """
    装饰器：注册一个预处理器（在遍历前执行）

    示例：
        @pre_processor(name="backup_dir", priority=10)
        def backup_before_processing(root_path, context):
            ...
    """
    def decorator(func):
        proc_name = name or func.__name__
        func.reload_info = ''
        if proc_name in PRE_PROCESSORS:
            func.reload_info = f'前处理器{proc_name}已存在，将重载'
#            raise ValueError(f"预处理器已存在: {proc_name}")

        func = _set_processor_attributes(func, proc_name, 'pre', priority,True, source, '', metadata)
        PRE_PROCESSORS[proc_name] = func
        AVAILABLE_PROCESSORS[proc_name] = func
        return func
    return decorator

def post_processor(name: str = None,  priority: int = 50, source = '未知', metadata: dict = None):
    """
    装饰器：注册一个后处理器（在所有文件处理后执行）

    示例：
        @post_processor(name="generate_report", priority=90)
        def generate_summary_report(root_path, context):
            ...
    """
    def decorator(func):
        proc_name = name or func.__name__
        func.reload_info = ''
        if proc_name in POST_PROCESSORS:
            func.reload_info = f'后处理器{proc_name}已存在，将重载'
 #           raise ValueError(f"后处理器已存在: {proc_name}")

        func = _set_processor_attributes(func, proc_name, "post", priority,True, source, '', metadata)
        POST_PROCESSORS[proc_name] = func
        AVAILABLE_PROCESSORS[proc_name] = func
        return func
    return decorator


##反注册func: Callable[[Path, ProcessingContext], Any]
def _unregister_processor(func_name: str):
    PROCESSORS.pop(func_name)

def _unregister_pre(func_name: str):
    PRE_PROCESSORS.pop(func_name)

def _unregister_post(func_name: str):
    POST_PROCESSORS.pop(func_name)



# ✅ 辅助函数：获取所有已注册处理器信息（可用于调试或生成文档）
def get_all_processors():
    """返回所有注册的处理器信息列表"""
    result = []
    for reg, kind in [
        (PRE_PROCESSORS, "pre"),
        (PROCESSORS, "file"),
        (POST_PROCESSORS, "post")
    ]:
        for name, func in reg.items():
            result.append({
                "name": name,
                "kind": kind,
                "priority": getattr(func, "processor_priority", 50),
                "source": getattr(func, "processor_source", '未知'),
                "type": getattr(func, "processor_type", ""),
                "metadata": getattr(func, "metadata", {}),
                "func": func
            })
    return sorted(result, key=lambda x: (["pre", "file", "post"].index(x["kind"]), x["priority"]))





##其他功能装饰器
# decorators.py
import time
from functools import wraps
##失败重试装饰器。通常与@processor协同使用。提供容错
'''
🔁 自动重试	函数失败时自动重试最多 max_attempts 次
⏳ 指数退避	每次等待时间翻倍（delay * backoff）
📝 结构化错误返回	失败后返回一个错误记录，而不是抛异常（避免中断整个批处理）
💬 日志输出	打印重试信息，便于监控


'''
def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            last_error = None
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    last_error = e
                    if attempts >= max_attempts:
                        break
                    print(f"🔁 {func.__name__} 失败，{current_delay:.1f}s 后重试 ({attempts}/{max_attempts})")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            print(f"❌ {func.__name__} 最终失败: {last_error}")
            # 返回错误记录
            if args and len(args) >= 2:
                path = args[0]
                return {
                    "file": str(path),
                    "processor": getattr(func, 'processor_name', func.__name__),
                    "status": "failed",
                    "error": str(last_error),
                    "attempt": attempts
                }
            raise last_error
        return wrapper
    return decorator

# 使用：先装饰为可重试函数，再装饰为processor
@processor("download_file")
@retry(max_attempts=3, delay=2)
def download_file(url: Path, context: ProcessingContext):
    # 模拟网络请求
    pass