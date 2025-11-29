Batch Process Framework (GUI + CLI)

概述
- 这是一个递归批处理框架，按配置（YAML/JSON）对目录与文件执行命名处理器，支持 GUI 与 CLI 两种入口。
- 重要模块：
  - `core/engine.py`：批处理核心（遍历、匹配规则、执行 pre/inline/post 处理器）
  - `decorators/processor.py`：处理器装饰器与 `ProcessingContext`
  - `config/loader.py`：配置与插件加载

## 辅助库使用指南

本项目提供了一组轻量的辅助库，帮助你在处理器中更方便地管理上下文数据、读取常见数据格式，以及与 Word/绘图等外部库安全集成。下面是高频用法的精简示例。

### Pipeline 核心辅助 (`utils/pipeline.py`)

- `get_bucket(context, name)`: 取或建一个命名“桶”（字典），用于聚合数据。
- `append_numbers(bucket, key, values)`: 将数值序列追加到桶的某个键。
- `set_output(context, key, value)` / `get_output(context, key)`: 记录/读取本次处理的输出产物路径等。
- `set_config(context, key, value)` / `get_config(context, key, default=None)`: 在上下文中记录/读取运行期配置。
- `record_result(context, status, message, **extra)`: 统一记录处理结果条目。

示例（在某个文件处理器中聚合数值并记录结果）：

```python
from utils.pipeline import get_bucket, append_numbers, record_result, set_output

def process_numbers(path, context):
  bucket = get_bucket(context, "folder_values")
  # 这里假设你已经得到一个数值列表 values
  values = [1, 2, 3]
  append_numbers(bucket, path.parent.name, values)
  set_output(context, "last_processed_file", str(path))
  record_result(context, status="ok", message="numbers appended", file=str(path), count=len(values))
```

### 数据读取辅助 (`utils/io_helpers.py`)

- `safe_read_text(path)`: 以 UTF-8 优先读取文本，自动处理常见换行符。
- `safe_read_json(path)`: 读取 JSON，返回 Python 对象；出错抛异常便于上层记录。
- `csv_values(path, column=None)`: 从 CSV 快速提取数值列（默认第一列）。

示例（自动识别并汇总不同格式的数值）：

```python
from pathlib import Path
from utils.io_helpers import safe_read_text, safe_read_json, csv_values
from utils.pipeline import get_bucket, append_numbers, record_result

def read_data_files(path: Path, context):
  bucket = get_bucket(context, "folder_values")
  name = path.parent.name
  try:
    if path.suffix.lower() == ".csv":
      vals = csv_values(path)
    elif path.suffix.lower() == ".json":
      data = safe_read_json(path)
      vals = data.get("values", []) if isinstance(data, dict) else []
    else:
      text = safe_read_text(path)
      vals = [float(x) for x in text.split() if x.replace('.', '', 1).isdigit()]
    append_numbers(bucket, name, vals)
    record_result(context, "ok", f"read {len(vals)} values", file=str(path))
  except Exception as e:
    record_result(context, "error", f"failed to read: {e}", file=str(path))
```

### Word 适配器 (`utils/adapters/docx_helpers.py`)

- `get_or_create_doc(doc_path)`: 返回 `(Document, Path)`；若文件存在则打开，不存在则创建。
  - 注意：`pipeline` 核心仅记录路径，不直接依赖 `python-docx`；适配器负责实际对象创建。

示例（在进入目录时写标题，在离开目录时保存）：

```python
from pathlib import Path
from utils.adapters.docx_helpers import get_or_create_doc
from utils.pipeline import set_output, get_output, record_result

def enter_dir_write_word(dir_path, context, doc_name="report.docx"):
  doc, resolved = get_or_create_doc(Path(dir_path) / doc_name)
  doc.add_heading(f"Folder: {Path(dir_path).name}", level=1)
  doc.save(str(resolved))
  set_output(context, "doc_path", str(resolved))
  record_result(context, "ok", "doc initialized", doc=str(resolved))

def append_paragraph(context, text):
  from utils.adapters.docx_helpers import get_or_create_doc
  doc_path = get_output(context, "doc_path")
  if not doc_path:
    return
  doc, resolved = get_or_create_doc(Path(doc_path))
  doc.add_paragraph(text)
  doc.save(str(resolved))
```

### 绘图适配器 (`utils/adapters/plot_helpers.py`)

- `save_plot_png_values(values, png_path, dpi=120, width_inches=5)`: 使用 Matplotlib 的 Agg 后端与 Pillow 安全生成 PNG，不依赖 GUI 线程。

示例（在离开目录时根据聚合值生成并记录图片）：

```python
from pathlib import Path
from utils.adapters.plot_helpers import save_plot_png_values
from utils.pipeline import get_bucket, get_output, set_output, record_result

def plot_on_exit_paste_word(dir_path, context):
  bucket = get_bucket(context, "folder_values")
  name = Path(dir_path).name
  values = bucket.get(name, [])
  img_path = Path(dir_path) / f"{name}_plot.png"
  try:
    save_plot_png_values(values, img_path)
    set_output(context, "image_path", str(img_path))
    record_result(context, "ok", "plot saved", image=str(img_path), count=len(values))
  except Exception as e:
    record_result(context, "error", f"plot failed: {e}")
```

### 依赖与安装

- 如果使用 Word 适配器：需要安装 `python-docx`。
- 如果使用绘图适配器：建议安装 `matplotlib` 与 `Pillow`。

在 Windows PowerShell 下安装示例：

```powershell
pip install python-docx matplotlib Pillow
```

以上示例与项目中的 `demos/demo3/plugins/word_plot_pipeline.py` 思路一致，适配器将与外部库的耦合集中管理，核心 `pipeline` 专注于上下文与数据聚合，便于在 GUI 的工作线程中稳定运行。
  - `processors/`、`plugins/`：内置与外部处理器
  - `cli/app.py`、`main_window.py`：命令行与图形界面入口

快速开始
- 依赖安装（建议使用虚拟环境）：
  - 基础：`qtpy`, `pyyaml`, `pandas`
  - 示例演示（可选）：`matplotlib`, `python-docx`, `Pillow`

```powershell
pip install qtpy pyyaml pandas
pip install matplotlib python-docx Pillow
```

- 运行 GUI：
```powershell
python main.py
```

- 运行 CLI：
```powershell
python -m batch_process.cli --processors
python -m batch_process.cli <root> -c <config.yaml>
```

配置文件（YAML）
- 配置以“模式”为键；模式支持 `**`、通配符与以 `/` 结尾的目录匹配。
- 每个规则可定义：
  - `pre_processors`：进入路径前执行的处理器
  - `processors`：针对该路径的处理器（文件或目录）
  - `post_processors`：离开路径时执行的处理器
  - `config`：传给处理器的参数字典
  - `priority`：同类处理器的优先级（越大越先执行）

匹配规则详解（engine 行为）
- 相对路径：所有模式与匹配均以 `root_path` 为基准的相对路径进行（`Path(relative).as_posix()`）。
- 目录模式（以 `/` 结尾）：
  - 只匹配目录本身，支持 `*`、`?`、字符类 `[...]`，以及 `**`（跨层级匹配）。
  - 示例：`"**/"` 匹配遍历中的每一个目录；`"data/**/"` 匹配 `data` 下所有层级的目录。
- 文件/通用模式（不以 `/` 结尾）：
  - 匹配文件或目录路径，支持 `**`。
  - 示例：`"**/*.txt"` 匹配所有文本文件；`"docs/**"` 匹配 `docs` 下所有文件与目录。
- 特殊模式：
  - `"."` 仅匹配根路径（`root_path` 本身）。
- 规则生效与排序：
  - 引擎会收集所有命中的规则，将各自的处理器分类到 `pre`、`inline`（processors）、`post` 三类候选。
  - 同一类处理器按规则内的 `priority` 值降序排序（值越大越先执行）。引擎不会去重，配置里重复列出的处理器会被重复调用。
- 执行时机：
  - `pre_processors` 与 `processors`（inline）在进入该路径前执行；`post_processors` 在处理完子节点后（离开该路径时）执行。
  - 目录会先执行自身的 `pre/inline`，再递归其子项，最后执行自身的 `post`。文件只执行自身的命中规则。
- 进阶建议：
  - 需要“进入目录写”与“离开目录画图”这类行为，分别把处理器挂到 `"**/"` 的 `pre_processors` 与 `post_processors`。
  - 需要文件级读取并在目录级聚合绘图，读取处理器把数据写入 `context.setdefault_data(["folder_data", str(file.parent)], [])`，离开目录时在 `context.get_data(["folder_data", str(dir)], [])` 取回。

示例（摘自 `demos/demo3/word_plot_config.yaml`）：
```yaml
".":
  pre_processors:
    - enter_dir_write_word
  config:
    doc_path: demos/output.docx

"**/":
  pre_processors:
    - enter_dir_write_word
  post_processors:
    - plot_on_exit_paste_word
  config:
    doc_path: demos/output.docx
    img_dir: demos/images

"**/*.txt":
  pre_processors:
    - read_data_files
  config:
    pattern: "*.txt"
    key: values
```

处理器与上下文
- 使用装饰器注册处理器（见 `decorators/processor.py`）：
  - `@processor(name="...", priority=..., source=..., metadata={...})`
  - 处理器签名：`(path: Path, context: ProcessingContext, **kwargs)`
- `ProcessingContext` 提供在处理函数之间传递数据的能力：
  - `context.set_shared(keys, value)`：跨目录/跨阶段共享（如文档路径）
  - `context.setdefault_data(keys, default)`：为某路径建立聚合数据桶
  - `context.get_shared(keys, default)` / `context.get_data(keys, default)`：读取共享或桶数据
  - `context.add_result({...})`：记录结构化结果，GUI“处理结果”表会显示

GUI 要点
- 主界面：
  - 路径设置（配置文件、目标目录、插件目录）
  - 插件表：支持列头点击排序（升/降序），显示来源、启用、名称、类型（PRE/FILE/POST）、优先级、作者、版本
  - 配置编辑区：加载/格式化/保存配置
  - 日志与控制台：实时日志、嵌入 Python 控制台
  - 结果表：批处理结束后显示所有结果项
- 插件管理：
  - 选择插件目录后“加载插件”，动态导入模块并注册处理器
  - “刷新插件表”保持当前勾选状态与排序

示例演示（Word + 绘图流水线）
- 插件：`demos/demo3/plugins/word_plot_pipeline.py`
  - `enter_dir_write_word`：进入目录时在 Word 中写入标题与路径
  - `read_data_files`：读取目录内文件（TXT/CSV/JSON/混合），聚合数值到目录数据桶
  - `plot_on_exit_paste_word`：离开目录时绘图为 PNG 并插入 Word（强制无交互后端；必要时 Pillow 后备）
- 运行演示：
```powershell
python demos/demo3/run_word_plot_demo.py
```

编写你自己的处理器
- 在 `processors/`（内置）或 `plugins/`（外部）目录新建文件，使用 `@processor` 注册。
- 示例骨架：
```python
from pathlib import Path
from decorators.processor import processor

@processor(name="my_proc", priority=50, source=__file__, metadata={"author":"me"})
def my_proc(path: Path, context, **cfg):
    # 读取或更新上下文
    bucket = context.setdefault_data(["folder_data", str(path)], [])
    # 执行业务逻辑并返回结构化结果
    return {"path": str(path), "info": "done"}
```

常见问题（FAQ）
- GUI 闪退或报 `Matplotlib GUI` 警告：
  - 在工作线程中使用交互后端会崩溃；示例插件已改用 Agg（非交互）或 Pillow 后备。
  - 请安装 `Pillow` 以支持 `python-docx` 插图：`pip install Pillow`。
- Word 未插入图片：
  - 检查处理结果中是否有 `error` 字段（例如 `failed to add picture: ...`）。
  - 确认图片路径存在并可读取；必要时设置 `img_dir` 为可写目录。
- 配置规则没有命中：
  - 规则的模式是相对 `root_path` 的路径；确认 `"**/"` 用于目录、`"**/*.ext"` 用于文件。

建议的工具化与复用
- 在 `utils/` 下添加可复用库：
  - `pipeline.py`：封装数据桶读写、文档获取、图片保存（Agg/Pillow 双后端）
  - `io_helpers.py`：安全读取文本/CSV/JSON（带编码与异常兜底）
  - `reporting.py`：统一记录结果到 `context.add_result`

开发与测试
- 快速验证：`test/run_validate_demo.py`
- Pytest 用例：`test/test_validate.py`
- 演示样例：`demos/demo*/` 目录

许可证
- 本仓库包含演示与工具代码。除非另有说明，示例代码可按你的项目自由调整与使用。
