# 批处理框架能力说明 - 你能干什么？

## 📋 概述

本批处理框架是一个功能强大的文件和目录自动处理系统，支持 GUI 和 CLI 两种使用方式。通过灵活的配置文件（YAML/JSON），您可以轻松定义复杂的批处理任务。

## 🎯 核心能力

### 1. 灵活的文件/目录匹配
- ✅ 支持 glob 模式匹配（`*`, `**`, `?`, `[...]`）
- ✅ 区分文件和目录模式（以 `/` 结尾的是目录模式）
- ✅ 支持多层级递归匹配（`**/` 匹配所有层级的目录）
- ✅ 相对路径匹配，以根目录为基准

**示例模式:**
```yaml
".":           # 只匹配根目录
"**/":         # 匹配所有目录
"**/*.txt":    # 匹配所有 .txt 文件
"data/**/":    # 匹配 data 下所有层级的目录
```

### 2. 处理器系统

#### 三种处理器类型
1. **前处理器 (Pre-processor)** - 在进入路径前执行
   - 用于初始化、准备工作
   - 例如：创建 Word 文档、设置上下文数据

2. **文件/目录处理器 (File/Dir Processor)** - 处理具体路径
   - 用于核心业务逻辑
   - 例如：读取数据、转换格式、备份文件

3. **后处理器 (Post-processor)** - 在离开路径时执行
   - 用于汇总、清理工作
   - 例如：生成图表、保存报告、统计总结

#### 处理器特性
- ✅ 优先级控制（priority 值越大越先执行）
- ✅ 可配置参数传递（通过 config 字段）
- ✅ 上下文数据共享（ProcessingContext）
- ✅ 结构化结果记录
- ✅ 错误重试机制（使用 @retry 装饰器）

### 3. 配置系统

#### 配置文件结构
```yaml
# 全局前处理
pre_process:
  - processor_name
config_pre:
  key: value

# 模式规则
"pattern":
  pre_processors:
    - pre_proc_1
  processors:
    - main_proc_1
    - main_proc_2
  post_processors:
    - post_proc_1
  config:
    param1: value1
    param2: value2
  priority: 100

# 全局后处理
post_process:
  - processor_name
config_post:
  key: value
```

#### 配置特性
- ✅ YAML 和 JSON 双格式支持
- ✅ 模板生成功能（`--generate-template`）
- ✅ 动态加载插件
- ✅ 配置热重载（GUI 支持）

### 4. 上下文管理 (ProcessingContext)

处理器之间可以通过上下文对象共享数据：

```python
# 设置/获取本地数据（按路径隔离）
context.set_data(keys, value)
context.get_data(keys, default)
context.setdefault_data(keys, default)

# 设置/获取共享数据（全局可见）
context.set_shared(keys, value)
context.get_shared(keys, default)
context.setdefault_shared(keys, default)

# 记录结果
context.add_result({"status": "ok", "message": "处理成功"})

# 元数据
context.set_metadata(keys, value)
context.get_metadata(keys, default)
```

### 5. 内置处理器

#### 文件操作类
- **backup_file** - 备份文件到指定目录
- **backup_file1** - 备份文件为 .bak 扩展名
- **rename_file** - 重命名文件
- **delete_file** - 删除文件
- **download_file** - 下载文件

#### 数据处理类
- **count_lines** - 统计文本文件行数
- **set_path_name_dict** - 构建文件名字典

#### 记录和持久化类
- **record_to_shared** - 记录执行历史到内存
- **persist_history_sqlite** - 持久化历史到 SQLite 数据库
- **persist_history_jsonl** - 持久化历史到 JSONL 文件

#### 可视化类
- **plot_from_spec** - 根据配置生成图表
- **prepare_plot_data** - 准备绘图数据
- **write_plot_extract_summary** - 写入绘图摘要

#### 报告类
- **generate_summary** - 生成处理总结报告

### 6. 插件系统

#### 插件开发
```python
from pathlib import Path
from decorators.processor import processor, pre_processor, post_processor

@processor(
    name="my_processor",
    priority=50,
    source=__file__,
    metadata={
        "author": "Your Name",
        "version": "1.0",
        "description": "处理器描述"
    }
)
def my_processor(path: Path, context, **kwargs):
    # 您的处理逻辑
    return {"status": "ok", "message": "完成"}

@pre_processor(name="my_pre", priority=60)
def my_pre(path: Path, context, **kwargs):
    # 前处理逻辑
    pass

@post_processor(name="my_post", priority=40)
def my_post(path: Path, context, **kwargs):
    # 后处理逻辑
    pass
```

#### 插件特性
- ✅ 动态加载（无需重启）
- ✅ 独立命名空间
- ✅ 元数据支持（作者、版本、描述等）
- ✅ GUI 插件管理界面

### 7. GUI 功能

#### 主要界面功能
- 📝 配置文件编辑器（支持语法高亮）
- 📂 路径选择器（配置、目标目录、插件目录）
- 🔌 插件管理器（加载、启用/禁用、信息显示）
- 📋 执行预览（显示执行顺序和计划）
- ▶️ 批处理执行器（实时进度显示）
- 📊 结果查看器（表格显示处理结果）
- 📝 日志查看器（实时日志输出）
- 💻 Python 控制台（内嵌交互式控制台）

#### GUI 特性
- ✅ 非模态预览窗口
- ✅ 实时执行状态更新
- ✅ 层级折叠控制
- ✅ 可排序表格
- ✅ 错误详情显示
- ✅ 树形路径显示

### 8. CLI 功能

#### 命令行选项
```bash
# 查看系统能力
python -m cli.app --capabilities

# 列出所有处理器
python -m cli.app --processors

# 生成配置模板
python -m cli.app --generate-template config.yaml

# 运行批处理
python -m cli.app <目录路径> -c config.yaml

# 查看帮助
python -m cli.app --help
```

### 9. 辅助工具库

#### Pipeline 工具 (`utils/pipeline.py`)
- `get_bucket()` - 获取或创建数据桶
- `append_numbers()` - 追加数值到桶
- `set_output()` / `get_output()` - 记录/读取输出路径
- `record_result()` - 统一记录处理结果

#### IO 工具 (`utils/io_helpers.py`)
- `safe_read_text()` - 安全读取文本文件
- `safe_read_json()` - 安全读取 JSON 文件
- `csv_values()` - 从 CSV 提取数值列

#### Word 适配器 (`utils/adapters/docx_helpers.py`)
- `get_or_create_doc()` - 获取或创建 Word 文档
- 支持 python-docx 库

#### 绘图适配器 (`utils/adapters/plot_helpers.py`)
- `save_plot_png_values()` - 生成 PNG 图表
- 支持 Matplotlib（非交互后端）

### 10. 数据持久化

#### SQLite 存储
- 文件位置: `<log_dir>/processed_history.db`
  - `<log_dir>` 默认为 `debug_logs` 目录（可在配置或处理器参数中自定义）
  - 如果目录不存在，会自动创建
- 表结构: processed_history
  - id, ts, path, processor, phase, status, cfg, result, error, raw

#### JSONL 存储
- 逐行 JSON 格式
- 易于追加和解析
- 适合大规模数据

#### 查询历史
```python
from processors.builtin_recorders import read_history_rows

rows = read_history_rows('debug_logs', limit=100)
for r in rows:
    print(r['ts'], r['processor'], r['status'])
```

### 11. 执行模拟

#### 预览功能
- ✅ 不实际执行，只显示执行计划
- ✅ 显示匹配的路径和处理器
- ✅ 按执行顺序排列
- ✅ 显示配置参数
- ✅ 计算预计执行步骤数

```python
from core.engine import BatchProcessor

processor = BatchProcessor(config, available_processors)
steps = processor.simulate(root_path, sequence=True)
for step in steps:
    print(step['Path'], step['Processor'], step['Phase'])
```

## 🚀 使用场景

### 1. 数据收集和汇总
- 扫描目录树，读取各类数据文件（TXT、CSV、JSON）
- 按文件夹汇总数值
- 生成统计报告和图表

### 2. 文件批处理
- 批量重命名文件
- 批量备份/删除文件
- 批量格式转换

### 3. 报告生成
- 读取数据文件
- 生成图表（PNG）
- 插入到 Word 文档
- 生成 PDF 报告

### 4. 代码分析
- 统计代码行数
- 检查代码质量
- 生成分析报告

### 5. 数据验证
- 验证文件格式
- 检查数据完整性
- 生成验证报告

## 📚 学习资源

### 文档
- **README.md** - 完整使用指南
- **demos/** - 示例演示
  - demo/ - 基础示例
  - demo_advanced/ - 高级示例
  - demo_complex/ - 复杂场景示例

### 测试
- **test/test_validate.py** - 单元测试
- **test/run_validate_demo.py** - 快速验证脚本

### 示例插件
- **plugins/** - 外部插件示例
- **processors/** - 内置处理器实现

## 💡 最佳实践

### 1. 配置组织
- 使用清晰的模式命名
- 合理设置优先级
- 充分利用 config 参数传递

### 2. 处理器设计
- 保持处理器职责单一
- 使用上下文传递数据
- 记录结构化结果
- 添加错误处理和重试

### 3. 性能优化
- 避免重复处理
- 使用数据桶聚合
- 考虑使用异步持久化

### 4. 调试技巧
- 使用 --capabilities 查看处理器
- 使用模拟模式预览执行计划
- 查看 SQLite 历史记录
- 使用 GUI 控制台交互调试

## 🔧 扩展开发

### 添加新处理器
1. 在 `processors/` 或 `plugins/` 创建 Python 文件
2. 使用 `@processor` 装饰器注册
3. 实现处理逻辑
4. 添加元数据（描述、作者、版本）
5. 在配置中引用

### 添加新工具
1. 在 `utils/` 创建工具模块
2. 导出常用函数
3. 在处理器中导入使用

### 添加新适配器
1. 在 `utils/adapters/` 创建适配器
2. 封装第三方库调用
3. 提供简洁 API

## 🎓 总结

本批处理框架能够:
- ✅ 灵活匹配和处理文件/目录
- ✅ 支持复杂的处理流程编排
- ✅ 提供丰富的内置处理器
- ✅ 支持插件扩展
- ✅ 提供友好的 GUI 和 CLI
- ✅ 记录和持久化处理结果
- ✅ 支持数据汇总和可视化

**简而言之：这是一个全功能的文件批处理自动化框架，可以帮助您高效完成各种文件和目录的自动化处理任务！**
