# Stage 2 Platform 文档

## 1. 定位与设计目标

Stage 2 是独立的数据处理平台层，面向 DataFrame 工作流。

- Stage 1（walk）负责文件系统递归、规则匹配与副作用处理（复制、重命名、提取等）。
- Stage 2 负责结构化数据编排（输入加载、series 执行、结果交付、运行记录）。

核心目标：

1. 将目录遍历逻辑与数据编排逻辑彻底解耦。
2. 让 Stage 2 能脱离 Stage 1 独立运行（CLI/UI/API）。
3. 保留桥接能力，让 Stage 1 产物可直接输入 Stage 2。

---

## 2. 核心原理

Stage 2 采用 project -> stage -> series -> step 的分层执行模型：

1. project：完整任务，包含 inputs 与 stages。
2. stage：逻辑分组，内部可包含多条 series。
3. series：最小执行单元，定义 input_key/output_key 与步骤序列。
4. step：单步转换，支持 builtin op、transform、group_by 嵌套。

执行主流程：

1. 归一化 project（补全默认字段，兼容旧配置）。
2. 加载 inputs 到 catalog。
3. 逐 stage 执行；每个 stage 内逐 series 执行。
4. 每个 series 内按 step 顺序执行，输出写回 catalog。
5. 汇总 RunManifest 与 SeriesManifest。

取消语义：

- run 过程中可 cancel；未执行的 series 会标记为 skipped/cancelled。

错误语义：

- 单 series 失败可由 continue_on_error 控制是否继续后续 series。

---

## 3. 架构与模块

### 3.1 关键包

- stage2_platform/contracts：运行时契约对象（StageSpec、SeriesSpec、RunManifest 等）。
- stage2_platform/config：project 读取、归一化、校验。
- stage2_platform/ingestion：输入适配器（file/memory/sql/api/stage1_artifact）。
- stage2_platform/execution：执行上下文、series/stage/project 执行器。
- stage2_platform/delivery：输出落盘（文件、bundle 等）。
- stage2_platform/registry：run journal / lineage 跟踪。
- stage2_platform/api：Stage2Service 门面 API。
- stage2_platform/cli：命令行入口。
- stage2_platform/ui：独立工作台（Project / Run / Outputs）。

### 3.2 关键执行组件

- ProjectRunner：project 级执行与 RunManifest 汇总。
- StageOrchestrator：stage 内多 series 编排。
- SeriesExecutor：单 series 步骤执行。
- Stage2Context：运行上下文，持有 catalog、manifest 等。

### 3.3 输入加载模型

source_type 由 adapter 负责加载，当前支持：

1. file
2. memory
3. stage1_artifact
4. sql
5. api

---

## 4. 配置模型

最小 project 结构：

```yaml
name: demo_project
inputs:
  - name: raw
    source_type: file
    source_params:
      path: ./demo.csv
stages:
  - name: clean
    type: data
    source: raw
    series:
      - name: keep_top
        input_key: raw
        output_key: raw_top
        enabled: true
        continue_on_error: true
        output_policy: overwrite
        steps:
          - dropna:
              subset: [a]
          - head: 10
```

兼容性：

- 若 stage 未提供 series，系统会把 stage-level steps 归一化为一个 default series。

校验规则（重点）：

1. project.name 必填。
2. inputs[*].source_type 必须是已注册 adapter。
3. series 内 output_key 在同 stage 内不可重复（非空时）。
4. step 必须是合法 builtin op / transform / group_by 结构。

---

## 5. 执行语义

### 5.1 Step 类型

1. builtin op：如 head/tail/dropna/select/sort/fillna/eval/astype/drop 等。
2. transform chain：`run: [transform_a, transform_b]`。
3. group_by：带子步骤的分组递归执行。

### 5.2 输出语义

- series 输出写入 catalog 的 output_key（为空时回写 input_key）。
- run 结束产出 RunManifest：
  - status: done / partial / cancelled
  - series_records: 每条 series 的状态、错误信息与输入输出键。

---

## 6. 用法

### 6.1 CLI

入口模块：[stage2_platform/cli/app.py](../stage2_platform/cli/app.py)

常用命令：

```powershell
python -m stage2_platform.cli.app validate .\project.yaml
python -m stage2_platform.cli.app simulate .\project.yaml
python -m stage2_platform.cli.app run .\project.yaml
python -m stage2_platform.cli.app list-ops
```

说明：

- validate 返回 ok/errors。
- simulate 返回归一化后的计划结构。
- run 返回 manifest JSON。

### 6.2 API

入口类：[stage2_platform/api/service.py](../stage2_platform/api/service.py)

典型流程：

```python
from stage2_platform.api import Stage2Service

svc = Stage2Service()
svc.load_project("project.yaml")

errors = svc.validate_project()
if errors:
    raise ValueError(errors)

sim = svc.simulate()
manifest = svc.run_project()
```

### 6.3 UI 工作台

入口窗口：[stage2_platform/ui/workspace_window.py](../stage2_platform/ui/workspace_window.py)

三页签职责：

1. Project：编辑 inputs/stages/series/steps。
2. Run：执行、查看实时日志与模拟结果。
3. Outputs：查看输出目录、预览数据、导出 csv/parquet。

### 6.4 Demo

仓库内置两个可直接运行的 Stage 2 示例：

1. `demos/stage2_simple/`
2. `demos/stage2_complex/`

建议先跑 simple，再看 complex。

---

## 7. 与 Stage 1 的桥接

桥接目标：把 Stage 1 的 context.main 中 DataFrame 与 pipeline data stage 配置转换成 Stage 2 project。

相关实现：

1. [main_window.py](../main_window.py) 中 `_build_stage2_bridge_project()`
2. [stage1_bridge.py](../stage1_bridge.py) 的 artifact 导出

当前桥接方式：

1. memory bridge：直接把 DataFrame 作为 memory input 注入 Stage 2 工作台。
2. artifact bridge：导出 Stage 1 artifact，再用 `stage1_artifact` adapter 加载。

建议：

- 长期保存/跨进程共享场景优先使用 artifact bridge。
- 临时交互分析可使用 memory bridge。

---

## 8. 常见问题

1. 为什么保存 project 时报错 memory DataFrame 无法序列化？
原因：DataFrame 是内存对象，不能直接安全写入 YAML/JSON。请先导出为 artifact/file 输入。

2. run 结果是 partial 代表什么？
表示至少有一条 series 失败，但整个 run 没有被取消。

3. 如何查看支持的操作？
使用 `list-ops` 命令查看 builtin_ops 与 transform 注册列表。

---

## 9. 代码入口索引

1. 配置归一化：[stage2_platform/config/normalizer.py](../stage2_platform/config/normalizer.py)
2. 配置校验：[stage2_platform/config/validator.py](../stage2_platform/config/validator.py)
3. 项目执行：[stage2_platform/execution/project_runner.py](../stage2_platform/execution/project_runner.py)
4. 阶段编排：[stage2_platform/execution/stage_orchestrator.py](../stage2_platform/execution/stage_orchestrator.py)
5. API 门面：[stage2_platform/api/service.py](../stage2_platform/api/service.py)
6. CLI 入口：[stage2_platform/cli/app.py](../stage2_platform/cli/app.py)
7. UI 工作台：[stage2_platform/ui/workspace_window.py](../stage2_platform/ui/workspace_window.py)