# Stage 2 Simple Demo

这个 demo 用最小配置演示 Stage 2 的完整流程：

1. 从 CSV 读取一个输入 DataFrame
2. 执行一条 series（清洗 + 派生列）
3. 输出 manifest 与结果预览

## 目录结构

- `project.yaml`: Stage 2 项目配置
- `input/orders.csv`: 示例输入数据
- `run_demo.py`: API 方式运行示例

## 运行方式

在仓库根目录执行：

```powershell
python -m stage2_platform.cli.app validate demos/stage2_simple/project.yaml
python -m stage2_platform.cli.app simulate demos/stage2_simple/project.yaml
python -m stage2_platform.cli.app run demos/stage2_simple/project.yaml
```

或执行 API demo：

```powershell
python demos/stage2_simple/run_demo.py
```
