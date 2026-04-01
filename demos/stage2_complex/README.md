# Stage 2 Complex Demo

这个 demo 展示较完整的 Stage 2 编排能力：

1. 多输入（orders + returns）
2. 多 stage（清洗、分析）
3. 每个 stage 多 series
4. `group_by + collect` 的嵌套步骤

## 目录结构

- `project.yaml`: 复杂项目配置
- `input/orders.csv`: 订单数据
- `input/returns.csv`: 退货数据
- `run_demo.py`: API 运行示例（含输出导出）

## 运行方式

在仓库根目录执行：

```powershell
python -m stage2_platform.cli.app validate demos/stage2_complex/project.yaml
python -m stage2_platform.cli.app simulate demos/stage2_complex/project.yaml
python -m stage2_platform.cli.app run demos/stage2_complex/project.yaml
```

或运行 API demo：

```powershell
python demos/stage2_complex/run_demo.py
```
