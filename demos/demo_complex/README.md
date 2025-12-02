# 复杂演示 - Complex Demo

这个演示展示了批处理框架的高级功能，包括多层文件夹结构、三类数据处理、Word报告生成和数据可视化。

## 功能特性

1. **多层文件夹结构**: 根目录下有多个文件夹，每个文件夹包含子文件夹，子文件夹内有数据文件
2. **三类数据处理**:
   - Type1: 主要数据（在Word表格中占据2个单元格）
   - Type2: 辅助数据1（占据1个单元格）
   - Type3: 辅助数据2（占据1个单元格）
3. **文件夹标签**: 每个文件夹都有对应的标签名称，进入时写入Word文档
4. **数据写入Word**: 每个文件的数据以表格形式写入Word文档
5. **个别绘图**: 为每个文件创建数据可视化图表
6. **文件夹总结**: 离开文件夹时，写入总结性文字并绘制综合图表（包含该文件夹内所有数据）

## 目录结构

```
demo_complex/
├── data_root/                    # 数据根目录
│   ├── folder_A/                 # 实验组A - 温度控制实验
│   │   ├── subfolder_A1/         # 子组A1 - 高温条件
│   │   │   ├── experiment_1.txt
│   │   │   └── experiment_2.txt
│   │   └── subfolder_A2/         # 子组A2 - 低温条件
│   │       └── measurement_1.txt
│   ├── folder_B/                 # 实验组B - 电力测试实验
│   │   ├── subfolder_B1/         # 子组B1 - 标准电压
│   │   │   └── test_1.txt
│   │   └── subfolder_B2/         # 子组B2 - 变压测试
│   │       └── test_2.txt
│   └── folder_C/                 # 实验组C - 化学分析实验
│       └── subfolder_C1/         # 子组C1 - 酸碱度测试
│           └── sample_1.txt
├── plugins/
│   └── complex_demo_processor.py # 自定义处理器插件
├── complex_config.yaml           # 配置文件
├── run_complex_demo.py           # 运行脚本
└── README.md                     # 本文件
```

## 数据文件格式

每个数据文件包含三类数据，格式如下：

```
# 文件注释
# Type1: 主要数据 (描述)
TYPE1: 值1, 值2, 值3, ...
# Type2: 辅助数据1 (描述)
TYPE2: 值1, 值2, 值3, ...
# Type3: 辅助数据2 (描述)
TYPE3: 值1, 值2, 值3, ...
```

示例：
```
# 实验数据文件 - Experiment 1
# Type1: 主要数据 (温度测量)
TYPE1: 25.3, 26.1, 25.8, 26.5, 27.2, 26.8, 27.5, 28.1
# Type2: 辅助数据1 (湿度)
TYPE2: 45, 47, 46, 48, 50
# Type3: 辅助数据2 (压力)
TYPE3: 101.3, 101.5, 101.4
```

## 依赖安装

```bash
pip install matplotlib python-docx Pillow
```

## 运行演示

### 方法1: 使用运行脚本

```bash
python demos/demo_complex/run_complex_demo.py
```

### 方法2: 使用主程序

```bash
# 从GUI运行
python main.py

# 从CLI运行
python -m batch_processor.cli demos/demo_complex/data_root -c demos/demo_complex/complex_config.yaml
```

## 处理流程

1. **进入根目录** (`data_root/`): 写入目录标签到Word
2. **进入folder_A**: 写入"实验组A - 温度控制实验"标签
3. **进入subfolder_A1**: 写入"子组A1 - 高温条件"标签
4. **处理experiment_1.txt**:
   - 解析三类数据
   - 创建表格（Type1占2列，Type2和Type3各占1列）
   - 生成个别数据图表
   - 插入到Word文档
5. **处理experiment_2.txt**: 同上
6. **离开subfolder_A1**:
   - 写入总结文字（文件数、数据点数、统计信息）
   - 生成综合图表（包含该子文件夹内所有数据）
   - 插入到Word文档
7. 对其他子文件夹和文件夹重复上述过程

## 输出文件

- `output_complex.docx`: 生成的Word文档报告
- `images/`: 包含所有生成的图表
  - `plot_*.png`: 个别文件的数据图表
  - `summary_*.png`: 文件夹的综合数据图表

## 自定义处理器

本演示使用了三个自定义处理器：

### 1. `enter_folder_label`
- **类型**: pre_processor
- **优先级**: 90
- **功能**: 进入目录时写入文件夹标签到Word文档

### 2. `read_three_type_data`
- **类型**: processor
- **优先级**: 70
- **功能**: 读取三类数据，创建表格和个别图表

### 3. `exit_folder_summary`
- **类型**: post_processor
- **优先级**: 60
- **功能**: 离开目录时写入总结和综合图表

## 配置说明

配置文件 `complex_config.yaml` 定义了处理规则：

```yaml
# 根目录
".":
  pre_processors:
    - enter_folder_label

# 所有子目录
"**/":
  pre_processors:
    - enter_folder_label
  post_processors:
    - exit_folder_summary

# 所有文本文件
"**/*.txt":
  processors:
    - read_three_type_data
```

## 扩展说明

你可以根据需要自定义：

1. **文件夹标签**: 修改 `complex_demo_processor.py` 中的 `FOLDER_LABELS` 字典
2. **数据格式**: 修改 `parse_three_type_data()` 函数以支持不同的数据格式
3. **表格样式**: 在 `read_three_type_data()` 中修改表格创建代码
4. **图表样式**: 修改绘图参数（颜色、标记、线型等）
5. **配置参数**: 在配置文件中调整 `doc_path`、`img_dir`、`fig_width`、`fig_height`、`dpi` 等参数

## 注意事项

- 确保安装了所有依赖包
- 运行前确保没有其他程序占用输出文件
- 图片文件会保存在 `images/` 目录中
- 如遇到闪退问题，确保使用非交互式后端（代码已设置 `MPLBACKEND=Agg`）
