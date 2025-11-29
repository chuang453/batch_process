# exporters.py
# exporters.py
"""
导出与汇总辅助（DataFrame 友好）

作用
- 将处理引擎累积的结果列表（如 context.results 中的字典项）快速转换为 DataFrame（支持 pandas 或 polars）。
- 按文件路径 `file` 自动聚合多条记录，得到更简洁的每文件汇总视图（如最大行数、字数/字符总计、首个语言、状态列表、最后处理时间等）。

典型用法
>>> from utils.exporters import results_to_dataframe, auto_merge_by_file
>>> df = results_to_dataframe(context.results, engine="pandas")
>>> merged = auto_merge_by_file(df, engine="pandas")
>>> merged.to_csv("summary.csv", index=False)

适用场景
- 作为流水线末端步骤：标准化结果并输出汇总报表（CSV/Excel/Markdown 等）。
- 在 GUI 中展示或在测试/验证脚本中做快速统计。
"""
import polars as pl
import pandas as pd
from typing import List, Dict, Union

def results_to_dataframe(
    results: List[Dict], 
    engine: str = "pandas"  # or "polars"
) -> Union[pd.DataFrame, pl.DataFrame]:
    """统一接口：结果转 DataFrame"""
    if engine == "polars":
        return pl.DataFrame(results)
    else:
        return pd.DataFrame(results)

def auto_merge_by_file(df: Union[pd.DataFrame, pl.DataFrame], engine: str = "pandas"):
    """自动合并同文件的多条记录"""
    if engine == "polars":
        return df.group_by("file").agg([
            pl.col("lines").max().alias("lines"),
            pl.col("words").sum().alias("words"),
            pl.col("chars").sum().alias("chars"),
            pl.col("lang").first().alias("lang"),
            pl.col("status").list().alias("statuses"),
            pl.col("timestamp").max().alias("last_processed")
        ])
    else:  # pandas
        return df.groupby("file").agg({
            'lines': 'max',
            'words': 'sum',
            'chars': 'sum',
            'lang': 'first',
            'status': list,
            'timestamp': 'max'
        }).reset_index()