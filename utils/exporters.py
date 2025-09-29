# exporters.py
import polars as pl
import pandas as pd
from typing import List, Dict

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