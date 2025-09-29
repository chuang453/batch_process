#
from core.engine import BatchProcessor
from config.loader import load_config, load_plugins
#from utils.exporters import results_to_dataframe, auto_merge_by_file
from decorators.processor import PROCESSORS
from processors import *       ##导入内置处理函数

def run_pipeline(root: str, config_path: str, merge=True, engine="pandas"):
    config = load_config(config_path)
    load_plugins()
    print(f"📦 已加载的处理器：{PROCESSORS}")
    bp = BatchProcessor(config, PROCESSORS)
    context = bp.run(root)

#    df = results_to_dataframe(context.results, engine=engine)
#    if merge and "file" in df.columns:
#        df = auto_merge_by_file(df, engine=engine)
    
    return context

if __name__ == "__main__":
    ctx = run_pipeline("./data", "config/default.yaml")
#    print(df.head())
#    df.write_csv("output/report.csv")  # polars 写法
    
    