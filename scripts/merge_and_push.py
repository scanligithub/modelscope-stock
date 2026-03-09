import duckdb
import os
from utils.ms_manager import MSManager

def run_merge_and_push():
    con = duckdb.connect()
    # 执行 DuckDB 合并逻辑...
    
    # 推送到 ModelScope
    manager = MSManager(os.getenv("MS_TOKEN"), os.getenv("MS_DATASET_ID"))
    manager.upload_folder("output/", "data/")
