import baostock as bs
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from utils.cleaner import DataCleaner
import os

def worker_task(code):
    lg = bs.login() # 每个子进程独立登录
    # 这里插入你原有的数据采集逻辑
    # 最终保存到 temp_parts/ 目录
    bs.logout()
    return f"Success: {code}"

def run_stock_pipeline():
    os.makedirs("temp_parts", exist_ok=True)
    # 此处添加获取股票列表的逻辑 (例如读取列表文件)
    codes = ["sh.600000", "sz.000001"] 
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(worker_task, codes))
