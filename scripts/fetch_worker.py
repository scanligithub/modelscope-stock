import baostock as bs
import pandas as pd
import os
import time
import datetime
from concurrent.futures import ProcessPoolExecutor
from utils.cleaner import DataCleaner

def fetch_single_stock(code):
    """单进程独立任务，规避 Socket 冲突"""
    # 1. 独立 Login
    bs.login()
    
    start_date = "2005-01-01"
    end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # 2. 获取 K 线
    rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST", 
                                      start_date=start_date, end_date=end_date, frequency="d", adjustflag="3")
    data = []
    while rs.next(): data.append(rs.get_row_data())
    df_k = pd.DataFrame(data, columns=rs.fields) if data else pd.DataFrame()
    
    # 3. 独立 Logout
    bs.logout()
    
    # 4. 清洗
    if not df_k.empty:
        df_k = DataCleaner.clean_stock_kline(df_k)
        return df_k
    return pd.DataFrame()

def run_stock_pipeline():
    # 获取全量股票列表逻辑
    bs.login()
    rs = bs.query_all_stock(day=datetime.datetime.now().strftime("%Y-%m-%d"))
    codes = []
    while rs.next(): codes.append(rs.get_row_data()[0])
    bs.logout()
    
    print(f"Total {len(codes)} stocks to fetch.")
    
    # 限制并发，避免触发云端 IP 风控
    results = []
    with ProcessPoolExecutor(max_workers=8) as executor:
        for res in executor.map(fetch_single_stock, codes):
            if not res.empty:
                results.append(res)
    
    if results:
        final_df = pd.concat(results)
        final_df.to_parquet("temp_parts/stock_kline_all.parquet", index=False)
        print("Stock K-line pipeline finished.")

if __name__ == "__main__":
    run_stock_pipeline()
