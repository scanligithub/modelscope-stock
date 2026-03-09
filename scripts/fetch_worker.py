import os
import datetime
import requests
import time
import baostock as bs
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_sina_flow(code, start, end):
    symbol = code.replace(".", "")
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=10000&sort=opendate&asc=0&daima={symbol}"
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            data = r.json()
            if not data: return pd.DataFrame()
            df = pd.DataFrame(data)
            rename_map = {
                'opendate': 'date', 'netamount': 'net_amount',
                'r0_net': 'main_net', 'r1_net': 'super_net',
                'r2_net': 'large_net', 'r3_net': 'medium_net',
                'r4_net': 'small_net'
            }
            df.rename(columns=rename_map, inplace=True)
            mask = (df['date'] >= start) & (df['date'] <= end)
            df = df.loc[mask].copy()
            if not df.empty: df['code'] = code
            return df
        except: time.sleep(1)
    return pd.DataFrame()

def process_single_stock(args):
    code, start, end = args
    # 【必须在子进程独立登录 Baostock】
    bs.login()
    
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,isST" 
    try:
        rs = bs.query_history_k_data_plus(code, fields, start_date=start, end_date=end, frequency="d", adjustflag="3")
        k_data = []
        if rs.error_code == '0':
            while rs.next(): k_data.append(rs.get_row_data())
        df_k = pd.DataFrame(k_data, columns=fields.split(",")) if k_data else pd.DataFrame()
        
        if not df_k.empty:
            rs_fac = bs.query_adjust_factor(code, start_date="1990-01-01", end_date="2099-12-31")
            fac_data = []
            if rs_fac.error_code == '0':
                while rs_fac.next(): fac_data.append(rs_fac.get_row_data())
            
            if fac_data:
                df_fac = pd.DataFrame(fac_data, columns=["code", "date", "fore", "back", "ratio"])
                df_fac = df_fac[['date', 'back']].rename(columns={'back': 'adjustFactor'})
                df_k['date'] = pd.to_datetime(df_k['date'])
                df_fac['date'] = pd.to_datetime(df_fac['date'])
                df_fac['adjustFactor'] = pd.to_numeric(df_fac['adjustFactor'], errors='coerce')
                df_k = df_k.sort_values('date')
                df_fac = df_fac.sort_values('date')
                df_k = pd.merge_asof(df_k, df_fac, on='date', direction='backward')
                df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
                df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
            else:
                df_k['adjustFactor'] = 1.0
    except Exception as e:
        df_k = pd.DataFrame()

    try:
        df_f = fetch_sina_flow(code, start, end)
    except:
        df_f = pd.DataFrame()
        
    bs.logout()
    time.sleep(0.1) # 防止频繁请求封禁
    return df_k, df_f

def run_stock_pipeline(year=0):
    future_date = "2099-12-31"
    if year == 9999:
        start, end = "2005-01-01", future_date
    elif year > 0:
        start, end = f"{year}-01-01", f"{year}-12-31"
    else:
        curr_year = datetime.datetime.now().year
        start, end = f"{curr_year}-01-01", future_date

    # 获取股票全量列表
    bs.login()
    d = datetime.datetime.now().strftime("%Y-%m-%d")
    rs = bs.query_all_stock(day=d)
    data = []
    if rs.error_code == '0':
        while rs.next(): data.append(rs.get_row_data())
    bs.logout()

    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.')) and x[2].strip()]
    print(f"Total {len(valid_codes)} stocks to fetch ({start} to {end}).")

    tasks = [(code, start, end) for code in valid_codes]
    res_k, res_f = [], []
    cleaner = DataCleaner()

    os.makedirs("temp_parts", exist_ok=True)
    
    # 采用 ProcessPoolExecutor 并行获取，最大 5 个并发避免断连
    with ProcessPoolExecutor(max_workers=5) as executor:
        for k, f in tqdm(executor.map(process_single_stock, tasks), total=len(tasks)):
            if not k.empty: res_k.append(k)
            if not f.empty: res_f.append(f)

    if res_k: 
        df_k_all = pd.concat(res_k)
        df_k_all = cleaner.clean_stock_kline(df_k_all)
        df_k_all.to_parquet("temp_parts/kline_part_all.parquet", index=False)
    
    if res_f: 
        df_f_all = pd.concat(res_f)
        df_f_all = cleaner.clean_money_flow(df_f_all)
        df_f_all.to_parquet("temp_parts/flow_part_all.parquet", index=False)
