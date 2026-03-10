import sys
import os
import datetime
import requests
import time
import baostock as bs
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from utils.cleaner import DataCleaner

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

# 【核心修复】：屏蔽 Baostock 恶心的霸屏输出
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')
    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def fetch_sina_flow(code, start, end, cf_url):
    symbol = code.replace(".", "")
    if not cf_url:
        return pd.DataFrame()
        
    worker_url = f"https://{cf_url}" if not cf_url.startswith("http") else cf_url
    
    params = {
        "target_func": "sina_flow",
        "page": 1,
        "num": 10000,
        "sort": "opendate",
        "asc": 0,
        "daima": symbol
    }
    
    for attempt in range(3):
        try:
            r = requests.get(worker_url, params=params, headers=HEADERS, timeout=15)
            if r.status_code == 200:
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
            else:
                time.sleep(1)
        except:
            time.sleep(1)
    return pd.DataFrame()

def process_single_stock(args):
    code, start, end, cf_url = args
    df_k, df_f = pd.DataFrame(), pd.DataFrame()
    
    # 【核心修复】：防止日志爆炸
    with HiddenPrints():
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
    except:
        pass

    with HiddenPrints():
        bs.logout()

    try:
        df_f = fetch_sina_flow(code, start, end, cf_url)
    except:
        pass
        
    time.sleep(0.05) # 防止频繁请求封禁
    return df_k, df_f

def run_stock_pipeline(year=0):
    future_date = "2099-12-31"
    if year == 9999: start, end = "2005-01-01", future_date
    elif year > 0: start, end = f"{year}-01-01", f"{year}-12-31"
    else: start, end = f"{datetime.datetime.now().year}-01-01", future_date

    with HiddenPrints():
        bs.login()
        
    d = datetime.datetime.now().strftime("%Y-%m-%d")
    rs = bs.query_all_stock(day=d)
    data = []
    if rs.error_code == '0':
        while rs.next(): data.append(rs.get_row_data())
        
    with HiddenPrints():
        bs.logout()

    valid_codes = [x[0] for x in data if x[0].startswith(('sh.', 'sz.', 'bj.')) and x[2].strip()]
    print(f"Total {len(valid_codes)} stocks to fetch ({start} to {end}).")

    cf_url_global = os.getenv("CF_WORKER_URL", "").strip()
    tasks = [(code, start, end, cf_url_global) for code in valid_codes]
    res_k, res_f = [], []
    cleaner = DataCleaner()

    os.makedirs("temp_parts", exist_ok=True)
    
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
