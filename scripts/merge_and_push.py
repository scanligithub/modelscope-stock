import sys
import os
import duckdb
import glob
import datetime
import shutil
import pandas as pd
import baostock as bs

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
    
from utils.ms_manager import MSManager
from utils.qc import QualityControl

# 屏蔽霸屏日志
class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')
    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def get_stock_list_with_names():
    with HiddenPrints():
        bs.login()
    d = datetime.datetime.now().strftime("%Y-%m-%d")
    rs = bs.query_all_stock(day=d)
    data = []
    if rs.error_code == '0':
        while rs.next(): data.append(rs.get_row_data())
    with HiddenPrints():
        bs.logout()
    
    if data:
        df = pd.DataFrame(data, columns=["code", "tradeStatus", "code_name"])
        df = df[df['code_name'].notna() & (df['code_name'].str.strip() != "")]
        df = df[df['code'].str.startswith(('sh.', 'sz.', 'bj.'))]
        return df
    return pd.DataFrame()

def run_merge_and_push(year=0):
    qc = QualityControl()
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET temp_directory='duckdb_temp.tmp'")
    
    k_files = glob.glob("temp_parts/kline_part_*.parquet")
    f_files = glob.glob("temp_parts/flow_part_*.parquet")
    sec_k_files = glob.glob("temp_parts/sector_kline_full.parquet")
    
    if k_files: con.execute(f"CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet({k_files}, union_by_name=True)")
    else: con.execute("CREATE OR REPLACE VIEW v_kline AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")
    
    if f_files: con.execute(f"CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet({f_files}, union_by_name=True)")
    else: con.execute("CREATE OR REPLACE VIEW v_flow AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")
    
    if sec_k_files: con.execute(f"CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet('{sec_k_files[0]}')")
    else: con.execute("CREATE OR REPLACE VIEW v_sec_k AS SELECT * FROM read_parquet([], schema={'date': 'VARCHAR', 'code': 'VARCHAR'})")

    os.makedirs("output", exist_ok=True)
    targets = {}

    df_stocks = get_stock_list_with_names()
    if not df_stocks.empty:
        p = "output/stock_list.parquet"
        df_stocks.to_parquet(p, index=False)
        targets[p] = "stock_list.parquet"
        qc.check_dataframe(df_stocks, "stock_list.parquet", ["code_name"], file_path=p)

    if sec_k_files:
        p = 'output/sector_list.parquet'
        con.execute(f"COPY (SELECT DISTINCT code, name, type FROM v_sec_k ORDER BY type, code) TO '{p}' (FORMAT 'PARQUET')")
        targets[p] = "sector_list.parquet"
        qc.check_dataframe(pd.read_parquet(p), "sector_list.parquet", ["name"], file_path=p)

    if year == 9999: years = range(2005, datetime.datetime.now().year + 1)
    elif year > 0: years = [year]
    else: years = [datetime.datetime.now().year]

    for y in years:
        start_date, end_date = f"{y}-01-01", f"{y}-12-31"
        tasks = [
            ("v_kline", f"stock_kline_{y}.parquet", ["close", "volume"]),
            ("v_flow", f"stock_money_flow_{y}.parquet", ["net_amount"]),
            ("v_sec_k", f"sector_kline_{y}.parquet", ["close"])
        ]
        
        for view_name, out_name, check_cols in tasks:
            out_path = f"output/{out_name}"
            try:
                count = con.execute(f"SELECT count(*) FROM {view_name} WHERE date >= '{start_date}' AND date <= '{end_date}'").fetchone()[0]
                if count == 0: continue
                con.execute(f"COPY (SELECT * FROM {view_name} WHERE date >= '{start_date}' AND date <= '{end_date}' ORDER BY code, date) TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')")
                
                if os.path.exists(out_path):
                    df_check = pd.read_parquet(out_path)
                    qc.check_dataframe(df_check, out_name, check_cols, file_path=out_path)
                    targets[out_path] = out_name
            except Exception as e: print(f"❌ Error processing {out_name}: {e}")

        sec_c_files = glob.glob("temp_parts/sector_constituents_latest.parquet")
        if sec_c_files:
            c_out = f"output/sector_constituents_{y}.parquet"
            shutil.copy(sec_c_files[0], c_out)
            targets[c_out] = f"sector_constituents_{y}.parquet"

    qc.save_report("output/qc_report.json")
    with open("output/qc_summary.md", "w") as f: f.write(qc.get_summary_md())
    targets["output/qc_report.json"] = "qc_report.json"
    targets["output/qc_summary.md"] = "qc_summary.md"

    ms_token = os.getenv("MS_TOKEN")
    ms_dataset_id = os.getenv("MS_DATASET_ID")
    if ms_token and ms_dataset_id:
        ms = MSManager(ms_token, ms_dataset_id)
        for local, remote in targets.items(): 
            ms.upload_file(local, remote)
    
    # 清理中间文件释放空间
    shutil.rmtree("temp_parts", ignore_errors=True)
