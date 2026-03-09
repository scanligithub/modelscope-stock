import pandas as pd
import numpy as np

class DataCleaner:
    @staticmethod
    def clean_stock_kline(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        # 强制类型转换，节省内存
        f32_cols = ['open', 'high', 'low', 'close', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor']
        for c in f32_cols:
            if c in df.columns: 
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
            
        f64_cols = ['volume', 'amount']
        for c in f64_cols:
            if c in df.columns: 
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('float64')

        if 'isST' in df.columns:
            df['isST'] = pd.to_numeric(df['isST'], errors='coerce').fillna(0).astype('int8')

        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates(subset=['date', 'code'], keep='last')
        return df.sort_values(['code', 'date'])

    @staticmethod
    def clean_money_flow(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        target_cols = ['date', 'code', 'net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']
        
        for col in target_cols:
            if col not in df.columns: df[col] = 0.0
        
        df = df[target_cols].copy()
        for col in ['net_amount', 'main_net', 'super_net', 'large_net', 'medium_net', 'small_net']:
            df[col] = (pd.to_numeric(df[col], errors='coerce').fillna(0.0) / 10000.0).astype('float32')
            
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        return df.drop_duplicates(subset=['date', 'code'], keep='last').sort_values(['code', 'date'])
