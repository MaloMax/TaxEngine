import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)
from converter_lib import con_lib

def run(filepaths, progress_callback=None):
    
    CexName = "Bybit"
    nome_file = Path(__file__).stem
    
    paths = con_lib.get_cex_paths(CexName)
    EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")
    con_lib.reset_result_file(EventsFile)
    
    dfs = []
    for f in filepaths:
        path = os.path.join(paths["report"], f)
        df_tmp = pd.read_csv(path, skiprows=1, on_bad_lines='skip', keep_default_na=False)
        df_tmp["_file"] = f
        df_tmp["_line"] = range(len(df_tmp))
        dfs.append(df_tmp)
    
    df = pd.concat(dfs, ignore_index=True)
    
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Date": "Time", "Asset": "Currency", "Amount": "CashFlow", "Wallet Balance": "Balance"})
    
    df["timestamp"] = df["Time"].apply(con_lib.to_timestamp)
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    def to_float(val):
        try:
            return float(val)
        except:
            return 0.0
    
    idx = 0
    total = len(df)
    while idx < len(df):
        
        if progress_callback:
            progress_callback(idx, total)
        
        row = df.iloc[idx]    
        typ = str(row.get("Type", "")).strip()
        currency = str(row.get("Currency", "")).strip()
        cashflow = to_float(row.get("CashFlow", 0))
        ts = row.timestamp
        event = None
    
        if typ == "Withdraw":
            event = {
                'timestamp': ts,
                'type': 'withdraw',
                'asset': currency,
                'qty': -cashflow,
                'fee': 0.0,
                'asset_b': '', 'qty_b': 0.0, 'fee_b': 0.0,
                'address': str(row.get("Received Address", "")).strip()
            }
    
        elif typ == "Deposit":
            event = {
                'timestamp': ts,
                'type': 'deposit',
                'asset': currency,
                'qty': cashflow,
                'fee': 0.0,
                'asset_b': '', 'qty_b': 0.0, 'fee_b': 0.0,
                'address': str(row.get("Received Address", "")).strip()
            }
    
        else:
            print(f"TRADE UNKNOWN _line={row['_line']} Type={typ} Currency={currency} cashflow={cashflow} File={row._file}")
    
    
        if event:
            con_lib.append_event_to_csv(EventsFile, {**event, 'Exchange': CexName, '_line': row._line, '_file': row._file})
    
        idx += 1
    
    if progress_callback:
        progress_callback(total, total)
    
    print('Finito')
    return EventsFile

if __name__ == "__main__":
    
    NomeRepFiles = [
        "Bybit_assetHistory_withdrawDepositHistory_154570665_20240101_20241231_0.csv",
        "Bybit_assetHistory_withdrawDepositHistory_154570665_20250101_20251231_0.csv",
    ]
    
    run(NomeRepFiles)