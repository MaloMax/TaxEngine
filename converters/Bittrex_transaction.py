import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

def run(filepaths, progress_callback=None):
    
    CexName = "Bittrex"
    skiprows = 0
    
    paths = con_lib.get_cex_paths(CexName)
    nome_file = Path(__file__).stem
    
    EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")
    
    con_lib.reset_result_file(EventsFile)
    
    dfs = []
    for f in filepaths:
        path = os.path.join(paths["report"], f)
        df_tmp = pd.read_csv(path, skiprows=skiprows, on_bad_lines='skip')
        df_tmp["_file"] = f
        df_tmp["_line"] = range(len(df_tmp))
        dfs.append(df_tmp)
    
    df = pd.concat(dfs, ignore_index=True)
    
    df["timestamp"] = (  pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S.%f", utc=True).astype("int64") // 10**9)
    
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    idx = 0
    total = len(df)
    
    for idx, row in df.iterrows():
        
        if progress_callback:
            progress_callback(idx, total)
        
        qty_amount = con_lib.to_float(row.Amount)
        fee_amount = con_lib.to_float(row.Commission) if pd.notna(row.Commission) else 0.0
        
        if row.Type in ['Withdrawal', 'Transfer'] and fee_amount > 0:
            net_qty = qty_amount
        else:
            net_qty = qty_amount - fee_amount
        
        event = {
            'timestamp': row.timestamp,
            'type': row.Type,
            'asset': row.Currency,
            'qty': net_qty,
            'fee': fee_amount,
            'asset_b': '',
            'qty_b': 0,
            'fee_b': 0,
            'address': row.get('Address', '')
        }
        
        save_row = {
            **event,
            'Exchange':CexName,
            '_line': row._line,
            '_file': row._file
        }
        
        con_lib.append_event_to_csv(EventsFile,save_row )
    
    if progress_callback:
        progress_callback(total, total)
    
    print('Finito')
    return EventsFile

if __name__ == "__main__":
    
    NomeRepFiles = ["Bittrex Transaction History - 2026-01-20_0602.csv"]
    
    run(NomeRepFiles)