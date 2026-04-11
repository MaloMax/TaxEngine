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
    skiprows = 4
    
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
    
    df["timestamp"] = (  pd.to_datetime(df["Time (UTC)"], format="%Y-%m-%dT%H:%M:%S", utc=True).astype("int64") // 10**9)
    
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    idx = 0
    total = len(df)
    
    for idx, row in df.iterrows():
        
        if progress_callback:
            progress_callback(idx, total)
        
        feeC = con_lib.to_float(row['Fees (Quote)'])
        
        addFee = feeC if row.Transaction == 'Sold' else -feeC
    
        event = {
            'timestamp': row.timestamp,
            'type': row.Transaction,
            'asset': row.Base,
            'qty': con_lib.to_float(row['Quantity (Base)']),
            'fee': 0.0,
            'asset_b': row.Quote,
            'qty_b': con_lib.to_float(row['Total (Quote)']) + addFee,
            'fee_b': feeC,
            'address': ''
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
    
    NomeRepFiles = ["BittrexOrderHistory_2017.csv", "BittrexOrderHistory_2018.csv"]
    
    run(NomeRepFiles)
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')
