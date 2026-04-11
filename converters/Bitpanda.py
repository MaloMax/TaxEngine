import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

def run(filepaths, progress_callback=None):
    
    CexName = "Bitpanda"
    skiprows = 5
    
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
    
    df.columns = [
        'ID', 'Timestamp', 'Type', 'InOut',   'Amt_Fiat', 'Fiat',
        'Amt_Asset', 'Asset',  'Price', 'PriceCurr', 'Class', 'ProdID',
        'Fee', 'FeeAsset', 'FeePct',  'Spread', 'SpreadCurr', 'Tax', 'address'
    ]
    
    idx = 0
    total = len(df)
    
    for idx, row in df.iterrows():
        
        if progress_callback:
            progress_callback(idx, total)
    
        event = {
            'timestamp': con_lib.to_timestamp(row.Timestamp),
            'type': row.Type,
            'asset': row.Asset,
            'qty': con_lib.to_float(row.Amt_Asset),
            'fee': con_lib.to_float(row.Fee),
            'asset_b': row.Fiat,
            'qty_b': con_lib.to_float(row.Amt_Fiat),
            'fee_b': 0.0,
            'address': row.address if pd.notna(row.address) else '-'
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
    
    NomeRepFiles = ["Bitpanda_cex_report.csv"]
    
    run(NomeRepFiles)
