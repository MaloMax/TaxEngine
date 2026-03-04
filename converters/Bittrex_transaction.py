import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "Bittrex Transaction History - 2026-01-20_0602.csv"
CexName = "Bittrex"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

       
df["timestamp"] = (  pd.to_datetime(df["Date"], format="%Y-%m-%d %H:%M:%S.%f", utc=True).astype("int64") // 10**9)

df = df.sort_values("timestamp").reset_index(drop=True)

print(datetime.utcfromtimestamp(df["timestamp"].min()))
print(datetime.utcfromtimestamp(df["timestamp"].max()))

#Date	Currency	Type	Address	Memo/Tag	TxId	Amount  Commission


for idx, row in df.iterrows():


    event = {
        'timestamp': row.timestamp,
        'type': row.Type,
        'asset': row.Currency,
        'qty': con_lib.to_float(row.Amount),
        'fee': con_lib.to_float(row.Commission),
        'asset_b': '',
        'qty_b': 0,
        'fee_b': 0,
        'address': row.get('Address', '')
    }
    
        
    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')