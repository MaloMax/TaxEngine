import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "MaloMax_ledgers.csv"
CexName = "Bitfinex"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')
print(df)


df["timestamp"] = (  pd.to_datetime(df["DATE"], format="%y-%m-%d %H:%M:%S", utc=True).astype("int64") // 10**9)

df = df.sort_values("timestamp").reset_index(drop=True)

print(datetime.utcfromtimestamp(df["timestamp"].min()))
print(datetime.utcfromtimestamp(df["timestamp"].max()))

#	DESCRIPTION	CURRENCY	AMOUNT	BALANCE	DATE	WALLET


for idx, row in df.iterrows():

    if not row.DESCRIPTION.startswith("Settlement"):
        continue
        
    event = {
        'timestamp': row.timestamp,
        'type': 'transfer',
        'asset': row.CURRENCY,
        'qty': row.AMOUNT,
        'fee': 0.0 ,
        'asset_b': '',
        'qty_b': 0.0,
        'fee_b': 0.0 ,
        'address': ''
    }

    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    print(event)
    con_lib.append_event_to_csv(EventsFile,save_row )   
    

print('Finito')
