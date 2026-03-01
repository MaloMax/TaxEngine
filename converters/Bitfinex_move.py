import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "MaloMax_movements.csv"
CexName = "Bitfinex"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')
print(df)

df["timestamp"] = (  pd.to_datetime(df["STARTED"], format="%y-%m-%d %H:%M:%S", utc=True).astype("int64") // 10**9)

df = df.sort_values("timestamp").reset_index(drop=True)

print(datetime.utcfromtimestamp(df["timestamp"].min()))
print(datetime.utcfromtimestamp(df["timestamp"].max()))

for idx, row in df.iterrows():
    
    if row.STATUS == 'CANCELED':
        continue

    AMOUNT = con_lib.to_float(row.AMOUNT)
    FEES = con_lib.to_float(row.FEES)
    STARTED = int(datetime.strptime(row.STARTED, "%y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    UPDATED = int(datetime.strptime(row.UPDATED, "%y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    
    event = {
        'timestamp': STARTED if AMOUNT > 0 else UPDATED,
        'type': 'DEPOSIT' if AMOUNT > 0 else 'WITHDRAWAL',
        'asset': row.CURRENCY,
        'qty': AMOUNT,
        'fee': abs(FEES),
        'asset_b': '',
        'qty_b': 0,
        'fee_b': 0,
        'address': row.get('DESCRIPTION', '')
    }
    
        
    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')