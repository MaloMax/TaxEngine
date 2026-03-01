import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "Kraken_cex_report.csv"
CexName = "Kraken"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

df["timestamp"] = df["time"].apply(con_lib.to_timestamp)
df = df.sort_values("timestamp").reset_index(drop=True)

#txid	refid	time	type	subtype	aclass	subclass	asset	wallet	amount	fee	balance	address

idx = 0

while idx < len(df):

    row = df.iloc[idx]
    
    if row.type == 'trade':
        idx += 1
        row2 = df.iloc[idx]
        
        if row.subclass != 'crypto':
            if row2.subclass != 'crypto':
                raise ValueError(f"linea {idx+3} == Scambio spot Fiat")
            else:
                row, row2 = row2, row
        
        event = {
            'timestamp': row.timestamp,
            'type': 'buy' if row.amount > 0 else 'sell',
            'asset': row.asset,
            'qty': con_lib.to_float(row.amount),
            'fee': con_lib.to_float(row.fee),
            'asset_b': row2.asset,
            'qty_b': con_lib.to_float(row2.amount),
            'fee_b': con_lib.to_float(row2.fee),
            'address': ''
        }
            
    else:
        

        event = {
            'timestamp': row.timestamp,
            'type': row.type,
            'asset': row.asset,
            'qty': con_lib.to_float(row.amount),
            'fee': con_lib.to_float(row.fee),
            'asset_b': '',
            'qty_b': 0,
            'fee_b': 0.0,
            'address': row.address if pd.notna(row.address) else ''
        }

      
    idx += 1  
    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')

