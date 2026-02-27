import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "versamenti_Bitso.csv"
CexName = "Bitso"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

df.columns = ['method', 'currency', 'gross', 'fee', 'net_amount', 'timestamp', 'datetime']  

df['row_order'] = range(len(df))
df['Timestamp'] = pd.to_datetime(df['time'], errors='coerce', utc=True)
df_valid = (df.dropna(subset=['Timestamp']).sort_values(['Timestamp', 'row_order']).reset_index(drop=True))

for idx, row in df.iterrows():


    event = {
        'timestamp': con_lib.to_timestamp(row.timestamp),
        'type': 'deposit',
        'asset': row.currency,
        'qty': con_lib.to_float(row.net_amount),
        'fee': 0.0,
        'asset_b': '',
        'qty_b': 0,
        'fee_b': 0,
        'address': row.get('address', '')
    }
    
        
    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')