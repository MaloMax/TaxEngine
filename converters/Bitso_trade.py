import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "trading_Bitso.csv"
CexName = "Bitso"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')
  
df.columns = ['type', 'major', 'minor', 'amount', 'rate', 'value', 'fee', 'total', 'timestamp', 'datetime']  

for idx, row in df.iterrows():
          
    event = {
        'timestamp': con_lib.to_timestamp(row.timestamp),
        'type': row.type,
        'asset': row.major,
        'qty': con_lib.to_float(row.amount),
        'fee': con_lib.to_float(row.fee) if row.type == 'buy' else 0.0,
        'asset_b': row.minor,
        'qty_b': con_lib.to_float(row.value),
        'fee_b': con_lib.to_float(row.fee) if row.type == 'sell' else 0.0,
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