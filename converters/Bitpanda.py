import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "Bitpanda_cex_report.csv"
CexName = "Bitpanda"
skiprows = 5

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

        
df.columns = [
    'ID', 'Timestamp', 'Type', 'InOut',   'Amt_Fiat', 'Fiat',
    'Amt_Asset', 'Asset',  'Price', 'PriceCurr', 'Class', 'ProdID',
    'Fee', 'FeeAsset', 'FeePct',  'Spread', 'SpreadCurr', 'Tax', 'address'
]

for idx, row in df.iterrows():

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
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')
