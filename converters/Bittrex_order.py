import sys
import os
import pandas as pd
from pathlib import Path
import glob
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "BittrexOrderHistory_2017.csv"
CexName = "Bittrex"
skiprows = 4

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")

con_lib.reset_result_file(EventsFile)


# carica piu file 

pattern = os.path.join(paths["report"], "BittrexOrderHistory_*.csv")

files = glob.glob(pattern)

print("File trovati:")
for f in files:
    print(f)

df_list = []
for file in files:
    df_sing = pd.read_csv(file, skiprows=skiprows, on_bad_lines='skip')
    df_sing['sourceFile'] = Path(file).stem
    df_list.append(df_sing)

df = pd.concat(df_list, ignore_index=True)

#df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

#TXID	Time (UTC)	Transaction	Order Type	Market	Base	Quote	Price	Quantity (Base)	Fees (Quote)	Total (Quote)	Approx Value (USD)	Time In Force	Notes
       
df["timestamp"] = (  pd.to_datetime(df["Time (UTC)"], format="%Y-%m-%dT%H:%M:%S", utc=True).astype("int64") // 10**9)

df = df.sort_values("timestamp").reset_index(drop=True)

print(datetime.utcfromtimestamp(df["timestamp"].min()))
print(datetime.utcfromtimestamp(df["timestamp"].max()))

for idx, row in df.iterrows():
    
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
        'fee_b': con_lib.to_float(row['Fees (Quote)']),
        'address': ''
    }

    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': row.sourceFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')
