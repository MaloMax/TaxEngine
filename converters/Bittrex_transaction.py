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

# Dopo riga 33, aggiungi:
print("\n=== TRANSAZIONI BITTREX ===")
print(df[['Date', 'Currency', 'Type', 'Amount', 'Commission']].to_string())
print(f"\nSaldo finale calcolato (qtà - fee):")
for curr in df['Currency'].unique():
    curr_df = df[df['Currency'] == curr]
    total_qty = curr_df['Amount'].astype(float).sum()
    total_fee = curr_df['Commission'].astype(float).sum()
    net = total_qty - total_fee
    print(f"  {curr}: {total_qty} - {total_fee} = {net}")
    
    
#Date	Currency	Type	Address	Memo/Tag	TxId	Amount  Commission

for idx, row in df.iterrows():
    qty_amount = con_lib.to_float(row.Amount)
    fee_amount = con_lib.to_float(row.Commission) if pd.notna(row.Commission) else 0.0
    
    # Se c'è una commissione e il Type è Withdrawal/Transfer,
    # la fee è già detratta da Amount
    if row.Type in ['Withdrawal', 'Transfer'] and fee_amount > 0:
        net_qty = qty_amount  # già netta
    else:
        net_qty = qty_amount - fee_amount
    
    event = {
        'timestamp': row.timestamp,
        'type': row.Type,
        'asset': row.Currency,
        'qty': net_qty,  # CORRETTO
        'fee': fee_amount,
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