import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "MaloMax_trades.csv"
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


for idx, row in df.iterrows():

    asset, asset_b = row.PAIR.split("/")
    asset_fee =  row ['FEE CURRENCY']
    amount = con_lib.to_float(row.AMOUNT)
    fee = con_lib.to_float(row.FEE)
    price = con_lib.to_float(row.PRICE)
    
    event = {
        'timestamp': row.timestamp,
        'type': 'buy' if amount > 0 else 'sell',
        'asset': asset,
        'qty': amount,
        'fee': fee if asset == asset_fee else 0.0 ,
        'asset_b': asset_b,
        'qty_b': -(amount * price),
        'fee_b': fee if asset_b == asset_fee else 0.0 ,
        'address': ''
    }

    save_row = {
        **event,
        'Exchange':CexName,
        'idx': idx+skiprows,
        'File': NomeRepFile
    }
    
    con_lib.append_event_to_csv(EventsFile,save_row )   
    
    if asset != asset_fee and asset_b != asset_fee:
        event = {
            'timestamp': row.timestamp,
            'type': 'realisedpnl',
            'asset': asset_fee,
            'qty': 0.0 ,
            'fee': fee ,
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

        con_lib.append_event_to_csv(EventsFile,save_row )

print('Finito')
