import sys
import os
import pandas as pd
from pathlib import Path


BITMEX_ASSET_MAP = {
    'XBt':  'BTC',
    'Gwei': 'ETH',
    'USDt': 'USDT',
    'USDc': 'USDC',
    'DOGe': 'DOGE',
    'BMEx': 'BMEX',
    'POl': 'POL',
    'LAMp': 'SOL',
}


BITMEX_MULTIPLIER = {
    'XBt':  100_000_000,
    'Gwei': 1_000_000_000,
    'USDt': 1_000_000,
    'USDc': 1_000_000,
    'DOGe': 1_000_000,
    'BMEx': 1_000_000,
    'POl': 1_000_000,
    'LAMp': 1_000_000_000,
}

def normalize_bitmex_asset(asset, qty, qty2):
    """
    Converte asset BitMEX in:
    - nome standard
    - quantità reale
    """

    if asset not in BITMEX_ASSET_MAP:
        raise ValueError(f"Asset BitMEX non gestito: {asset}")

    real_asset = BITMEX_ASSET_MAP[asset]
    multiplier = BITMEX_MULTIPLIER[asset]
    
    
    qty_real = float(qty) / multiplier if qty not in (None, '') else 0
    qty2_real = float(qty2) / multiplier if qty2 not in (None, '') else 0

    return real_asset, qty_real, qty2_real



LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

NomeRepFile = "transaction history 2287190.csv"
CexName = "Bitmex"
skiprows = 0

paths = con_lib.get_cex_paths(CexName)
nome_file = Path(__file__).stem

ReportFile = os.path.join(paths["report"], NomeRepFile)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")


con_lib.reset_result_file(EventsFile)

df = pd.read_csv(ReportFile, skiprows=skiprows, on_bad_lines='skip')

df["timestamp"] = df["transactTime"].apply(con_lib.to_timestamp)
df = df.sort_values("timestamp").reset_index(drop=True)

my_wallet_ids = {"2287190", "233348", "2238926", "2217244", "2189738"}
#currency	transactType	transactTime	amount	fee	address	text	walletBalance	transactID	transactStatus	tx	account


idx = 0

while idx < len(df):

    row = df.iloc[idx]
        
        
    if row.transactType == 'Transfer':
        if str(row.address) in my_wallet_ids:
            idx += 1  
            continue  # transfer interno, ignoralo
    
    if row.transactType == 'Transfer' and row.currency != 'BMEx':
        print(row.currency, row.amount, row.address,  row.account,  row.transactTime)
    
    if row.transactType == 'SpotTrade' or row.transactType == 'Conversion' or row.transactType == 'AutoConversion':
        idx += 1
        row2 = df.iloc[idx]
        
        
        if con_lib.isTax(row.currency):
            if con_lib.isTax(row2.currency):
                if row.currency.upper() == 'EUR' or con_lib.to_float(row.amount) > 0:
                    row, row2 = row2, row
            else:
                row, row2 = row2, row
        
        currency1, amount1, fee1 = normalize_bitmex_asset(row.currency, con_lib.to_float(row.amount), con_lib.to_float(row.fee))
        currency2, amount2, fee2 = normalize_bitmex_asset(row2.currency, con_lib.to_float(row2.amount), con_lib.to_float(row2.fee))
    
        event = {
            'timestamp': row.timestamp,
            'type': 'buy' if amount1 > 0 else 'sell',
            'asset': currency1,
            'qty': amount1,
            'fee': fee1,
            'asset_b': currency2,
            'qty_b': amount2,
            'fee_b': fee2,
            'address': ''
        }
            
    else:
        
        currency1, amount1, fee1 = normalize_bitmex_asset(row.currency, con_lib.to_float(row.amount), con_lib.to_float(row.fee))

        event = {
            'timestamp': row.timestamp,
            'type': row.transactType,
            'asset': currency1,
            'qty': amount1,
            'fee': 0,
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
