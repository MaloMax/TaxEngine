import sys
import os
import pandas as pd
import glob

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

CexName = "Bybit"

paths = tax_lib.get_cex_paths(CexName)

pattern = os.path.join(paths["events"], CexName+"*_event.csv")

# Trova tutti i file
files = glob.glob(pattern)

print("File trovati:")
for f in files:
    print(f)

# Carica e unisci
df_list = []

for file in files:
    df_sing = pd.read_csv(file)
    df_sing["_event_file"] = os.path.basename(file)
    df_list.append(df_sing)
   
    
# Unione finale
df = pd.concat(df_list, ignore_index=True)


engine = CryptoTaxEngine(tax_lib, CexName)

df = df.sort_values("timestamp").reset_index(drop=True)

tax_lib.reset_result_file(CexName+'_debug.csv')

last_balance = {} 

for idx, row in df.iterrows():
    
    
    event = {
        'timestamp': row.timestamp,
        'type': row.type,
        'asset': row.asset,
        'qty': row.qty,
        'fee': row.fee,
        'asset_b': row.asset_b,
        'qty_b': row.qty_b,
        'fee_b': row.fee_b,
        'address': row.get('address', '')
    }

            
    result = engine.process_event(event)
    
    
    file_key = row._event_file
    if file_key not in last_balance:
        last_balance[file_key] = {}
    if pd.notna(row.Balance) and not pd.isna(row.Balance) and str(row.Balance).strip() != '':
        last_balance[file_key][row.asset] = row.Balance
    
    

    # Somma balance CEX per asset
    cex_balance = {}
    for bal in last_balance.values():
        for asset, val in bal.items():
            cex_balance[asset] = cex_balance.get(asset, 0) + val

            
        
    save_row = {
        **event,
        'Uid': row.Uid,
        'BalanceCex': cex_balance.get(row.asset, 0),
        **result,
        'idx': idx,
        'idxF': row.idx,
        'File': row.File
    }
    
    tax_lib.append_event_to_csv(CexName+'_debug.csv', save_row)

print(last_balance)
    
engine.finalize()
report = engine.build_report()
engine.debug_state()

df_finale = pd.DataFrame(report)

print("\n### TABELLA RIASSUNTO ###")
print(df_finale.to_markdown(index=False))

df_finale.to_csv(CexName+'_test.csv', index=False)

tax_lib.report_status()
   
        
        
        