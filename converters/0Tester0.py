import sys
import os
import pandas as pd
import glob

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

CexName = "Bitfinex"

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
    df_list.append(df_sing)

# Unione finale
df = pd.concat(df_list, ignore_index=True)


engine = CryptoTaxEngine(tax_lib, CexName)


df_valid = df.copy()

invalid_ts = df_valid['timestamp'].isna().sum()

if invalid_ts > 0:
    print(f"ATTENZIONE: {invalid_ts} timestamp null trovati")
    
df['row_order'] = range(len(df))
df = (df.dropna(subset=['timestamp']).sort_values(['timestamp', 'row_order']).reset_index(drop=True))


for idx, row in df.iterrows():
    
        
    event = {
        'idx': row.idx,
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
    
    save_row = {
        'idx': idx,
        **event,
        **result
    }
    tax_lib.append_event_to_csv(CexName+'_debug.csv', save_row)
        
        
    
engine.finalize()
report = engine.build_report()
engine.debug_state()

df_finale = pd.DataFrame(report)

print("\n### TABELLA RIASSUNTO ###")
print(df_finale.to_markdown(index=False))

df_finale.to_csv(CexName+'_test.csv', index=False)

tax_lib.report_status()
   
        
        
        