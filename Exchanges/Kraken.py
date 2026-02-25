import sys
import os
import pandas as pd

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Library")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

CexName = "Kraken"
skiprows = 0

# recupero tutti i path
paths = tax_lib.get_cex_paths(CexName)

engine = CryptoTaxEngine(tax_lib, CexName)

# reset file eventi
tax_lib.reset_result_file(paths["events"])

# carico report originale
df = pd.read_csv(paths["report"], skiprows=skiprows, on_bad_lines='skip')

df.columns = [
    'txid', 'refid', 'Timestamp', 'Type', 'subtype',
    'aclass', 'subclass',    'asset', 'wallet',
    'amount', 'fee', 'balance','address'
]

df['row_order'] = range(len(df))
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', utc=True)

df_valid = (
    df.dropna(subset=['Timestamp'])
      .sort_values(['Timestamp', 'row_order'])
      .reset_index(drop=True)
)

idx = 0

while idx < len(df_valid):

    row = df_valid.iloc[idx]
    
    if row.Type == 'trade':
        idx += 1
        row2 = df_valid.iloc[idx]
        
        if row.subclass != 'crypto':
            if row2.subclass != 'crypto':
                raise ValueError(f"linea {idx+3} == Scambio spot Fiat")
            else:
                row, row2 = row2, row
        
        event = {
            'timestamp': row.Timestamp,
            'type': 'buy' if row.amount > 0 else 'sell',
            'asset': row.asset,
            'qty': row.amount,
            'fee': row.fee,
            'asset_b': row2.asset,
            'qty_b': row2.amount,
            'fee_b': row2.fee,
            'address': '-'
        }
            
    else:
        

        event = {
            'timestamp': row.Timestamp,
            'type': row.Type,
            'asset': row.asset,
            'qty': row.amount,
            'fee': row.fee,
            'asset_b': '',
            'qty_b': 0,
            'fee_b': 0.0,
            'address': row.address if pd.notna(row.address) else '-'
        }

    result = engine.process_event(event)
    
    save_row = {
        'idx': idx+skiprows,
        **event,
        **result
    }
    
    
    idx += 1
        
    
    tax_lib.append_event_to_csv(paths["events"],save_row )

engine.finalize()
report = engine.build_report()
engine.debug_state()

df_finale = pd.DataFrame(report)

print("\n### TABELLA RIASSUNTO ###")
print(df_finale.to_markdown(index=False))

df_finale.to_csv(paths["tax"], index=False)

tax_lib.report_status()
tax_lib.report_consolidato()
