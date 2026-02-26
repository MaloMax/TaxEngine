import sys
import os
import pandas as pd

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

CexName = "Bitso"
skiprows = 0

# recupero tutti i path
paths = tax_lib.get_cex_paths(CexName)

engine = CryptoTaxEngine(tax_lib, CexName)

# reset file eventi
tax_lib.reset_result_file(paths["events"])

# carico report originale
df = pd.read_csv(paths["report"], skiprows=skiprows, on_bad_lines='skip')

df.columns = ['method', 'currency', 'timestamp', 'datetime', 'address', 'xrptag', 'source', 
              'amount', 'fee', 'net_amount', 'Type', 'major', 'minor', 'rate', 'value', 
              'total', 'date', 'Year', 'data', 'eur_mxn']  # Tue colonne OK [code_file:27]


df['row_order'] = range(len(df))
df['Timestamp'] = pd.to_datetime(df['data'], errors='coerce', utc=True)

df_valid = (
    df.dropna(subset=['Timestamp'])
      .sort_values(['Timestamp', 'row_order'])
      .reset_index(drop=True)
)


for idx, row in df.iterrows():
    source = row['source']
    
    
    if source == 'trading_Bitso.csv':
                
        event = {
            'timestamp': row.Timestamp,
            'type': row.Type,
            'asset': row.major,
            'qty': row.amount,
            'fee': row.fee if row.Type == 'buy' else 0.0,
            'asset_b': row.minor,
            'qty_b': row.value,
            'fee_b': row.fee if row.Type == 'sell' else 0.0,
            'address': row.get('address', '')
        }
        
    elif source == 'prelievi_Bitso.csv':  
        
        event = {
            'timestamp': row.Timestamp,
            'type': 'withdrawal',
            'asset': row.currency,
            'qty': row.amount,
            'fee': 0.0,
            'asset_b': '',
            'qty_b': 0,
            'fee_b': 0,
            'address': row.address if pd.notna(row.address) else '-'
        }
    
    elif source == 'versamenti_Bitso.csv': 
        
        event = {
            'timestamp': row.Timestamp,
            'type': 'deposit',
            'asset': row.currency,
            'qty': row.net_amount,
            'fee': 0.0,
            'asset_b': '',
            'qty_b': row.eur_mxn,
            'fee_b': 0,
            'address': row.address if pd.notna(row.address) else '-'
        }

    result = engine.process_event(event)
    
    save_row = {
        'idx': idx+skiprows,
        **event,
        **result
    }
    
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
   
        
        
        