import sys
import os
import pandas as pd

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

CexName = "Bitpanda"
skiprows = 5

# recupero tutti i path
paths = tax_lib.get_cex_paths(CexName)

engine = CryptoTaxEngine(tax_lib, CexName)

# reset file eventi
tax_lib.reset_result_file(paths["events"])

# carico report originale
df = pd.read_csv(paths["report"], skiprows=skiprows, on_bad_lines='skip')

df.columns = [
    'ID', 'Timestamp', 'Type', 'InOut',   'Amt_Fiat', 'Fiat',
    'Amt_Asset', 'Asset',  'Price', 'PriceCurr', 'Class', 'ProdID',
    'Fee', 'FeeAsset', 'FeePct',  'Spread', 'SpreadCurr', 'Tax', 'address'
]

df['row_order'] = range(len(df))
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', utc=True)

df_valid = (
    df.dropna(subset=['Timestamp'])
      .sort_values(['Timestamp', 'row_order'])
      .reset_index(drop=True)
)

for idx, row in df_valid.iterrows():

    event = {
        'timestamp': row.Timestamp,
        'type': row.Type,
        'asset': row.Asset,
        'qty': row.Amt_Asset,
        'fee': row.Fee,
        'asset_b': row.Fiat,
        'qty_b': row.Amt_Fiat,
        'fee_b': 0.0,
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
