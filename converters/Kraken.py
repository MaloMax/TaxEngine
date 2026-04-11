import sys
import os
import pandas as pd
from pathlib import Path

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from converter_lib import con_lib

def run(filepaths, progress_callback=None):

    NomeRepFiles = filepaths
    CexName = "Kraken"
    skiprows = 0
    
    TYPE_MAP = {
        "promo_credit": "reward",
        "staking_reward": "reward",
        "airdrop": "airdrop",
    }

    paths = con_lib.get_cex_paths(CexName)
    nome_file = Path(__file__).stem

    EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")

    con_lib.reset_result_file(EventsFile)

    dfs = []
    for f in NomeRepFiles:
        path = os.path.join(paths["report"], f)
        df_tmp = pd.read_csv(path, skiprows=skiprows, on_bad_lines='skip', keep_default_na=False)
        df_tmp["_file"] = f
        df_tmp["_line"] = range(len(df_tmp))  # indice riga file originale
        dfs.append(df_tmp)

    df = pd.concat(dfs, ignore_index=True)

    df["timestamp"] = df["time"].apply(con_lib.to_timestamp)
    df = df.sort_values("timestamp").reset_index(drop=True)

    #txid	refid	time	type	subtype	aclass	subclass	asset	wallet	amount	fee	balance	address

    idx = 0
    total = len(df)

    while idx < len(df):
        
        if progress_callback:
            progress_callback(idx, total)

        row = df.iloc[idx]

        if row.type == 'trade':
            idx += 1
            row2 = df.iloc[idx]

            if row.subclass != 'crypto':
                if row2.subclass != 'crypto':
                    raise ValueError(f"linea {idx+3} == Scambio spot Fiat")
                else:
                    row, row2 = row2, row

            event = {
                'timestamp': row.timestamp,
                'type': 'buy' if con_lib.to_float(row.amount) > 0 else 'sell',
                'raw_type': row.type,
                'asset': row.asset,
                'qty': con_lib.to_float(row.amount),
                'fee': con_lib.to_float(row.fee),
                'asset_b': row2.asset,
                'qty_b': con_lib.to_float(row2.amount),
                'fee_b': con_lib.to_float(row2.fee),
                'address': ''
            }

        else:


            event = {
                'timestamp': row.timestamp,                
                'type': TYPE_MAP.get(row.type, row.type),
                'raw_type': row.type,
                'asset': row.asset,
                'qty': con_lib.to_float(row.amount),
                'fee': con_lib.to_float(row.fee),
                'asset_b': '',
                'qty_b': 0,
                'fee_b': 0.0,
                'address': row.address if pd.notna(row.address) else ''
            }


        idx += 1  
        save_row = {
            **event,
            'Exchange':CexName,
            '_line': row._line,
            '_file': row._file
        }

        con_lib.append_event_to_csv(EventsFile,save_row )

    if progress_callback:
        progress_callback(total, total)
        
    print('Finito')
    return EventsFile

if __name__ == "__main__":
        
    NomeRepFiles = [
        "Kraken_cex_report.csv"
    ]
    
    run(NomeRepFiles)
