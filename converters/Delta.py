import sys
import os
import pandas as pd
from pathlib import Path

def run(filepaths, progress_callback=None):
    
    LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
    sys.path.append(LIBRARY_DIR)

    from converter_lib import con_lib

    NomeRepFiles = filepaths
    CexName = "Delta"
    skiprows = 0

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

    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Date": "time", "Asset Symbol": "asset", "Transaction type": "type", "Contract/Fund": "Contract"})

    df["timestamp"] = df["time"].apply(con_lib.to_timestamp)
    #df = df.sort_values("timestamp").reset_index(drop=True)

    df = df.sort_values( by=["timestamp", "_line"], ascending=[True, False]).reset_index(drop=True)

    #id	Asset ID	Asset Symbol	Amount	Balance	Transaction type	Contract/Fund	Reference ID	Date

    idx = 0
    total = len(df)

    while idx < len(df):
        
        if progress_callback:
            progress_callback(idx, total)

        row = df.iloc[idx]

        if row.type == 'spot_trade' or row.type == 'conversion':
            idx += 1
            row2 = df.iloc[idx]

            if (row2.type != 'spot_trade' and row2.type != 'conversion') or row2.timestamp - row.timestamp > 1 or row.Contract != row2.Contract:
                raise ValueError(f"linea {row._line} {row._file}  == No coppia spot_trade o Conversion")

            idx += 1
            row3 = df.iloc[idx]

            if (row3.type != 'trading fees' and row3.type != 'conversion') or row3.timestamp - row.timestamp > 1 or row.Contract != row3.Contract:
                idx -= 1
                row3 = None

            if con_lib.isTax(row.asset):
                if con_lib.isTax(row2.asset):
                    if row.asset.upper() == 'EUR' or con_lib.to_float(row.Amount) > 0:
                        row, row2 = row2, row
                else:
                    row, row2 = row2, row

            fee = 0.0
            fee_b = 0.0
            amount = con_lib.to_float(row.Amount)
            amount_b = con_lib.to_float(row2.Amount)

            if row3 is not None:
                if row3.type == 'trading fees' :
                    if row.asset == row3.asset :
                        fee = con_lib.to_float(row3.Amount)
                    if row2.asset == row3.asset :
                        fee_b = con_lib.to_float(row3.Amount)

                elif row3.type == 'conversion' :
                    if row.asset == row3.asset :
                        amount += con_lib.to_float(row3.Amount)
                    if row2.asset == row3.asset :
                        amount_b += con_lib.to_float(row3.Amount)

            event = {
                'timestamp': row.timestamp,
                'type': 'buy' if amount > 0 else 'sell',
                'asset': row.asset,
                'qty': amount,
                'fee': fee,
                'asset_b': row2.asset,
                'qty_b': amount_b,
                'fee_b': fee_b,
                'address': ''
            }

        else:


            event = {
                'timestamp': row.timestamp,
                'type': row.type,
                'asset': row.asset,
                'qty': con_lib.to_float(row.Amount),
                'fee': 0.0,
                'asset_b': '',
                'qty_b': 0,
                'fee_b': 0.0,
                'address': ''
            }


        idx += 1  
        save_row = {
            **event,
            'Exchange':CexName,
            'idx': idx+skiprows,
            '_file': row._file,
            '_line': row._line
        }

        con_lib.append_event_to_csv(EventsFile,save_row )
        
    if progress_callback:
        progress_callback(total, total)
        
    print('Finito')
    
    return EventsFile

if __name__ == "__main__":
        
    NomeRepFiles = [
        "AssetHistoryDelta_73721019.csv",
        "AssetHistoryDelta_73721019 2022.csv",
    ]
    
    run(NomeRepFiles)