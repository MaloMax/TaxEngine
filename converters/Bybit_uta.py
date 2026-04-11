import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)
from converter_lib import con_lib

def run(filepaths, progress_callback=None):
    
    CexName = "Bybit"
    skiprows = 1
    
    nome_file = Path(__file__).stem
    
    paths = con_lib.get_cex_paths(CexName)
    EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")
    con_lib.reset_result_file(EventsFile)
    
    dfs = []
    for f in filepaths:
        path = os.path.join(paths["report"], f)
        df_tmp = pd.read_csv(path, skiprows=skiprows, on_bad_lines='skip', keep_default_na=False)
        df_tmp["_file"] = f
        df_tmp["_line"] = range(len(df_tmp))
        dfs.append(df_tmp)
    
    df = pd.concat(dfs, ignore_index=True)
    
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"Time(UTC)": "Time", "Fee Paid": "FeePaid", "Cash Flow": "CashFlow", "Wallet Balance": "Balance"})
    
    df["timestamp"] = df["Time"].apply(con_lib.to_timestamp)
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    def to_float(val):
        try:
            return float(val)
        except:
            return 0.0
    
    idx = 0
    total = len(df)
    while idx < len(df):
        
        if progress_callback:
            progress_callback(idx, total)
        
        row = df.iloc[idx]    
        typ = str(row.get("Type", "")).strip().upper()
        action = str(row.get("Action", "")).strip().upper()
        contract = str(row.get("Contract", "")).strip()
        currency = str(row.get("Currency", "")).strip()
        cashflow = to_float(row.get("CashFlow", 0))
        funding = to_float(row.get("Funding", 0))
        fee = to_float(row.get("FeePaid", 0))
        ts = row.timestamp
        event = None
    
        cashflow += funding
        
        if action == "--" and  contract != "" and typ != "INTEREST" and  typ != "TRADE" and not typ.startswith("TRANSFER") :
            print(f"***** action _line={row['_line']} Type={typ} Currency={currency}  contract={contract} CashFlow={cashflow} File={row._file} idx={row._line}")
    
        
        if typ == "TRADE" and action != "--":
        
            if action == "CLOSE" or action == "OPEN":
                event = {
                    'timestamp': ts,
                    'type': 'realisedpnl',
                    'asset': currency,
                    'qty': cashflow,
                    'fee': abs(fee),
                    'asset_b': '',
                    'qty_b': 0.0,
                    'fee_b': 0.0,
                    'address': ''
                }
            else:
                print(f"TRADE no CLOSE OPEN _line={row['_line']} Type={typ} action={action} CashFlow={cashflow} File={row._file} idx={row._line}")
                
    
        elif typ == "INTEREST" or typ == "SETTLEMENT" or typ == "DELIVERY" or typ == "FEE_REFUND" or typ == "LIQUIDATION" or typ == "BONUS": 
            
            event = {
                'timestamp': ts,
                'type': 'funding',
                'asset': currency,
                'qty': cashflow,
                'fee': abs(fee),
                'asset_b': '',
                'qty_b': 0.0,
                'fee_b': 0.0,
                'address': ''
            }
                
        elif typ.startswith("TRANSFER"):
            event = {
                'timestamp': ts,
                'type': 'no_tax_'+typ,
                'asset': currency,
                'qty': cashflow,
                'fee': abs(fee),
                'asset_b': '',
                'qty_b': 0.0,
                'fee_b': 0.0,
                'address': ''
            }
     
               
        elif  action == "--":
            
            cashArr = {}
            feeArr = {}
            
            cashArr[currency] = cashflow
            feeArr[currency] = fee
                    
            while idx < len(df):
                idx += 1
                row = df.iloc[idx]
                typN = str(row.get("Type", "")).strip().upper()
                actionN = str(row.get("Action", "")).strip().upper()
                contractN = str(row.get("Contract", "")).strip()
                currencyN = str(row.get("Currency", "")).strip()
                cashflowN = to_float(row.get("CashFlow", 0))
                fundingN = to_float(row.get("Funding", 0))
                feeN = to_float(row.get("FeePaid", 0))
                tsN = row.timestamp
                
                if ts != tsN or contract != contractN:
                    idx -= 1
                    break
                    
                cashArr[currencyN] = cashArr.get(currencyN, 0) + cashflowN
                feeArr[currencyN] = feeArr.get(currencyN, 0) + feeN
    
            if len(cashArr) == 1:
    
                event = {
                    'timestamp': ts,
                    'type': 'no_tax_NoDettagli',
                    'asset': currency,
                    'qty': cashflow,
                    'fee': abs(fee),
                    'asset_b': '',
                    'qty_b': 0.0,
                    'fee_b': 0.0,
                    'address': ''
                }  
    
    
            elif len(cashArr) == 2:
    
                cur1 = list(cashArr)[0]
                cur2 = list(cashArr)[1]
    
                if con_lib.isTax(cur1):
                    asset,   qty,   fee_a = cur2, cashArr[cur2], feeArr[cur2]
                    asset_b, qty_b, fee_b = cur1, cashArr[cur1], feeArr[cur1]
                else:
                    asset,   qty,   fee_a = cur1, cashArr[cur1], feeArr[cur1]
                    asset_b, qty_b, fee_b = cur2, cashArr[cur2], feeArr[cur2]
    
                trade_type = 'buy' if qty > 0 else 'sell'
    
                event = {
                    'timestamp': ts,
                    'type': trade_type,
                    'asset': asset,
                    'qty': qty,
                    'fee': fee_a,
                    'asset_b': asset_b,
                    'qty_b': qty_b,
                    'fee_b': fee_b,
                    'address': ''
                }
    
            else:
    
                for cur, cf in cashArr.items():
                    
                    ev = {
                        'timestamp': ts,
                        'type': 'funding',
                        'asset': cur,
                        'qty': cf,
                        'fee': abs(feeArr[cur]),
                        'asset_b': '', 'qty_b': 0.0, 'fee_b': 0.0, 'address': ''
                    }
                    con_lib.append_event_to_csv(EventsFile, {**ev, 'Exchange': CexName, '_line': row._line, '_file': row._file})
    
        else:
            print(f"TRADE UNKNOWN _line={row['_line']} Type={typ} Currency={currency} action={action} File={row._file} idx={row._line}")
    
    
        if event:
            con_lib.append_event_to_csv(EventsFile, {**event, 'Exchange': CexName, '_line': row._line, '_file': row._file})
    
        idx += 1
    
    if progress_callback:
        progress_callback(total, total)
    
    print('Finito')
    return EventsFile

if __name__ == "__main__":
    
    NomeRepFiles = [
        "Bybit_AssetChangeDetails_uta_154570665_20240101_20241231_0.csv",
        "Bybit_AssetChangeDetails_uta_154570665_20250101_20251231_0.csv",
    ]
    
    run(NomeRepFiles)