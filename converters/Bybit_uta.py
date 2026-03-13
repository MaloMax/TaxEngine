import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)
from converter_lib import con_lib

NomeRepFiles = [
    "Bybit_AssetChangeDetails_uta_154570665_20240101_20241231_0.csv",
    "Bybit_AssetChangeDetails_uta_154570665_20250101_20251231_0.csv",
]
CexName = "Bybit"
nome_file = Path(__file__).stem

paths = con_lib.get_cex_paths(CexName)
EventsFile = os.path.join(paths["events"], f"{nome_file}_event.csv")
con_lib.reset_result_file(EventsFile)

# Carica e unisci tutti i file, saltando la riga 0 (header UID)
dfs = []
for f in NomeRepFiles:
    path = os.path.join(paths["report"], f)
    df_tmp = pd.read_csv(path, skiprows=1, on_bad_lines='skip', keep_default_na=False)
    df_tmp["_file"] = f
    df_tmp["_line"] = range(len(df_tmp))  # indice riga file originale
    dfs.append(df_tmp)

df = pd.concat(dfs, ignore_index=True)

# Rinomina colonne per comodità
df.columns = df.columns.str.strip()
df = df.rename(columns={"Time(UTC)": "Time", "Fee Paid": "FeePaid", "Cash Flow": "CashFlow"})


df["timestamp"] = df["Time"].apply(con_lib.to_timestamp)
df = df.sort_values("timestamp").reset_index(drop=True)

print(datetime.utcfromtimestamp(df["timestamp"].min()))
print(datetime.utcfromtimestamp(df["timestamp"].max()))

def to_float(val):
    try:
        return float(val)
    except:
        return 0.0

def is_futures_trade(row):
    """Trade futures: ha Contract, Action è OPEN/CLOSE"""
    return (
        str(row.get("Type", "")).upper() == "TRADE"
        and str(row.get("Action", "")).upper() in ("OPEN", "CLOSE")
    )

def is_spot_trade(row):
    """Trade spot: ha Contract, Position=0, Action=--"""
    return (
        str(row.get("Type", "")).upper() == "TRADE"
        and str(row.get("Action", "--")).strip() == "--"
        and pd.notna(row.get("Contract")) and str(row.get("Contract", "")).strip() != ""
    )

idx = 0
while idx < len(df):
    row = df.iloc[idx]
    typ = str(row.get("Type", "")).strip().upper()
    action = str(row.get("Action", "")).strip().upper()
    contract = str(row.get("Contract", "")).strip()
    currency = str(row.get("Currency", "")).strip()
    cashflow = to_float(row.get("CashFlow", 0))
    funding = to_float(row.get("Funding", 0))
    fee = to_float(row.get("FeePaid", 0))
    ts = row.timestamp
    NomeRepFile = row["_file"]
    event = None

    cashflow += funding
    
    if action == "--" and  contract != "" and typ != "INTEREST" and  typ != "TRADE" and not typ.startswith("TRANSFER") :
        print(f"***** action _line={row['_line']} Type={typ} Currency={currency}  contract={contract} CashFlow={cashflow} File={NomeRepFile} idx={row._line}")

    
    if typ == "TRADE" and action != "--":
    
    # ── TRADE FUTURES CLOSE: realisedpnl ───────────────────────────────────
        if action == "CLOSE" or action == "OPEN":
            if cashflow != 0:
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

    # ── INTEREST ────────────────────────────────────────────────────────────
    elif typ == "INTEREST" or typ == "SETTLEMENT" or typ == "DELIVERY" or typ == "FEE_REFUND" or typ == "LIQUIDATION" or typ == "BONUS": 
        if cashflow != 0:
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
            
    # ── TRANSFER_OUT_IN:  ──────────────────────────────────────────────
    elif typ.startswith("TRANSFER")  :
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
        
    # ── Type -- Contract vuoto: entrate/uscite varie (es. conversioni) ──────
    elif typ == "--" and contract == "":
        if cashflow != 0:
            event = {
                'timestamp': ts,
                'type': 'funding',   # entrata/uscita generica
                'asset': currency,
                'qty': cashflow,
                'fee': 0.0,
                'asset_b': '',
                'qty_b': 0.0,
                'fee_b': 0.0,
                'address': ''
            }
            
    '''        
    # ── TRADE SPOT: accoppio 2 righe stesso Contract+Time ──────────────────
    elif  action == "--":
        
        
        elif action == "--" and contract != "":
            idx2 = idx + 1
            if idx2 < len(df):
                row2 = df.iloc[idx2]
                if (str(row2.get("Contract", "")).strip() == contract
                        and row2.timestamp == ts):

                    cur1 = str(row.get("Currency", "")).strip()
                    cur2 = str(row2.get("Currency", "")).strip()
                    cf1 = to_float(row.get("CashFlow", 0))
                    cf2 = to_float(row2.get("CashFlow", 0))
                    fee1 = abs(to_float(row.get("FeePaid", 0)))
                    fee2 = abs(to_float(row2.get("FeePaid", 0)))

                    # asset_b = stablecoin/fiat, asset = crypto
                    if con_lib.isTax(cur1):
                        asset_b, qty_b, fee_b = cur1, cf1, fee1
                        asset,   qty,   fee_a = cur2, cf2, fee2
                    else:
                        asset,   qty,   fee_a = cur1, cf1, fee1
                        asset_b, qty_b, fee_b = cur2, cf2, fee2

                    # buy se qty crypto > 0, sell se < 0
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
                    idx += 2
                else:
                    idx += 1
            else:
                idx += 1

            if event:
                con_lib.append_event_to_csv(EventsFile, {**event, 'Exchange': CexName, 'idx': idx, 'File': NomeRepFile})
            continue
        
        else:
            print(f"TRADE UNKNOWN _line={row['_line']} Type={typ} Currency={currency} CashFlow={cashflow} File={NomeRepFile} idx={row._line}")
    
    
    else:  
    # ── tutto il resto: stampa per debug ────────────────────────────────────
        bol = True
        #print(f"UNKNOWN _line={row['_line']} Type={typ} Currency={currency} CashFlow={cashflow} fee={fee} File={NomeRepFile} idx={row._line}")
    '''  
    if event:
        con_lib.append_event_to_csv(EventsFile, {**event, 'Exchange': CexName, 'idx': idx, 'File': NomeRepFile})

    idx += 1

print('Finito')