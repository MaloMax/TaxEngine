"""
build_transfers_db.py
Legge tutti i file *_event.csv dalla cartella Events,
estrae solo deposit e withdrawal, e popola il DB transfers.db
"""

import os
import sqlite3
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
EVENTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "Events")
DB_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers.db")
# ─────────────────────────────────────────────────────────────────────────────

TYPES_WITHDRAWAL = {'withdrawal', 'withdraw'}
TYPES_DEPOSIT    = {'deposit'}
SKIP_ASSETS      = {'EUR', 'MXN'}


def init_db(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS transfers (
            id          TEXT PRIMARY KEY,
            type        TEXT NOT NULL,
            asset       TEXT NOT NULL,
            qty         REAL NOT NULL,
            fee         REAL DEFAULT 0,
            timestamp   INTEGER NOT NULL,
            exchange    TEXT,
            txid        TEXT,
            address     TEXT,
            linked_id   TEXT,
            source      TEXT DEFAULT 'cex',
            status      TEXT DEFAULT 'unmatched',
            source_file TEXT,
            source_idx  INTEGER
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_asset_ts ON transfers(asset, timestamp)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_status   ON transfers(status)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_txid     ON transfers(txid)')
    conn.commit()


def make_id(row, typ):
    return f"{row['Exchange']}_{typ}_{row['asset']}_{int(row['timestamp'])}_{row['source_idx']}"


def load_events(events_dir):
    rows = []
    for fname in sorted(os.listdir(events_dir)):
        if not fname.endswith('_event.csv'):
            continue
        fpath = os.path.join(events_dir, fname)
        try:
            df = pd.read_csv(fpath)
        except Exception as e:
            print(f"  SKIP {fname}: {e}")
            continue

        if 'type' not in df.columns:
            print(f"  SKIP {fname}: colonna 'type' non trovata")
            continue

        mask = df['type'].str.lower().isin(TYPES_WITHDRAWAL | TYPES_DEPOSIT)
        filtered = df[mask].copy()

        if filtered.empty:
            continue

        if 'Exchange' not in filtered.columns:
            filtered['Exchange'] = fname.replace('_event.csv', '').split('_')[0]

        filtered['source_idx']  = filtered['idx'] if 'idx' in filtered.columns else filtered.index
        filtered['source_file'] = fname
        rows.append(filtered)
        print(f"  {fname}: {len(filtered)} righe deposit/withdrawal")

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def build_record(row):
    typ      = row['type'].strip().lower()
    typ_norm = 'withdrawal' if typ in TYPES_WITHDRAWAL else 'deposit'

    asset = str(row.get('asset', '') or row.get('asset_b', '')).strip().upper()
    if not asset or asset == 'NAN':
        return None, 'asset vuoto'
    if asset in SKIP_ASSETS:
        return None, f'asset escluso ({asset})'

    qty = to_float(row.get('qty') or row.get('qty_b', 0))
    if qty == 0:
        qty = to_float(row.get('qty_b', 0))

    address = str(row.get('address', '') or '').strip()
    address = None if address in ('-', 'nan', 'None', '') else address

    return {
        'id':          make_id(row, typ_norm),
        'type':        typ_norm,
        'asset':       asset,
        'qty':         abs(qty),
        'fee':         abs(to_float(row.get('fee', 0))),
        'timestamp':   int(to_float(row.get('timestamp', 0))),
        'exchange':    str(row.get('Exchange', '')),
        'txid':        None,
        'address':     address,
        'linked_id':   None,
        'source':      'cex',
        'status':      'unmatched',
        'source_file': row.get('source_file', ''),
        'source_idx':  int(row.get('source_idx', 0)),
    }, None


def populate_db(conn, df):
    inserted = 0
    skipped  = 0
    skip_log = {}

    for _, row in df.iterrows():
        rec, reason = build_record(row)
        if rec is None:
            skip_log[reason] = skip_log.get(reason, 0) + 1
            skipped += 1
            continue
        try:
            conn.execute('''
                INSERT OR IGNORE INTO transfers
                (id, type, asset, qty, fee, timestamp, exchange, txid,
                 address, linked_id, source, status, source_file, source_idx)
                VALUES
                (:id,:type,:asset,:qty,:fee,:timestamp,:exchange,:txid,
                 :address,:linked_id,:source,:status,:source_file,:source_idx)
            ''', rec)
            inserted += conn.execute('SELECT changes()').fetchone()[0]
        except sqlite3.Error as e:
            reason = f'DB error: {e}'
            skip_log[reason] = skip_log.get(reason, 0) + 1
            skipped += 1

    conn.commit()

    if skip_log:
        print("  Scartati:")
        for reason, count in skip_log.items():
            print(f"    {count:3d}x  {reason}")

    return inserted, skipped


if __name__ == '__main__':
    print(f"Events dir : {EVENTS_DIR}")
    print(f"DB         : {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    print("Carico eventi...")
    df = load_events(EVENTS_DIR)

    if df.empty:
        print("Nessun evento trovato.")
    else:
        print(f"\nTotale righe deposit/withdrawal: {len(df)}")
        ins, skip = populate_db(conn, df)
        print(f"Inseriti: {ins}  |  Saltati: {skip}")

    conn.close()
