"""
import_onchain.py
Legge i pickle mempool.space e inserisce nel DB transfers.
Un record per ogni mio indirizzo coinvolto nella tx:
  - vin  → withdrawal (address = mio indirizzo che spende)
  - vout → deposit    (address = mio indirizzo che riceve)
Fee totale memorizzata sul deposit.
"""

import os
import sys
import sqlite3
import pickle
import requests
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data"))
from addresses import keys

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_URL  = "https://mempool.space/api"
BASE_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "AnalisiOC", "btc_data")
TX_DIR    = os.path.join(BASE_DIR, "tx_raw")
DB_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers.db")
# ─────────────────────────────────────────────────────────────────────────────

os.makedirs(TX_DIR, exist_ok=True)


# ── FETCH / CACHE TX ─────────────────────────────────────────────────────────

def load_txs(address):
    path = os.path.join(TX_DIR, f"{address}.pkl")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    return None

def save_txs(address, txs):
    path = os.path.join(TX_DIR, f"{address}.pkl")
    with open(path, "wb") as f:
        pickle.dump(txs, f)

def get_all_txs(address, sleep=0.2):
    cached = load_txs(address)
    if cached is not None:
        return cached

    print(f"  Scarico {address}")
    all_txs   = []
    last_txid = None
    while True:
        url = f"{BASE_URL}/address/{address}/txs/chain"
        if last_txid:
            url += f"/{last_txid}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        txs = r.json()
        if not txs:
            break
        all_txs.extend(txs)
        last_txid = txs[-1]["txid"]
        if len(txs) < 25:
            break
        time.sleep(sleep)

    save_txs(address, all_txs)
    return all_txs


# ── INIT DB ───────────────────────────────────────────────────────────────────

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


# ── PARSE TX → RECORDS ───────────────────────────────────────────────────────

def parse_tx(tx, my_addresses):
    txid = tx["txid"]
    ts   = tx["status"].get("block_time")
    if not ts:
        print(f"  [SKIP] unconfirmed  txid={txid}")
        return []

    fee     = tx.get("fee", 0)
    records = []

    # withdrawal: ogni mio indirizzo in input
    for vin in tx["vin"]:
        prev = vin.get("prevout") or {}
        addr = prev.get("scriptpubkey_address")
        val  = prev.get("value", 0)
        if addr and addr in my_addresses:
            records.append({
                'id':        f"onchain_withdrawal_{txid}_{addr}",
                'type':      'withdrawal',
                'asset':     'BTC',
                'qty':       val / 1e8,
                'fee':       0.0,
                'timestamp': ts,
                'exchange':  None,
                'txid':      txid,
                'address':   addr,
                'linked_id': None,
                'source':    'onchain',
                'status':    'unmatched',
            })

    # deposit: ogni mio indirizzo in output
    for vout in tx["vout"]:
        addr = vout.get("scriptpubkey_address")
        val  = vout.get("value", 0)
        if addr and addr in my_addresses:
            records.append({
                'id':        f"onchain_deposit_{txid}_{addr}",
                'type':      'deposit',
                'asset':     'BTC',
                'qty':       val / 1e8,
                'fee':       fee / 1e8,
                'timestamp': ts,
                'exchange':  None,
                'txid':      txid,
                'address':   addr,
                'linked_id': None,
                'source':    'onchain',
                'status':    'unmatched',
            })

    return records


# ── INSERT DB ─────────────────────────────────────────────────────────────────

def insert_record(conn, rec):
    try:
        conn.execute('''
            INSERT OR IGNORE INTO transfers
            (id, type, asset, qty, fee, timestamp, exchange, txid,
             address, linked_id, source, status)
            VALUES
            (:id,:type,:asset,:qty,:fee,:timestamp,:exchange,:txid,
             :address,:linked_id,:source,:status)
        ''', rec)
        return conn.execute('SELECT changes()').fetchone()[0]
    except sqlite3.Error as e:
        print(f"  [ERR]  id={rec['id']}  errore={e}")
        return 0


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    my_addresses = set(keys.keys())
    print(f"Indirizzi: {len(my_addresses)}")
    print(f"DB: {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    inserted = 0

    for address in sorted(my_addresses):
        txs = get_all_txs(address)
        print(f"{address}: {len(txs)} tx")

        for tx in txs:
            records = parse_tx(tx, my_addresses)
            for rec in records:
                inserted += insert_record(conn, rec)

    conn.commit()

    cur = conn.execute("SELECT status, COUNT(*) FROM transfers WHERE source='onchain' GROUP BY status")
    print(f"\nInseriti nel DB: {inserted}")
    print("\nStatistiche onchain nel DB:")
    for row in cur.fetchall():
        print(f"  {row[0]:12s}: {row[1]}")

    conn.close()
