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


# ── PARSE TX → RECORDS ───────────────────────────────────────────────────────

def parse_tx(tx, my_address, my_addresses):
    txid = tx["txid"]
    ts   = tx["status"].get("block_time")
    if not ts:
        print(f"  [SKIP] unconfirmed  txid={txid}  addr={my_address}")
        return []

    fee     = tx.get("fee", 0)
    records = []
    spending = False
    from_addr = None
    for vin in tx["vin"]:
        prev = vin.get("prevout")
        if not prev:
            continue
        if prev.get("scriptpubkey_address") == my_address:
            spending = True
        if prev.get("scriptpubkey_address"):
            from_addr = prev["scriptpubkey_address"]

    seen_out_addrs = []

    for vout in tx["vout"]:
        to_addr = vout.get("scriptpubkey_address")
        amount  = vout.get("value", 0)

        if to_addr == my_address:
            internal = (from_addr in my_addresses) if from_addr else False
            records.append({
                'id':           f"onchain_deposit_{txid}_{my_address}",
                'type':         'deposit',
                'asset':        'BTC',
                'qty':          amount / 1e8,
                'fee':          0.0,
                'timestamp':    ts,
                'exchange':     None,
                'txid':         txid,
                'address_from': from_addr,
                'address_to':   my_address,
                'linked_id':    None,
                'source':       'onchain',
                'status':       'internal' if internal else 'unmatched',
            })

        elif spending and to_addr and to_addr not in seen_out_addrs:
            tx_fee = fee if not seen_out_addrs else 0
            internal = (to_addr in my_addresses)
            records.append({
                'id':           f"onchain_withdrawal_{txid}_{to_addr}",
                'type':         'withdrawal',
                'asset':        'BTC',
                'qty':          amount / 1e8,
                'fee':          tx_fee / 1e8,
                'timestamp':    ts,
                'exchange':     None,
                'txid':         txid,
                'address_from': my_address,
                'address_to':   to_addr,
                'linked_id':    None,
                'source':       'onchain',
                'status':       'internal' if internal else 'unmatched',
            })
            seen_out_addrs.append(to_addr)

    if not records:
        print(f"  [SKIP] no_match     txid={txid}  addr={my_address}  "
              f"spending={spending}  from={from_addr}  "
              f"vouts={[v.get('scriptpubkey_address') for v in tx['vout']]}")

    return records

# ── INSERT DB ─────────────────────────────────────────────────────────────────

def insert_record(conn, rec):
    try:
        conn.execute('''
            INSERT OR IGNORE INTO transfers
            (id, type, asset, qty, fee, timestamp, exchange, txid,
             address_from, address_to, linked_id, source, status)
            VALUES
            (:id,:type,:asset,:qty,:fee,:timestamp,:exchange,:txid,
             :address_from,:address_to,:linked_id,:source,:status)
        ''', rec)
        changes = conn.execute('SELECT changes()').fetchone()[0]
        if changes == 0:
            # Recupera il record già presente per confronto
            existing = conn.execute(
                'SELECT type, qty, timestamp, status FROM transfers WHERE id=?',
                (rec['id'],)
            ).fetchone()
            import datetime
            ts_new = datetime.datetime.utcfromtimestamp(rec['timestamp']).strftime('%Y-%m-%d %H:%M')
            if existing:
                ts_ex = datetime.datetime.utcfromtimestamp(existing[2]).strftime('%Y-%m-%d %H:%M')
                print(f"  [DUP]  id={rec['id']}")
                print(f"         nuovo:     type={rec['type']}  qty={rec['qty']:.8f}  ts={ts_new}  status={rec['status']}")
                print(f"         esistente: type={existing[0]}  qty={existing[1]:.8f}  ts={ts_ex}  status={existing[3]}")
            else:
                print(f"  [DUP?] id={rec['id']}  changes=0 ma record non trovato (race condition?)")
        return changes
    except sqlite3.Error as e:
        print(f"  [ERR]  id={rec['id']}  errore={e}")
        return 0

# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    my_addresses = set(keys.keys())
    print(f"Indirizzi: {len(my_addresses)}")
    print(f"DB: {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)

    inserted  = 0
    duplicate = 0
    tx_count  = 0

    for address in sorted(my_addresses):
        txs = get_all_txs(address)
        tx_count += len(txs)

        for tx in txs:
            records = parse_tx(tx, address, my_addresses)
            for rec in records:
                n = insert_record(conn, rec)
                if n:
                    inserted += 1
                else:
                    duplicate += 1

    conn.commit()

    # Riepilogo onchain nel DB
    cur = conn.execute("SELECT status, COUNT(*) FROM transfers WHERE source='onchain' GROUP BY status")
    print(f"\nTx elaborate: {tx_count}")
    print(f"Inseriti: {inserted}  |  Duplicati/già presenti: {duplicate}")
    print("\nStatistiche onchain nel DB:")
    for row in cur.fetchall():
        print(f"  {row[0]:12s}: {row[1]}")

    conn.close()
