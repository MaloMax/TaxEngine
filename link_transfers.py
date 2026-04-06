"""
link_transfers.py
Collega withdrawal → deposit in due fasi:

Fase 1 - TXID linking (onchain):
  withdrawal e deposit con stesso txid → linked automaticamente

Fase 2 - qty/ts/address linking (CEX → onchain o CEX → CEX):
  per ogni deposit unmatched cerca withdrawal con:
    - stesso asset
    - qty ≈ tolleranza QTY_TOL
    - 0 < ts_deposit - ts_withdrawal < TIME_WINDOW_H
    - address match → score 2, solo qty/ts → score 1

Al termine esporta transfers_export.csv con i soli record non linkati.
"""

import os
import csv
import sqlite3
import datetime
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers.db")
CSV_PATH      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers_export.csv")
QTY_TOL       = 0.001
TIME_WINDOW_H = 48
# ─────────────────────────────────────────────────────────────────────────────


def ts_str(ts):
    if ts is None:
        return ""
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)


# ── FASE 1: TXID LINKING ─────────────────────────────────────────────────────

def link_by_txid(conn):
    rows = conn.execute('''
        SELECT txid, type, id FROM transfers
        WHERE txid IS NOT NULL AND status='unmatched'
        ORDER BY txid, type
    ''').fetchall()

    by_txid = defaultdict(lambda: {'withdrawal': [], 'deposit': []})
    for txid, typ, rid in rows:
        by_txid[txid][typ].append(rid)

    linked = 0
    for txid, groups in by_txid.items():
        ws = groups['withdrawal']
        ds = groups['deposit']
        if not ws or not ds:
            continue
        for rid in ws + ds:
            conn.execute(
                "UPDATE transfers SET linked_id=?, status='linked' WHERE id=?",
                (txid, rid)
            )
            linked += 1

    conn.commit()
    print(f"Fase 1 (txid):  {linked} record linkati")


# ── FASE 2: QTY/TS/ADDRESS LINKING ───────────────────────────────────────────

def load_unmatched(conn):
    rows = conn.execute('''
        SELECT id, type, asset, qty, fee, timestamp, exchange, address, source
        FROM transfers
        WHERE status='unmatched'
        ORDER BY timestamp ASC
    ''').fetchall()

    by_asset = defaultdict(lambda: {'withdrawals': [], 'deposits': []})
    for r in rows:
        d = dict(r)
        by_asset[d['asset']][d['type'] + 's'].append(d)
    return by_asset


def find_withdrawal(deposit, withdrawals, qty_tol, window_sec):
    d_addr = deposit['address'] or ''
    best, best_score, best_dt = None, 0, None

    for w in withdrawals:
        dt = deposit['timestamp'] - w['timestamp']
        if dt <= 0 or dt > window_sec:
            continue

        qty_w_net = w['qty'] - w.get('fee', 0)
        if abs(deposit['qty'] - w['qty']) > qty_tol and abs(deposit['qty'] - qty_w_net) > qty_tol:
            continue

        w_addr = w['address'] or ''
        score = 2 if (d_addr and w_addr and d_addr == w_addr) else 1

        if score > best_score or (score == best_score and (best_dt is None or dt < best_dt)):
            best, best_score, best_dt = w, score, dt

    return best, best_score


def link_pair(conn, withdrawal, deposit, score):
    conn.execute(
        "UPDATE transfers SET linked_id=?, status='linked' WHERE id=?",
        (deposit['id'], withdrawal['id'])
    )
    conn.execute(
        "UPDATE transfers SET linked_id=?, status='linked' WHERE id=?",
        (withdrawal['id'], deposit['id'])
    )
    dt = deposit['timestamp'] - withdrawal['timestamp']
    addr_info = f"addr={deposit['address'] or '—'}" if score == 2 else "qty/ts only"
    print(f"    LINK [score={score}] dt={dt//60}min  {addr_info}")
    print(f"      W: {withdrawal['id']}")
    print(f"      D: {deposit['id']}")


def link_by_qty_ts(conn, qty_tol, window_h):
    window_sec     = window_h * 3600
    by_asset       = load_unmatched(conn)
    total_linked   = 0
    total_unlinked_d = []
    total_unlinked_w = []

    for asset, groups in sorted(by_asset.items()):
        withdrawals = groups['withdrawals']
        deposits    = groups['deposits']
        if not deposits and not withdrawals:
            continue

        print(f"\n  {asset}: {len(withdrawals)} withdrawal, {len(deposits)} deposit")

        linked_withdrawal_ids = set()
        unlinked_d = []

        for dep in deposits:
            available = [w for w in withdrawals if w['id'] not in linked_withdrawal_ids]
            match, score = find_withdrawal(dep, available, qty_tol, window_sec)
            if match:
                link_pair(conn, match, dep, score)
                linked_withdrawal_ids.add(match['id'])
                total_linked += 1
            else:
                unlinked_d.append(dep)

        unlinked_w = [w for w in withdrawals if w['id'] not in linked_withdrawal_ids]
        total_unlinked_d.extend(unlinked_d)
        total_unlinked_w.extend(unlinked_w)

    conn.commit()
    print(f"\nFase 2 (qty/ts): {total_linked} coppie linkate")
    return total_linked, total_unlinked_d, total_unlinked_w


# ── EXPORT CSV ────────────────────────────────────────────────────────────────

def export_csv(conn, csv_path):
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM transfers WHERE status != 'linked' ORDER BY timestamp ASC"
        #"SELECT * FROM transfers ORDER BY timestamp ASC"
    ).fetchall()

    if not rows:
        print("Nessun record non linkato da esportare.")
        return

    cols = list(rows[0].keys())
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            d = dict(row)
            d['timestamp'] = ts_str(d.get('timestamp'))
            writer.writerow(d)

    print(f"\nCSV esportato: {csv_path}  ({len(rows)} righe non linkate)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(db_path, csv_path, qty_tol=QTY_TOL, window_h=TIME_WINDOW_H):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("── Fase 1: txid linking ───────────────────────────────────────")
    link_by_txid(conn)

    print("\n── Fase 2: qty/ts/address linking ────────────────────────────")
    total_linked, unlinked_d, unlinked_w = link_by_qty_ts(conn, qty_tol, window_h)

    print(f"\n══ RIEPILOGO ══════════════════════════════════════════════════")
    print(f"  Deposit senza provenienza: {len(unlinked_d)}")
    print(f"  Withdrawal in wallet:      {len(unlinked_w)}")

    if unlinked_d:
        print(f"\n── DEPOSIT SENZA PROVENIENZA ──────────────────────────────────")
        for d in unlinked_d:
            print(f"  {ts_str(d['timestamp'])}  {d['asset']:6s}  qty={d['qty']:.8f}  "
                  f"addr={d['address'] or '—'}  [{d['exchange'] or 'onchain'}]")

    if unlinked_w:
        print(f"\n── WITHDRAWAL IN WALLET ───────────────────────────────────────")
        for w in unlinked_w:
            print(f"  {ts_str(w['timestamp'])}  {w['asset']:6s}  qty={w['qty']:.8f}  "
                  f"addr={w['address'] or '—'}  [{w['exchange'] or 'onchain'}]")

    export_csv(conn, csv_path)
    conn.close()


if __name__ == '__main__':
    print(f"DB : {DB_PATH}\n")
    run(DB_PATH, CSV_PATH)
