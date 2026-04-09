"""
link_transfers.py
Collega withdrawal → deposit in due fasi:

Fase 1 - TXID linking (onchain):
  withdrawal e deposit con stesso txid → linked_id = txid

Fase 2 - qty/ts/address linking (CEX ↔ onchain, CEX ↔ CEX):
  per ogni deposit unmatched cerca:
    a) withdrawal singolo con qty ≈ deposit qty
    b) gruppo di withdrawal onchain con stesso txid e sum(qty) ≈ deposit qty
  finestra temporale: -30min < dt < 48h (CEX timestamp può precedere onchain)
  score: 2 = address match, 1 = solo qty/ts

Al termine esporta transfers_export.csv con i soli record non linkati.
"""

import os
import csv
import sqlite3
import datetime
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers.db")
CSV_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers_export.csv")
QTY_TOL_PCT    = 0.02    # tolleranza 2% sulla quantità
TIME_WINDOW_H  = 48
TIME_BEFORE_M  = 30      # minuti: deposit può precedere withdrawal CEX
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
        SELECT id, type, asset, qty, fee, timestamp, exchange,
               address_from, address_to, source, txid
        FROM transfers
        WHERE status='unmatched'
        ORDER BY timestamp ASC
    ''').fetchall()

    by_asset = defaultdict(lambda: {'withdrawals': [], 'deposits': []})
    for r in rows:
        d = dict(r)
        by_asset[d['asset']][d['type'] + 's'].append(d)
    return by_asset


def in_window(dep_ts, w_ts, window_sec, before_sec):
    """dt = dep_ts - w_ts: positivo = deposit dopo withdrawal (normale)
       negativo = deposit prima del withdrawal (CEX timestamp lag)"""
    dt = dep_ts - w_ts
    return -before_sec <= dt <= window_sec


def address_match(dep, w):
    """Controlla se gli indirizzi sono compatibili per il linking."""
    # address_to del withdrawal == address_to del deposit (wallet destinazione)
    w_to = w.get('address_to') or ''
    d_to = dep.get('address_to') or ''
    if w_to and d_to:
        # address_to withdrawal può essere | separato
        w_addrs = set(w_to.split('|'))
        if d_to in w_addrs:
            return True
    # address_from del deposit contiene address_to del withdrawal
    d_from = dep.get('address_from') or ''
    if w_to and d_from:
        d_from_addrs = set(d_from.split('|'))
        w_addrs = set(w_to.split('|'))
        if w_addrs & d_from_addrs:
            return True
    return False


def find_match(deposit, withdrawals, qty_tol_pct, window_sec, before_sec):
    """
    Cerca:
    1) withdrawal singolo con qty ≈ deposit qty
    2) gruppo di withdrawal con stesso txid e sum(qty) ≈ deposit qty
    Ritorna (match, score, is_group) dove match è lista di withdrawal.
    """
    best_single, best_score = None, 0

    # ── match singolo ─────────────────────────────────────────────────────
    for w in withdrawals:
        if not in_window(deposit['timestamp'], w['timestamp'], window_sec, before_sec):
            continue

        qty_w_net = w["qty"] - w.get("fee", 0)
        tol       = w["qty"] * qty_tol_pct
        if abs(deposit["qty"] - w["qty"]) > tol and abs(deposit["qty"] - qty_w_net) > tol:
            continue

        score = 2 if address_match(deposit, w) else 1
        if score > best_score:
            best_single, best_score = w, score

    if best_single:
        return [best_single], best_score, False

    # ── match gruppo (N withdrawal stesso txid) ────────────────────────────
    # raggruppa withdrawal onchain per txid
    by_txid = defaultdict(list)
    for w in withdrawals:
        if w.get('txid') and w.get('source') == 'onchain':
            if in_window(deposit['timestamp'], w['timestamp'], window_sec, before_sec):
                by_txid[w['txid']].append(w)

    for txid, group in by_txid.items():
        total_qty = sum(w['qty'] for w in group)
        total_fee = sum(w.get('fee', 0) for w in group)
        qty_net   = total_qty - total_fee
        tol_g = total_qty * qty_tol_pct
        if abs(deposit["qty"] - total_qty) <= tol_g or abs(deposit["qty"] - qty_net) <= tol_g:
            score = 2 if any(address_match(deposit, w) for w in group) else 1
            return group, score, True

    return [], 0, False


def link_group(conn, withdrawals, deposit, score, is_group):
    # usa txid del gruppo come linked_id se disponibile, altrimenti id del withdrawal
    if is_group and withdrawals[0].get('txid'):
        link_ref = withdrawals[0]['txid']
    else:
        link_ref = withdrawals[0]['id']

    for w in withdrawals:
        conn.execute(
            "UPDATE transfers SET linked_id=?, status='linked' WHERE id=?",
            (deposit['id'], w['id'])
        )
    conn.execute(
        "UPDATE transfers SET linked_id=?, status='linked' WHERE id=?",
        (link_ref, deposit['id'])
    )

    dt = deposit['timestamp'] - withdrawals[0]['timestamp']
    tag = f"group({len(withdrawals)})" if is_group else "single"
    addr_info = "addr match" if score == 2 else "qty/ts only"
    print(f"    LINK [{tag}][score={score}] dt={dt//60}min  {addr_info}")
    for w in withdrawals:
        print(f"      W: {w['id']}")
    print(f"      D: {deposit['id']}")


def link_by_qty_ts(conn, qty_tol_pct, window_h, before_m):
    window_sec   = window_h * 3600
    before_sec   = before_m * 60
    by_asset     = load_unmatched(conn)
    total_linked = 0
    all_unlinked_d = []
    all_unlinked_w = []

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
            matches, score, is_group = find_match(dep, available, qty_tol_pct, window_sec, before_sec)

            if matches:
                link_group(conn, matches, dep, score, is_group)
                for w in matches:
                    linked_withdrawal_ids.add(w['id'])
                total_linked += 1
            else:
                unlinked_d.append(dep)

        unlinked_w = [w for w in withdrawals if w['id'] not in linked_withdrawal_ids]
        all_unlinked_d.extend(unlinked_d)
        all_unlinked_w.extend(unlinked_w)

    conn.commit()
    print(f"\nFase 2 (qty/ts): {total_linked} deposit linkati")
    return total_linked, all_unlinked_d, all_unlinked_w


# ── EXPORT CSV ────────────────────────────────────────────────────────────────

def export_csv(conn, csv_path):
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM transfers WHERE status != 'linked' ORDER BY timestamp ASC"
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

def run(db_path, csv_path, qty_tol_pct=QTY_TOL_PCT, window_h=TIME_WINDOW_H, before_m=TIME_BEFORE_M):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("── Fase 1: txid linking ───────────────────────────────────────")
    link_by_txid(conn)

    print("\n── Fase 2: qty/ts/address linking ────────────────────────────")
    total_linked, unlinked_d, unlinked_w = link_by_qty_ts(conn, qty_tol_pct, window_h, before_m)

    print(f"\n══ RIEPILOGO ══════════════════════════════════════════════════")
    print(f"  Deposit senza provenienza: {len(unlinked_d)}")
    print(f"  Withdrawal in wallet:      {len(unlinked_w)}")

    if unlinked_d:
        print(f"\n── DEPOSIT SENZA PROVENIENZA ──────────────────────────────────")
        for d in unlinked_d:
            print(f"  {ts_str(d['timestamp'])}  {d['asset']:6s}  qty={d['qty']:.8f}  "
                  f"from={d['address_from'] or '—'}  to={d['address_to'] or '—'}  "
                  f"[{d['exchange'] or 'onchain'}]")

    if unlinked_w:
        print(f"\n── WITHDRAWAL IN WALLET ───────────────────────────────────────")
        for w in unlinked_w:
            print(f"  {ts_str(w['timestamp'])}  {w['asset']:6s}  qty={w['qty']:.8f}  "
                  f"from={w['address_from'] or '—'}  to={w['address_to'] or '—'}  "
                  f"[{w['exchange'] or 'onchain'}]")

    export_csv(conn, csv_path)
    conn.close()


if __name__ == '__main__':
    print(f"DB : {DB_PATH}\n")
    run(DB_PATH, CSV_PATH)
