"""
export_transfers.py
Esporta tutto il DB transfers in CSV ordinato per timestamp.
"""

import os
import sqlite3
import csv
import datetime

DB_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers.db")
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "transfers_export.csv")

def ts_to_str(ts):
    if ts is None:
        return ""
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT * FROM transfers ORDER BY timestamp ASC"
    ).fetchall()

    if not rows:
        print("DB vuoto o tabella transfers non trovata.")
        conn.close()
        exit()

    cols = rows[0].keys()

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            d = dict(row)
            d['timestamp'] = ts_to_str(d.get('timestamp'))
            writer.writerow(d)

    conn.close()
    print(f"Esportati {len(rows)} record → {CSV_PATH}")