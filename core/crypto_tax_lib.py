import os, sys, sqlite3, pandas as pd
import csv
from datetime import datetime, timezone
import re
import numpy as np
import numbers


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "Data", "DataBase")
DB_PATH = os.path.join(DATABASE_DIR, "crypto_tax.db")

from price_provider import PriceProvider

# I TUOI PREZZI (cache primaria)
PREZZI_STORICI = {
    'BTC': {2017: 14156, 2018: 3743, 2019: 7194, 2020: 29041, 2021: 46306, 2022: 16547, 2023: 42266, 2024: 93429, 2025: 87509},  
    'ETH': {2017: 757, 2018: 133, 2019: 130, 2020: 738, 2021: 3683, 2022: 1197, 2023: 2281, 2024: 3333, 2025: 2967},  
    'LTC': {2018: 31, 2019: 37, 2020: 124, 2021: 150, 2022: 72, 2023: 73, 2024: 98},  
    'XRP': {2017: 2.30, 2018: 0.35, 2019: 0.19, 2020: 0.22, 2021: 0.83},  
    'MXN': {2017: 0.052, 2018: 0.050, 2019: 0.050, 2020: 0.049, 2021: 0.048},  
    'TUSD': {2019: 1.00, 2020: 1.00, 2021: 1.00},
    'PAN': {2018: 0.01, 2019: 0.015, 2020: 0.03}
}

USD_TO_EUR_31DIC = {2017: 0.8333, 2018: 0.8734, 2019: 0.8913, 2020: 0.8753,
                    2021: 0.8446, 2022: 0.9382, 2023: 0.9065, 2024: 0.9259, 2025: 0.8523}


class CryptoTaxLib:
    
    """
    Libreria di supporto al CryptoTaxEngine.

    Responsabilit?:
    - Gestione DB (trasferimenti, cache prezzi, report)
    - Recupero prezzi storici (cache ? DB ? CCXT)
    - Conversione EUR/USD
    - Normalizzazione valori numerici e timestamp
    - Persistenza CSV di audit

    Non contiene logica fiscale.
    """
    
    def __init__(self):
        self.db_path = DB_PATH
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.init_db()
        print("DB Tax pronto")
                
        self.price_lib = PriceProvider()
        print("PriceProvider pronto")
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode=WAL')
        c = conn.cursor()
        
        # Prelievi con costo
        c.execute('''
        CREATE TABLE IF NOT EXISTS trasferimenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL, address_dest TEXT NOT NULL, qty REAL NOT NULL,
            cost_unitario_eur REAL NOT NULL, data_prelievo TEXT NOT NULL,
            source_exchange TEXT NOT NULL, status TEXT DEFAULT 'pending',
            UNIQUE(asset, address_dest, data_prelievo, source_exchange)
        )
        ''')
        
        # Depositi non matchati
        c.execute('''
        CREATE TABLE IF NOT EXISTS depositi_non_matchati (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL, address_source TEXT NOT NULL, qty REAL NOT NULL,
            data_deposito TEXT NOT NULL, dest_exchange TEXT NOT NULL,
            UNIQUE(dest_exchange, asset, address_source, data_deposito)
        )
        ''')
        

            # TAB 4: Report fiscali per exchange/anno
        c.execute('''
        CREATE TABLE IF NOT EXISTS report_fiscali (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL, anno INTEGER NOT NULL,
            saldo_31dic_eur REAL DEFAULT 0,
            plusvalenze_eur REAL DEFAULT 0,
            minusvalenze_eur REAL DEFAULT 0,
            diversi_plus_eur REAL DEFAULT 0,
            diversi_minus_eur REAL DEFAULT 0,
            data_calcolo TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(exchange, anno)
        )
        ''')

        conn.commit()
        conn.close()
    
    # DB: Registra prelievo
    def registra_prelievo(self, asset, address, qty, cost_unit, timestamp, exchange):
        conn = sqlite3.connect(self.db_path)
        try:
            pd.DataFrame([{
                'asset': asset.upper(), 'address_dest': address,
                'qty': qty, 'cost_unitario_eur': cost_unit,
                'data_prelievo': timestamp, 'source_exchange': exchange
            }]).to_sql('trasferimenti', conn, if_exists='append', index=False)
            #print(f" {exchange}: {qty:.6f} {asset} @ {cost_unit:.1f} → {address[:10]}")
        except sqlite3.IntegrityError:
            pass
        conn.commit()
        conn.close()
    
    # DB: Match deposito
    def match_deposito(self, asset, address, qty, dest_exchange, timestamp):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT id, cost_unitario_eur FROM trasferimenti 
            WHERE UPPER(asset)=? AND address_dest=? 
            AND status='pending' AND qty>=?
            ORDER BY data_prelievo ASC LIMIT 1
        ''', conn, params=(asset.upper(), address, qty))
        
        if not df.empty:
            cost = df.iloc[0]['cost_unitario_eur']
            conn.execute("UPDATE trasferimenti SET status='matched' WHERE id=?", (df.iloc[0]['id'],))
            conn.commit()
            conn.close()
            return {'matched': True, 'cost': cost}
        
        # Salva non-match
        try:
            pd.DataFrame([{
                'asset': asset.upper(), 'address_source': address,
                'qty': qty, 'data_deposito': timestamp, 'dest_exchange': dest_exchange
            }]).to_sql('depositi_non_matchati', conn, if_exists='append', index=False)
        except sqlite3.IntegrityError:
            pass
        
        conn.commit()
        conn.close()
        return {'matched': False, 'cost': 0}
    
    # Report
    def report_status(self):
        conn = sqlite3.connect(self.db_path)
        stats = pd.read_sql_query('SELECT status, COUNT(*) cnt FROM trasferimenti GROUP BY status', conn)
        print("\n STATUS:", stats.to_dict())
        conn.close()
        
    def prezzo_31dic(self, asset, year):

        asset = asset.upper()

        # 1. I TUOI PrezziStorici
        if asset in PREZZI_STORICI and year in PREZZI_STORICI[asset]:
            price = PREZZI_STORICI[asset][year]
            return round(price * USD_TO_EUR_31DIC.get(year, 1), 2)
        
        price_usd = self.price_lib.prezzo(asset, self.to_timestamp(f'{year}-12-31 23:59:00'))
        if price_usd:
            return round(price_usd * USD_TO_EUR_31DIC.get(year, 1), 2)
        

        raise ValueError(f"Problema a recuperare prezzo 31 12 {year} {asset}")


    def to_float(self, val, field_name=''):
        """
        Converte qualsiasi numero/stringa numerica in float.
        Supporta:
        - int
        - float
        - numpy int/float
        - stringhe numeriche
        - None / '' / '-' -> None
        """

        label = f" ({field_name})" if field_name else ""

        if val is None:
            return None

        # Gestione NaN (pandas / numpy)
        if isinstance(val, float) and np.isnan(val):
            return None

        # Tutti i numerici (Python + numpy)
        if isinstance(val, numbers.Real):
            return float(val)

        # Stringhe
        if isinstance(val, str):
            v = val.strip()

            if v in ('', '-', 'nan', 'NaN', 'None'):
                return None
            
            
                v = v.replace(',', '.')
            try:
                return float(v)
            except ValueError as e:
                raise ValueError(
                    f"Valore non convertibile in float{label}: {val}"
                ) from e

        raise ValueError(
            f"Tipo non gestito in to_float{label}: {type(val)} -> {val}"
        )

    def to_timestamp(self, val, field_name=''):
        """
        Normalizza qualsiasi timestamp in UNIX a 10 cifre (secondi, UTC).

        Supporta:
        - datetime
        - pandas.Timestamp
        - int / float
        - ISO 8601 (con T, spazio, Z, offset)
        - millisecondi (13 cifre)
        - microsecondi (16 cifre)
        """

        label = f" ({field_name})" if field_name else ""

        if val is None:
            raise ValueError(f"Timestamp None{label}")

        # --------------------------------------------------
        # DATETIME / PANDAS
        # --------------------------------------------------
        if isinstance(val, (datetime, pd.Timestamp)):
            if val.tzinfo is None:
                # assumiamo UTC se naive (evitiamo conversioni locali)
                val = val.replace(tzinfo=timezone.utc)
            else:
                val = val.astimezone(timezone.utc)

            return int(val.timestamp())

        # --------------------------------------------------
        # NUMERICO
        # --------------------------------------------------
        if isinstance(val, (int, float)):
            ts = int(val)

        # --------------------------------------------------
        # STRINGA
        # --------------------------------------------------
        elif isinstance(val, str):
            v = val.strip()

            if not v:
                raise ValueError(f"Timestamp vuoto{label}")

            # Caso numerico puro
            if re.fullmatch(r"\d+", v):
                ts = int(v)
            else:
                # Normalizzazione ISO
                # gestisce Z → +00:00
                if v.endswith("Z"):
                    v = v[:-1] + "+00:00"

                try:
                    dt = datetime.fromisoformat(v)

                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)

                    return int(dt.timestamp())

                except Exception:
                    raise ValueError(f"Formato timestamp non riconosciuto{label}: {val}")

        else:
            raise ValueError(
                f"Tipo non gestito in to_timestamp{label}: {type(val)} -> {val}"
            )

        # --------------------------------------------------
        # NORMALIZZAZIONE NUMERICA
        # --------------------------------------------------
        digits = len(str(abs(ts)))

        if digits == 10:          # secondi
            return ts
        elif digits == 13:        # millisecondi
            return ts // 1000
        elif digits == 16:        # microsecondi
            return ts // 1_000_000
        elif digits > 16:
            # fallback: riduce fino a 10 cifre
            while len(str(abs(ts))) > 10:
                ts //= 10
            return ts

        raise ValueError(
            f"Lunghezza timestamp inattesa ({digits}){label}: {ts}"
        )
    def get_cex_paths(self,cex_name):

        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        data_dir = os.path.join(base_dir, "Data")
        Reports_dir = os.path.join(data_dir, "CexReports")
        Events_dir = os.path.join(data_dir, "Events")
        Cex_dir = os.path.join(Reports_dir, cex_name)

        return {
            "report": Cex_dir,
            "events": Events_dir
        } 
        
        
    def append_event_to_csv(self, filename, row_dict):
        """
        Appende una riga ad un CSV nella cartella Result.
        Se il file non esiste, crea il file e scrive l'intestazione.
        """
        headers = list(row_dict.keys())

        file_exists = os.path.isfile(filename)

        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                writer.writeheader()

            writer.writerow(row_dict)

    def reset_result_file(self,filename):

        if os.path.exists(filename):
            os.remove(filename)
            
    def salva_report_fiscale(self, exchange, anno, saldo=0, plus=0, minus=0, redditiP=0, redditiM=0):
        """Salva/aggiorna report anno-exchange."""
        conn = sqlite3.connect(self.db_path)
        try:
            pd.DataFrame([{
                'exchange': exchange, 'anno': anno,
                'saldo_31dic_eur': saldo, 'plusvalenze_eur': plus,
                'minusvalenze_eur': minus, 'diversi_plus_eur': redditiP, 'diversi_minus_eur': redditiM
            }]).to_sql('report_fiscali', conn, if_exists='append', index=False)
            print(f" REPORT {exchange}-{anno}: Saldo {saldo:,.0f}€ | NetPlus {plus-minus:,.2f}€")
        except sqlite3.IntegrityError:
            # Update esistente
            conn.execute('''
                UPDATE report_fiscali SET saldo_31dic_eur=?, plusvalenze_eur=?, 
                minusvalenze_eur=?, diversi_plus_eur=?,  diversi_minus_eur=?, data_calcolo=CURRENT_TIMESTAMP
                WHERE exchange=? AND anno=?
            ''', (saldo, plus, minus, redditiP, redditiM, exchange, anno))
            print(f" UPDATE {exchange}-{anno}")
        conn.commit()
        conn.close()
        
        
    def report_consolidato(self):
        """Somma TUTTI exchange per RW/RT."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT anno, 
                   SUM(saldo_31dic_eur) saldo_tot,
                   SUM(plusvalenze_eur) plus_tot,
                   SUM(minusvalenze_eur) minus_tot,
                   SUM(diversi_plus_eur) diversi_plus_eur,
                   SUM(diversi_minus_eur) diversi_minus_eur
            FROM report_fiscali GROUP BY anno ORDER BY anno
        ''', conn)
        print("\n CONSOLIDATO RW/RT:")
        print(df.to_markdown(index=False))
        conn.close()
        return df


  
tax_lib = CryptoTaxLib()
