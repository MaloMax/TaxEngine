from pathlib import Path
import sqlite3
import ccxt
import pandas as pd
from datetime import datetime
import requests

# === PATH BASE PROGETTO ===
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
PRICES_DIR = PROJECT_ROOT / "prices"
DB_PATH = PRICES_DIR / "price_history.db"


SYMBOL_ALIASES = {
    "MATIC": "POL",
    "DOT.S": "DOT",
}

class PriceProvider:

    def __init__(self):
        
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.init_db()
        
        self.cex_priority = ['bybit','binance', 'kraken', 'bitstamp', 'bitfinex']
        self._exchange_instances = {}
        self._exchange_markets = {}
        self.trade_price_history = {}
        self.exclude_from_trade_history = {
            "BTC", "ETH", "USDT", "USDC", "EUR"
        }
        # aggiungo anche quelli già nel DB
        self.exclude_from_trade_history.update(
            self.load_symbols_from_db()
        )
        
        # sempre in memoria
        self.df_eurusd = self._load_price_file("EURUSD.csv")
        self.df_btceur = self._load_price_file("BTCEUR.csv")

        # cache dinamica per altre fiat
        self._fiat_cache = {}

    def register_trade_price(self, base, quote, qty_base, qty_quote, timestamp):

        # Provo a ottenere EUR per uno dei due
        quote_eur = self.prezzo(quote, timestamp, True)

        if quote_eur:
            total_eur = quote_eur * qty_quote
            price_base = total_eur / qty_base
            self.register_token(base, price_base, timestamp)
            return  

        base_eur = self.prezzo(base, timestamp, True)
        
        if base_eur:
            total_eur = base_eur * qty_base
            price_quote = total_eur / qty_quote
            self.register_token(quote, price_quote, timestamp)
        
    def register_token(self, token, price_eur, timestamp):
        
        token = token.upper()

        # 🚫 Se è nella lista esclusione → non memorizzo
        if token in self.exclude_from_trade_history:
            return
        
        if token not in self.trade_price_history:
            self.trade_price_history[token] = []

        self.trade_price_history[token].append({
            "timestamp": timestamp,
            "price_eur": price_eur
        })
        
    def get_crypto_history_price(self, token, date):

        # normalizzazione data
        if isinstance(date, int):
            if date > 1e12:  # millisecondi
                date = datetime.utcfromtimestamp(date / 1000)
            else:
                date = datetime.utcfromtimestamp(date)

        if isinstance(date, datetime):
            day = date.strftime("%Y-%m-%d")
        elif isinstance(date, str):
            day = date
        else:
            raise ValueError("Unsupported date format")

        url = f"https://cryptohistory.one/api/{token}/{day}"

        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()

            if data.get("found"):
                if data.get("price_eur") is not None:
                    return float(data["price_eur"])
                else:
                    raise ValueError(f"Token {asset}  trovato su cryptohistory , ma non cambio in EURO ", data)

        except Exception:
            pass

        return None

    def get_closest_trade_price(self, token, date, max_days=30):

        if token not in self.trade_price_history:
            print(f"asset {token}  not in self.trade_price_history ")
            return None

        # --- normalizzazione input ---
        if isinstance(date, int):
            if date > 1e12:  # millisecondi
                date = date // 1000
        else:
            raise ValueError("date must be unix timestamp int")

        prices = self.trade_price_history[token]

        # Trovo il prezzo con distanza minima
        best = min(
            prices,
            key=lambda p: abs(p["timestamp"] - date)
        )

        # controllo distanza massima
        diff_days = abs(best["timestamp"] - date) / 86400
        
        print('Recupero prezzo da trade diff_days ',diff_days)

        if diff_days <= max_days:
            return best["price_eur"]

        return None

    def load_symbols_from_db(self):
        c = self.conn.cursor()
        c.execute("SELECT DISTINCT symbol FROM prices")
        rows = c.fetchall()
        return {row[0] for row in rows}

    def _load_price_file(self, filename):
        path = PRICES_DIR / filename
        df = pd.read_csv(path)
        df = df.set_index('timestamp')
        df = df.sort_index()
        return df
    
    def get_fiat_df(self, fiat):
        if fiat not in self._fiat_cache:
            filename = f"{fiat}EUR.csv"
            self._fiat_cache[fiat] = self._load_price_file(filename)

        return self._fiat_cache[fiat]

    def init_db(self):
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (symbol, timestamp)
            )
        ''')
        self.conn.commit()

    # =========================
    # API PUBBLICA
    # =========================
    def prezzo(self, asset, timestamp, allow_missing=False):
        
        asset = asset.upper()
        
        if self.isEuro(asset):
            return 1
        
        if asset == 'BTC':
            posInd = self.df_btceur.index.get_indexer([timestamp], method='nearest')[0]
            price = self.df_btceur.iloc[posInd]['price']
            return price
        
        if asset == "MXN":
            df_mxneur = self.get_fiat_df("MXN")
            posInd = df_mxneur.index.get_indexer([timestamp], method='nearest')[0]
            price = df_mxneur.iloc[posInd]['price']
            return price
        
        
        if self.isUsd(asset):
            posInd = self.df_eurusd.index.get_indexer([timestamp], method='nearest')[0]
            priceUSD = 1 / self.df_eurusd.iloc[posInd]['price']
            return priceUSD
        
        asset = SYMBOL_ALIASES.get(asset, asset)

        # 1️⃣ DB cache
        price = self._get_price_db(asset, timestamp)
        if price is not None:
            return price
        

        pairs_to_try = [
            f"{asset}/EUR",
            f"{asset}/BTC",
            f"{asset}/USD",
            f"{asset}/USDC",
            f"{asset}/USDT",
            f"{asset}/USDT:USDT"
        ]

        for ex_id in self.cex_priority:

            ex = self._get_exchange(ex_id)
            if not ex:
                continue

            symbols = self._exchange_markets.get(ex_id, [])
            
            if f"{asset}/EUR" in pairs_to_try:
                priceMulty = 1
            elif f"{asset}/BTC" in pairs_to_try:
                posInd = self.df_btceur.index.get_indexer([timestamp], method='nearest')[0]
                price = self.df_btceur.iloc[posInd]['price']
            else:
                posInd = self.df_eurusd.index.get_indexer([timestamp], method='nearest')[0]
                priceMulty = 1 / self.df_eurusd.iloc[posInd]['price']
                        
            for pair in pairs_to_try:
                if pair in symbols:
                    try:
                        print(f"Trovato {pair} su {ex_id}", datetime.utcfromtimestamp(timestamp))

                        raw_price = self.get_price_ccxt(ex_id, pair, timestamp)
                        
                        
                        if ex_id == 'bitmex' and pair == 'BMEX/USDT':
                            print (' ---------- ', ex_id, pair, datetime.utcfromtimestamp(timestamp) ,raw_price)
                            if raw_price is None:
                                self._save_price_db(asset, timestamp, 0)
                                return 0
                            
                        if raw_price:                        
                            RetPrice = raw_price * priceMulty
                            self._save_price_db(asset, timestamp, RetPrice)
                            return RetPrice

                    except Exception as e:
                        print(f"Errore fetch {pair} su {ex_id}: {e}")
        
            
        timestamp = self.normalize_day(timestamp)
        price = self._get_price_db(asset, timestamp)
        if price is not None:
            #print(f"[INFO] Price from DB giorno .cryptohistory.one for {asset} on {datetime.utcfromtimestamp(timestamp)} = {price}")
            return price
        
        price = self.get_crypto_history_price(asset, timestamp)
        if price:
            print(f"[INFO] Price Daily from cryptohistory.one for {asset} on {datetime.utcfromtimestamp(timestamp)} = {price}")
            self._save_price_db(asset, timestamp, price)
            return price            
        
        if allow_missing:
            return None 
        
        price = self.get_closest_trade_price(asset, timestamp)
        if price:
            print(f"[ALERT] Price recuperato da past trade for {asset} on {datetime.utcfromtimestamp(timestamp)} = {price} ")
            return price  
        

        raise ValueError(f"Impossibile recuperare prezzo per {asset} {datetime.utcfromtimestamp(timestamp)}   ")
        
    def _get_exchange(self, ex_id):
        if ex_id in self._exchange_instances:
            return self._exchange_instances[ex_id]

        try:
            ex = getattr(ccxt, ex_id)({'enableRateLimit': True})
            ex.load_markets()

            self._exchange_instances[ex_id] = ex
            self._exchange_markets[ex_id] = ex.symbols

            print(f"Inizializzato {ex_id}")
            return ex

        except Exception as e:
            print(f"Errore inizializzazione {ex_id}: {e}")
            return None
        
    # =========================
    # DB
    # =========================
    def _get_price_db(self, asset, timestamp):
                
        timestamp = self.normalize_hour(timestamp)
        
        asset = asset.upper()
        c = self.conn.cursor()

        # prende ultimo prezzo disponibile <= timestamp
        c.execute("""
            SELECT price FROM prices
            WHERE symbol=? AND timestamp=?
        """, (asset, timestamp))

        row = c.fetchone()
        return row[0] if row else None

    def _save_price_db(self, asset, timestamp, price):
        
        timestamp = self.normalize_hour(timestamp)

        c = self.conn.cursor()
        c.execute("""
            INSERT OR IGNORE INTO prices (symbol, timestamp, price)
            VALUES (?, ?, ?)
        """, (asset, timestamp, price))
        self.conn.commit()

    # ============================================================
    # IDENTIFICAZIONE VALUTA
    # ============================================================

    def isEuro(self, asset):
        return asset.upper() in ('EUR')

    def isUsd(self, asset):
        return asset.upper() in ('USD', 'USDT', 'USDC', 'DAI')
    
    def isTax(self, asset):
        return asset.upper() in ('EUR','USD', 'USDT', 'USDC', 'DAI', 'MXN')
    
    def normalize_hour(self, ts):
        """
        Rimuove minuti e secondi.
        Mantiene anno-mese-giorno-ora.
        Ritorna timestamp unix (secondi).
        """
        if ts > 1e12:  # millisecondi
            ts = ts / 1000

        dt = datetime.utcfromtimestamp(ts)
        dt = dt.replace(minute=0, second=0, microsecond=0)
        
        return int(dt.timestamp())
    
    
    def normalize_day(self, ts):
            
        """
        Rimuove ore, minuti e secondi.
        Ritorna inizio giorno UTC.
        """
        if ts > 1e12:  # millisecondi
            ts = ts / 1000

        dt = datetime.utcfromtimestamp(ts)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp())
    
    # =========================
    # CCXT
    # =========================
    def get_price_ccxt(self, exchange_id, symbol, timestamp):

        try:
            exchange_class = getattr(ccxt, exchange_id)()
            ohlcv = exchange_class.fetch_ohlcv(
                symbol,
                timeframe='1m',
                since=timestamp * 1000,
                limit=2
            )

            return ohlcv[-1][4] if ohlcv else None

        except Exception as e:
            print(f"CCXT error {exchange_id}/{symbol}: {e}")
            return None