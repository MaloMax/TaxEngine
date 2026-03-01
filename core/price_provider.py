from pathlib import Path
import sqlite3
import ccxt
import pandas as pd
from datetime import datetime

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
        self.cex_priority = ['binance', 'kraken', 'bitstamp', 'bitmex', 'bitfinex']
        self._exchange_instances = {}
        self._exchange_markets = {}
        
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.init_db()
        
        # sempre in memoria
        self.df_eurusd = self._load_price_file("EURUSD.csv")
        self.df_btceur = self._load_price_file("BTCEUR.csv")

        # cache dinamica per altre fiat
        self._fiat_cache = {}

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
    def prezzo(self, asset, timestamp):
        
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

        print(f"Scarica da CCXT {asset} {timestamp}")


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
                        print(f"Trovato {pair} su {ex_id}")

                        raw_price = self.get_price_ccxt(ex_id, pair, timestamp)
                        
                        
                        if ex_id == 'bitmex' and pair == 'BMEX/USDT':
                            print (' ---------- ', ex_id, pair, datetime.utcfromtimestamp(timestamp) ,raw_price)
                            if raw_price is None:
                                self._save_price_db(asset, timestamp, 0)
                                return 0
                            
                        if raw_price is None:
                            raise ValueError(f"Prezzo non disponibile per {pair} su {ex_id}")
                        
                        RetPrice = raw_price * priceMulty

                        self._save_price_db(asset, timestamp, RetPrice)
                        return RetPrice

                    except Exception as e:
                        print(f"Errore fetch {pair} su {ex_id}: {e}")
                        
        raise ValueError(f"Impossibile recuperare prezzo per {asset}")
        
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
                
        asset = asset.upper()
        c = self.conn.cursor()

        # prende ultimo prezzo disponibile <= timestamp
        c.execute("""
            SELECT price FROM prices
            WHERE symbol=?
            AND timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (asset, timestamp))

        row = c.fetchone()
        return row[0] if row else None

    def _save_price_db(self, asset, timestamp, price):

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