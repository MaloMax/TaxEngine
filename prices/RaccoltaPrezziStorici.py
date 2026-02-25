import ccxt
import pandas as pd
import time
from datetime import datetime

exchange = ccxt.bitstamp({
    'enableRateLimit': True
})

markets = exchange.load_markets()
symbols = list(markets.keys())
print(symbols)


symbol = 'BTC/EUR'
timeframe = '1h'

since = exchange.parse8601('2017-01-01T00:00:00Z')
all_data = []

while since < exchange.parse8601('2026-01-01T00:00:00Z'):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
    
    if not ohlcv:
        break
    
    all_data.extend(ohlcv)
    since = ohlcv[-1][0] + 1
    time.sleep(exchange.rateLimit / 1000)

df = pd.DataFrame(all_data, columns=['timestamp','open','high','low','close','volume'])
df['price'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

df['timestamp'] = df['timestamp'] // 1000  # ms → sec
df[['timestamp','price']].to_csv("BTCEUR.csv", index=False)

symbol = 'EUR/USD'
timeframe = '1h'

since = exchange.parse8601('2017-01-01T00:00:00Z')
all_data = []

while since < exchange.parse8601('2026-01-01T00:00:00Z'):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
    
    if not ohlcv:
        break
    
    all_data.extend(ohlcv)
    since = ohlcv[-1][0] + 1
    time.sleep(exchange.rateLimit / 1000)

df = pd.DataFrame(all_data, columns=['timestamp','open','high','low','close','volume'])
df['price'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

df['timestamp'] = df['timestamp'] // 1000  # ms → sec
df[['timestamp','price']].to_csv("EURUSD.csv", index=False)








