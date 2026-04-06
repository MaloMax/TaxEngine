# Tax Engine — Ravvedimento Operoso Crypto (FIFO Italia)
# Repo: https://github.com/MaloMax/TaxEngine

## Struttura progetto
```
TaxEngine/
├── core/
│   ├── crypto_tax_engine.py   # FIFO engine (_consume_fifo / _add_fifo)
│   ├── crypto_tax_lib.py
│   └── price_provider.py
├── prices/                    # prezzi storici in locale (deterministici)
│   ├── price_history.db
│   ├── EURUSD.csv
│   ├── BTCEUR.csv
│   └── MXNEUR.csv
├── converters/                # un modulo per ogni CEX → produce *_event.csv
├── docs/
│   ├── METODOLOGIA_CALCOLO_FISCALE.md
│   └── METODOLOGIA_PREZZI_STORICI.md
├── Data/                      # NON versionato (.gitignore)
│   ├── transfers.db           # SQLite trasferimenti
│   ├── transfers_export.csv   # export audit
│   ├── addresses.py           # dict keys {address: group_id}
│   └── AnalisiOC/btc_data/tx_raw/  # pickle mempool.space per indirizzo
├── build_transfers_db.py
├── import_onchain.py
└── export_transfers.py
```

## Colonne *_event.csv
`timestamp, type, asset, qty, fee, asset_b, qty_b, fee_b, address, Exchange, idx, File`

## Tipi evento
`buy, sell, deposit, withdrawal, reward, airdrop, staking, no_tax, ...`

## Schema tabella transfers (SQLite)
| campo | note |
|---|---|
| id | `{Exchange}_{type}_{asset}_{ts}_{idx}` / `onchain_{deposit|withdrawal}_{txid}_{addr}` |
| type | deposit / withdrawal |
| asset | BTC, ... |
| qty | float |
| fee | float |
| timestamp | Unix int |
| exchange | nome CEX o None |
| txid | hash onchain |
| address_from | |
| address_to | |
| linked_id | FK manuale per propagare costo FIFO |
| source | `cex` / `onchain` |
| status | `unmatched` / `internal` / `linked` |

## Script
| file | funzione |
|---|---|
| `build_transfers_db.py` | legge `*_event.csv` → popola transfers.db (movimenti CEX) |
| `import_onchain.py` | legge pickle mempool.space → inserisce BTC onchain nel DB |
| `export_transfers.py` | dump transfers.db → CSV ordinato per timestamp (audit) |

## Problema centrale
Collegare withdrawal CEX ↔ deposit onchain ↔ deposit CEX per propagare costo di carico FIFO.
Linking via `linked_id` — manuale oggi, auto-linking da costruire.

## Auto-linking (prossimo step)
Criteri match:
- stesso `asset`
- stessa `qty` (o `qty - fee`)
- `timestamp` entro finestra (es. 24h)
- `address_to` withdrawal == `address_to` deposit onchain

## Pattern tipico
```
Bitpanda (buy) → CEX withdrawal → onchain deposit → [move] → CEX deposit
```
Esempio reale trovato:
- `Bitpanda_withdrawal_BTC_1483789775_8` ↔ `onchain_deposit_e3c88e..._1M39a7Q...`
- stessa qty `0.01455776 BTC`, distanza ~5 min