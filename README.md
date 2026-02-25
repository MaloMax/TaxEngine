# TaxEngine

Motore di calcolo fiscale per operazioni crypto multi-exchange.

---

## ? Obiettivo

TaxEngine permette di:

- Importare report da diversi CEX
- Normalizzare eventi (trade, depositi, prelievi, fee)
- Risolvere prezzi storici in EUR
- Generare un risultato fiscale coerente e verificabile

Il progetto è pensato per essere:
- deterministico
- auditabile
- modulare
- indipendente dall’exchange

---

## ? Struttura del progetto

```
TaxEngine/
?
??? Library/                      # motore fiscale + gestione prezzi
?
??? Exchanges/                    # parser per ciascun exchange
?
??? Data/
?   ??? DataBase/                 # eventuali database locali
?   ?
?   ??? ExchangesData/
?       ??? Kraken/
?       ?   ??? Dati/             # report originali scaricati dal CEX (non versionati)
?       ?   ??? Kraken_cex_report.csv
?       ?   ??? Kraken_events.csv
?       ?   ??? Kraken_tax.csv
?       ?
?       ??? BitMex/
?       ??? BitFinex/
?       ??? ...
?
??? README.md
```

---

## ? Prezzi Storici

La cartella `Library` contiene:

- database prezzi (`price_history.db`)
- CSV di riferimento (EURUSD, BTCEUR, MXNEUR)
- logica di risoluzione prezzi (`price_provider.py`)

I prezzi sono deterministici e salvati localmente per garantire coerenza nel tempo.

---

## ? Cartella Dati

La cartella `Dati/` deve contenere:

- report CSV esportati dai vari exchange
- file originali non modificati

? Non è pensata per essere tracciata su GitHub.

---
## ? Gestione report CEX

Per ogni exchange:

- `Dati/` contiene i file originali scaricati
- I file vengono poi copiati e rinominati in:
  - `CEX_cex_report.csv`

La struttura è pensata per:

- mantenere gli originali intatti
- separare parsing e calcolo
- poter rieseguire il motore in modo replicabile


## ? Filosofia

Ogni movimento viene:

1. Parsato
2. Normalizzato
3. Valorizzato in EUR
4. Elaborato secondo la metodologia documentata

L’obiettivo non è solo ottenere un numero,
ma poter spiegare ogni singolo passaggio del calcolo.

---

## ? Stato del progetto

Progetto personale in evoluzione continua.
Pensato per utilizzo pluriennale.

---

## ? Autore

MaloMax