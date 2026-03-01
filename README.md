TaxEngine

Motore deterministico di calcolo fiscale per operazioni crypto multi-exchange.

TaxEngine nasce per risolvere un problema concreto:
ricostruire in modo coerente, verificabile e replicabile la fiscalità di operazioni crypto provenienti da più exchange centralizzati (CEX).

Perché questo progetto

Gli strumenti fiscali disponibili online sono spesso:

opachi

non replicabili

dipendenti da API esterne

non verificabili nel tempo

difficili da auditare

TaxEngine è progettato con un obiettivo diverso:

Ogni numero deve poter essere spiegato.

Obiettivi principali

Importare report CSV originali dai vari CEX

Normalizzare ogni evento (trade, depositi, prelievi, fee)

Risolvere prezzi storici in EUR in modo deterministico

Applicare una metodologia documentata

Generare risultati fiscali replicabili nel tempo

Principi di progettazione

Il progetto è costruito per essere:

Deterministico

Auditabile

Modulare

Indipendente dall’exchange

Indipendente da API esterne

Riproducibile a distanza di anni

I prezzi storici vengono salvati localmente per evitare variazioni nel tempo.

## Struttura del progetto

```text
TaxEngine/
├── core/                        # motore fiscale e logica principale
│   ├── crypto_tax_engine.py
│   ├── crypto_tax_lib.py
│   └── price_provider.py
├── prices/                      # database e CSV prezzi storici
│   ├── price_history.db
│   ├── EURUSD.csv
│   ├── BTCEUR.csv
│   └── MXNEUR.csv
├── converters/                  # parser specifici per ciascun CEX
├── Data/                        # dati locali (non versionati)
├── docs/                        # documentazione metodologica
│   ├── METODOLOGIA_CALCOLO_FISCALE.md
│   └── METODOLOGIA_PREZZI_STORICI.md
└── README.md
```
Come funziona

Ogni movimento viene processato in quattro fasi:

Parsing del report originale

Normalizzazione dell’evento

Risoluzione del prezzo storico in EUR

Elaborazione secondo la metodologia fiscale

Il risultato non è solo un totale, ma una ricostruzione coerente di ogni passaggio.

Prezzi Storici

I prezzi sono:

salvati in locale

non dipendenti da chiamate API live

coerenti nel tempo

deterministici

Il sistema utilizza:

CSV di riferimento (EURUSD, BTCEUR, ecc.)

un database SQLite locale

una logica di fallback documentata

Questo garantisce che lo stesso input produca sempre lo stesso output.

Gestione dei report CEX

Per ogni exchange:

I file originali vengono conservati intatti

Il parsing è separato dalla logica fiscale

Ogni exchange ha il proprio modulo dedicato

Questo permette:

aggiunta di nuovi CEX senza modificare il motore centrale

controllo completo del flusso dati

audit indipendente per singolo exchange

Stato del progetto

Progetto personale in evoluzione continua.
Pensato per utilizzo pluriennale.

Attualmente supporta:

parsing modulare

risoluzione prezzi multi-valuta

database storico locale

struttura espandibile per nuovi exchange

Requisiti

Python 3.10+

SQLite

pandas

ccxt (per eventuali integrazioni)

Filosofia

TaxEngine non è un tool “magico”.

È un motore trasparente.

L’obiettivo non è solo ottenere un numero finale,
ma poter spiegare ogni singolo passaggio del calcolo.

Autore

MaloMax
