# TaxEngine

Motore deterministico per il calcolo fiscale di operazioni crypto multi-exchange (CEX + on-chain).

Obiettivo: ricostruire in modo coerente, verificabile e replicabile la fiscalità crypto nel tempo.

---

## Disclaimer

Questo software è fornito a scopo esclusivamente educativo e informativo.

Non costituisce consulenza fiscale, legale o finanziaria.

L'utilizzo è sotto la piena responsabilità dell'utente.
I risultati devono essere verificati con un professionista qualificato.

---

## Perché questo progetto

Gli strumenti fiscali disponibili online sono spesso:

* opachi
* non replicabili
* dipendenti da API esterne
* non verificabili nel tempo
* difficili da auditare

TaxEngine nasce con un approccio diverso:

👉 Ogni numero deve poter essere spiegato.

---

## Obiettivi principali

* Importare report CSV originali dai vari CEX
* Normalizzare ogni evento (trade, depositi, prelievi, fee)
* Risolvere prezzi storici in EUR in modo deterministico
* Applicare una metodologia documentata
* Generare risultati replicabili nel tempo

---

## Principi di progettazione

Il progetto è costruito per essere:

* Deterministico
* Auditabile
* Modulare
* Indipendente dall’exchange
* Indipendente da API esterne
* Riproducibile a distanza di anni

I prezzi storici vengono salvati localmente per evitare variazioni nel tempo.

---

## Struttura del progetto

```
TaxEngine/
├── core/                        
├── converters/                  
├── prices/                      
├── Data/                        # dati locali (non versionati)
│   ├── CexReports/              # report originali per exchange
│   ├── Events/                  # eventi normalizzati
│   └── ...                      
├── docs/                        
└── README.md
```

⚠️ La cartella `Data/` contiene dati sensibili locali e non è versionata.

---

## Utilizzo attuale

Il progetto è in fase di sviluppo e non esiste ancora un flusso automatico completo.

Attualmente:

* i report dei CEX vengono salvati manualmente nella cartella
  `Data/CexReports/<nome_exchange>/`
* ogni exchange ha il proprio formato e richiede un parsing dedicato
* gli eventi vengono progressivamente normalizzati
* i dati vengono poi utilizzati per costruire il motore fiscale

Esempio reale:

```
Data/
└── CexReports/
    └── Bitfinex/
        ├── MaloMax_trades.csv
        ├── MaloMax_movements.csv
        └── ...
```

Il flusso completo di elaborazione è in fase di costruzione.

---

## Prezzi Storici

Il sistema utilizza diverse fonti per determinare i prezzi storici:

- dati locali (CSV e database)
- logiche deterministiche interne
- integrazioni esterne quando necessario

I prezzi vengono salvati localmente per garantire coerenza nel tempo.

Per dettagli sulla gestione dei prezzi:
vedi `docs/METODOLOGIA_PREZZI_STORICI.md`

---

## Stato del progetto

Progetto personale in evoluzione.

Attualmente in sviluppo attivo:

* parsing multi-exchange
* gestione eventi
* sistema prezzi storico locale

Da sviluppare:

* pipeline completa automatica
* supporto DeFi
* output fiscale finale

---

## Roadmap

* [x] Parsing base CEX
* [x] Struttura eventi
* [x] Sistema prezzi storico
* [ ] Pipeline completa automatica
* [ ] Supporto DeFi
* [ ] Output fiscale finale

---

## Contributi

Il progetto è aperto a contributi.

Se vuoi partecipare:

* apri una issue
* proponi miglioramenti
* contribuisci con nuovi converter

---

## Autore

MaloMax

Progetto nato da un'esigenza reale di ricostruzione fiscale crypto.
