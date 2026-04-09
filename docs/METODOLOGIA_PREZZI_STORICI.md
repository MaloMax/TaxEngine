# Metodologia prezzi storici

Il sistema di determinazione dei prezzi è basato su una logica a priorità.

Per ogni richiesta di prezzo, TaxEngine utilizza una sequenza di fonti ordinate.
La prima fonte disponibile valida viene utilizzata.

## Ordine di priorità

1. **Casi deterministici**

   * EUR = 1
   * Stablecoin e conversioni dirette (USD, USDT, ecc.)
   * BTC/EUR da dataset locale

2. **Dati locali**

   * CSV storici (es. EURUSD, BTCEUR)
   * Database locale SQLite (`price_history.db`)

3. **Exchange (ccxt)**

   * Ricerca del prezzo su exchange prioritari
   * Coppie tentate: EUR, BTC, USD, USDT, USDC

4. **API esterne**

   * Servizi pubblici (es. cryptohistory)
   * Utilizzati solo come fallback

5. **Dati derivati dai trade**

   * Prezzi ricostruiti da operazioni precedenti
   * Utilizzati solo se entro una soglia temporale

6. **Errore**

   * Se nessuna fonte è disponibile, viene sollevata un'eccezione

---

## Principi

Il sistema è progettato per essere:

* deterministico (stesso input → stesso output)
* riproducibile nel tempo
* indipendente da API live quando possibile
* progressivamente migliorabile tramite cache locale

---

## Persistenza

I prezzi recuperati vengono salvati localmente nel database SQLite,
in modo da evitare variazioni future dovute a cambiamenti delle API.

---

## Note

La logica è in evoluzione e può essere estesa con nuove fonti o strategie,
mantenendo sempre la priorità deterministica.
