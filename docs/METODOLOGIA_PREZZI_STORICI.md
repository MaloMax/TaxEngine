1. Fonte dati

I prezzi storici BTC/EUR sono acquisiti tramite API pubbliche dell’exchange Bitstamp.

2. Risoluzione temporale

Viene utilizzata risoluzione oraria (H1) per l’intero periodo storico.

3. Determinazione del prezzo

Per ogni intervallo orario il prezzo è calcolato come media aritmetica della candela:

price = (open + high + low + close) / 4
4. Selezione del prezzo per evento

Per ogni operazione fiscale viene utilizzato il prezzo con timestamp più vicino.

Non vengono applicate interpolazioni o correzioni manuali.

5. Principi adottati

Il metodo è oggettivo, uniforme, ripetibile e non discrezionale.