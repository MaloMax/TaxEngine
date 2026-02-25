import sqlite3
from collections import defaultdict
from datetime import datetime
import numpy as np
import pandas as pd
import os


class CryptoTaxEngine:
    '''
    ============================================================
    CRYPTO TAX ENGINE
    ============================================================

    Implementa il modello descritto in METODOLOGIA_CALCOLO_FISCALE.md.

    PRINCIPI APPLICATI:
    - Metodo FIFO per determinazione costo fiscale
    - Plus/minus generate solo su eventi imponibili (SELL)
    - Snapshot saldi al 31/12
    - Trasferimenti inter-exchange non imponibili
    - Reward/Airdrop trattati come redditi diversi
    - Stato completamente in memoria

    STRUTTURE PRINCIPALI:
        purchases  ? lotti FIFO (asset ? [(qty, costo_unitario)])
        balances   ? quantit� correnti per asset
        total_plus ? plusvalenze per anno
        total_minus ? minusvalenze per anno
        redditi_diversi ? redditi staking/airdrop per anno
    ============================================================
    '''
    
    
    def __init__(self, tax_lib, exchange_name):

        # Riferimento alla libreria fiscale (DB, prezzi, match depositi)
        self.tax_lib = tax_lib
        self.exchange = exchange_name
        self.debug = False

        # ============================================================
        # STATO IN MEMORIA
        # ============================================================

        # Lotti FIFO per ogni asset
        self.purchases = defaultdict(list)

        # Saldi correnti
        self.balances = defaultdict(float)
        self.balances['EUR'] = 0.0

        # Risultati fiscali annuali
        self.total_plus = defaultdict(float)
        self.total_minus = defaultdict(float)

        self.diversi_plus = defaultdict(float)
        self.diversi_minus = defaultdict(float)
        
        # Snapshot 31 dicembre
        self.year_end_balances = {}
        self.current_year = None


    # ============================================================
    # GESTIONE LOTTI FIFO
    # ============================================================
    # - Ogni acquisto genera un lotto
    # - Ogni vendita consuma i lotti pi� vecchi
    # - NON � media globale, ma media dei lotti consumati
    # ============================================================

    def _consume_fifo(self, asset, qty):

        remaining = qty
        total_cost = 0.0
        invalid = False

        # Consumo lotti in ordine cronologico
        while remaining > 0 and self.purchases.get(asset):

            last_qty, last_cost = self.purchases[asset].pop(0)
            used = min(remaining, last_qty)

            if last_cost == 0:
                invalid = True

            total_cost += used * last_cost
            remaining -= used

            # Se lotto non completamente consumato lo reinserisco
            if last_qty > used:
                self.purchases[asset].append((last_qty - used, last_cost))

        if invalid or qty == 0:
            return 0.0

        return total_cost / qty




    # ============================================================
    # PROCESSAMENTO EVENTO
    # ============================================================
    # Evento gi� normalizzato dal parser exchange.
    #
    # Tipi supportati:
    #   - buy
    #   - sell
    #   - reward / airdrop
    #   - withdraw
    #   - deposit
    # ============================================================

    def process_event(self, event):

        ts = self.tax_lib.to_timestamp(event['timestamp'])
        year = datetime.utcfromtimestamp(ts).year
        typ = event['type']
        tsStr = str(ts)
        plus = 0
        diversi = 0

        # --------------------------------------------------------
        # Normalizzazione dati numerici
        # --------------------------------------------------------
        asset = event['asset']
        qty = self.tax_lib.to_float(event['qty'], 'qty '+tsStr)
        fee = self.tax_lib.to_float(event['fee'], 'fee '+tsStr)
        fee = 0.0 if pd.isna(fee) else fee

        asset_b = event.get('asset_b', '')
        qty_b = self.tax_lib.to_float(event.get('qty_b', 0), 'qty_b '+tsStr)
        fee_b = self.tax_lib.to_float(event.get('fee_b', 0), 'fee_b '+tsStr)
        fee_b = 0.0 if pd.isna(fee_b) else fee_b

        # --------------------------------------------------------
        # Gestione cambio anno fiscale
        # --------------------------------------------------------
        if self.current_year and self.current_year != year:
            self.year_end_balances[self.current_year] = dict(self.balances)

        self.current_year = year

        # ========================================================
        # BUY (NON IMPONIBILE)
        # ========================================================
        # - Creo lotto FIFO
        # - Aggiorno balances
        # - Nessuna plus/minus generata
        # ========================================================

        if typ == 'buy':
            
            qty_out = abs(qty_b) + fee_b   # qty e' negativo . fee positivo ma un costo .. quantity -99 , fee 1 .. esce 100 

            if self.tax_lib.price_lib.isEuro(asset_b):
                valEur = qty_out
            else:
                unit_cost = self._consume_fifo(asset_b, qty_out)
                valEur = unit_cost * qty_out

            price = valEur / (qty - fee)

            self.purchases[asset].append(((qty - fee), price))
            self.balances[asset] += (qty - fee)
            self.balances[asset_b] -= qty_out
            

        # ========================================================
        # SELL (EVENTO IMPONIBILE)
        # ========================================================
        # 1) Determino costo fiscale con FIFO
        # 2) Calcolo controvalore EUR
        # 3) Genero plus/minus per anno
        # ========================================================

        elif typ == 'sell':

            qty_out = abs(qty) + fee
            qty_in = qty_b - fee_b
            
            unit_cost = self._consume_fifo(asset, qty_out)
            cost_basis = unit_cost * qty_out

            self.balances[asset] -= qty_out
            self.balances[asset_b] += qty_in

            # Determinazione controvalore EUR
            if self.tax_lib.price_lib.isEuro(asset_b):
                valEur = qty_in

            elif self.tax_lib.price_lib.isTax(asset_b):
                valUsd = self.tax_lib.price_lib.prezzo(asset_b,ts)
                valEur = qty_in * valUsd
                self.purchases[asset_b].append((qty_in, valUsd))
                
            else:
                valEur = 0
                self.purchases[asset_b].append((qty_in, cost_basis / qty_in))

            if valEur > 0:
                plus = valEur - cost_basis
                if plus >= 0:
                    self.total_plus[year] += plus
                else:
                    self.total_minus[year] += abs(plus)

                    
            #print(event['idx'], 'sell', valEur, qty_out, qty_in, unit_cost, cost_basis)
            #for k, v in self.purchases.items():
                #print(k, v)
                
                
        # ========================================================
        # REDDITI DIVERSI (Reward / Airdrop)
        # ========================================================
        # - Valorizzazione al momento della ricezione
        # - Creazione lotto con quel costo fiscale
        # - Accumulo redditi diversi annuali
        # ========================================================

            
        elif typ in ('reward', 'airdrop', 'rollover', 'margin', 'staking', 'transfer'):
            
            qty_in = qty - fee
            self.balances[asset] += qty_in
            
            
            if asset_b and qty_b:
                price_as = self.tax_lib.price_lib.prezzo(asset_b, ts)
                val_eur = price_as * qty_b
                price = val_eur / abs(qty_in)

            else:
                price = self.tax_lib.price_lib.prezzo(asset, ts)

            diversi = price * qty_in

            if qty_in > 0:
                if not self.tax_lib.price_lib.isEuro(asset):
                    self.purchases[asset].append((qty_in, price))
                self.diversi_plus[year] += diversi

            elif qty_in < 0:
                if not self.tax_lib.price_lib.isEuro(asset):
                    self._consume_fifo(asset, abs(qty_in))
                self.diversi_minus[year] += abs(diversi)

 
        # ========================================================
        # WITHDRAW (NON IMPONIBILE)
        # ========================================================
        # Registra prelievo per successivo match con deposito
        # ========================================================

        elif typ in ('withdraw', 'withdrawal'):
            
            qty_out = abs(qty) + fee
            
            if not self.tax_lib.price_lib.isEuro(asset):
                address = event.get('address', 'unknown')
                unit_cost = self._consume_fifo(asset, qty_out)

                self.tax_lib.registra_prelievo(
                    asset, address, abs(qty),
                    unit_cost, ts, self.exchange
                )

            self.balances[asset] -= qty_out

        # ========================================================
        # DEPOSIT (NON IMPONIBILE)
        # ========================================================
        # Recupera costo storico tramite match
        # ========================================================

        elif typ == 'deposit':
                        
            qty = qty if qty else qty_b    # in caso di euro a volte e' in qty_b
            qty_in = qty - fee

            if not self.tax_lib.price_lib.isEuro(asset):
                address = event.get('address', 'unknown')
                match = self.tax_lib.match_deposito(
                    asset, address, qty,
                    self.exchange, ts
                )
                cost = 0
                if match['matched']:
                    cost = match['cost']
                elif qty_b > 0 :    # se ho un costo di carico di un prelievo lo metto in qty_b nel parse
                    cost = qty_b
                
                self.purchases[asset].append((qty_in, cost))

            self.balances[asset] += qty_in

        else:
            raise ValueError(f"Tipo evento non gestito: {typ}")

        return {
            'plus': plus,
            'diversi': diversi
        }


    # ============================================================
    # CHIUSURA ANNO FISCALE
    # ============================================================

    def finalize(self):
        if self.current_year:
            self.year_end_balances[self.current_year] = dict(self.balances)


    # ============================================================
    # REPORT FISCALE ANNUALE
    # ============================================================
    # - Saldo al 31 dicembre
    # - Plus
    # - Minus
    # - Redditi diversi
    # ============================================================

    def build_report(self):

        report = []

        if self.year_end_balances:
            min_year = min(self.year_end_balances.keys())
            max_year = max(self.year_end_balances.keys())
        else:
            min_year, max_year = 2017, 2025

        all_years = range(min_year, max_year + 1)

        prev = {}

        for year in all_years:

            bals = self.year_end_balances.get(year, prev)
            prev = dict(bals)

            saldo = 0.0

            # Valorizzazione 31 dicembre
            for asset, qty in bals.items():
                if qty > 0 and asset != 'EUR':
                    price = self.tax_lib.prezzo_31dic(asset, year) or 0
                    saldo += qty * price

            report.append({
                'anno': year,
                'saldo_31dic': saldo,
                'plus': self.total_plus.get(year, 0),
                'minus': self.total_minus.get(year, 0),
                'diversi_plus': self.diversi_plus.get(year, 0),
                'diversi_minus': self.diversi_minus.get(year, 0),
                'exchange': self.exchange
            })

            self.tax_lib.salva_report_fiscale(
                self.exchange,
                year,
                saldo,
                self.total_plus.get(year, 0),
                self.total_minus.get(year, 0),
                self.diversi_plus.get(year, 0),
                self.diversi_minus.get(year, 0)
            )
            
        return report


    # ============================================================
    # DEBUG
    # ============================================================

    def debug_state(self):

        print("=== BALANCES ===")
        for k, v in self.balances.items():
            print(k, v)

        print("\n=== PURCHASES ===")
        for k, v in self.purchases.items():
            print(k, v)