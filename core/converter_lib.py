import os, sys, pandas as pd
from datetime import datetime
import csv

class CronverterLib:
    
    def to_float(self, val, field_name=''):
        
        
        label = f" ({field_name})" if field_name else ""

        if val is None:
            return None

        if isinstance(val, (int, float)):
            return float(val)

        if isinstance(val, str):
            v = val.strip()
            if v in ('', '-'):
                return None
            try:
                return float(v)
            except ValueError as e:
                raise ValueError(f"Valore non convertibile in float{label}: {val}") from e

        raise ValueError(f"Tipo non gestito in to_float{label}: {type(val)} -> {val}")

    def to_timestamp(self, val, field_name=''):
        
        # Normalizza timestamp in UNIX a 10 cifre (secondi).
        # Supporta:
        # - datetime
        # - pandas.Timestamp
        # - int / float
        # - ISO string
        # - millisecondi (13 cifre)
        # - microsecondi (14 cifre)    

        label = f" ({field_name})" if field_name else ""

        if val is None:
            raise ValueError(f"Timestamp None{label}")

        if isinstance(val, datetime) or isinstance(val, pd.Timestamp):
            return int(val.timestamp())

        # numerico
        if isinstance(val, (int, float)):
            ts = int(val)

        # stringa
        
        
        elif isinstance(val, str):
            v = val.strip()
            if not v:
                raise ValueError(f"Timestamp vuoto{label}")

            # Prova parsing automatico ISO (funziona sia con 'T' che con spazio)
            try:
                return int(datetime.fromisoformat(v).timestamp())
            except ValueError:
                pass  # non era formato datetime

            # Se non è datetime prova numerico
            if v.isdigit():
                ts = int(v)
            else:
                raise ValueError(f"Timestamp non numerico{label}: {val}")

        else:
            raise ValueError(
                f"Tipo non gestito in to_timestamp{label}: "
                f"{type(val)} -> {val}"
            )

        # normalizzazione lunghezza
        digits = len(str(abs(ts)))

        if digits == 10:
            return ts
        elif digits == 13:
            return ts // 1000
        elif digits == 14:
            return ts // 10000

        raise ValueError(
            f"Lunghezza timestamp inattesa ({digits}){label}: {ts}"
        )
    def get_cex_paths(self,cex_name):

        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        data_dir = os.path.join(base_dir, "Data")
        Reports_dir = os.path.join(data_dir, "CexReports")
        Events_dir = os.path.join(data_dir, "Events")
        Cex_dir = os.path.join(Reports_dir, cex_name)

        return {
            "report": Cex_dir,
            "events": Events_dir
        }
    
    def append_event_to_csv(self, filename, row_dict):
        """
        Appende una riga ad un CSV nella cartella Result.
        Se il file non esiste, crea il file e scrive l'intestazione.
        """
        
        headers = [
            'timestamp', 'type', 'asset', 'qty',
            'fee', 'asset_b', 'qty_b', 'fee_b',
            'address', 'Exchange', 'idx', 'File'
        ]

        file_exists = os.path.isfile(filename)

        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                writer.writeheader()

            writer.writerow(row_dict)

    def reset_result_file(self,filename):

        if os.path.exists(filename):
            os.remove(filename)
            
      
con_lib = CronverterLib()  