import os, sys, pandas as pd
import csv
from datetime import datetime, timezone
import re
import numpy as np
import numbers

class CronverterLib:
    

    def to_float(self, val, field_name=''):
        """
        Converte qualsiasi numero/stringa numerica in float.
        Supporta:
        - int
        - float
        - numpy int/float
        - stringhe numeriche
        - None / '' / '-' -> None
        """

        label = f" ({field_name})" if field_name else ""

        if val is None:
            return None

        # Gestione NaN (pandas / numpy)
        if isinstance(val, float) and np.isnan(val):
            return None

        # Tutti i numerici (Python + numpy)
        if isinstance(val, numbers.Real):
            return float(val)

        # Stringhe
        if isinstance(val, str):
            v = val.strip()

            if v in ('', '-', 'nan', 'NaN', 'None'):
                return None
            
            
                v = v.replace(',', '.')
            try:
                return float(v)
            except ValueError as e:
                raise ValueError(
                    f"Valore non convertibile in float{label}: {val}"
                ) from e

        raise ValueError(
            f"Tipo non gestito in to_float{label}: {type(val)} -> {val}"
        )

    def to_timestamp(self, val, field_name=''):
        """
        Normalizza qualsiasi timestamp in UNIX a 10 cifre (secondi, UTC).

        Supporta:
        - datetime
        - pandas.Timestamp
        - int / float
        - ISO 8601 (con T, spazio, Z, offset)
        - millisecondi (13 cifre)
        - microsecondi (16 cifre)
        """

        label = f" ({field_name})" if field_name else ""

        if val is None:
            raise ValueError(f"Timestamp None{label}")

        # --------------------------------------------------
        # DATETIME / PANDAS
        # --------------------------------------------------
        if isinstance(val, (datetime, pd.Timestamp)):
            if val.tzinfo is None:
                # assumiamo UTC se naive (evitiamo conversioni locali)
                val = val.replace(tzinfo=timezone.utc)
            else:
                val = val.astimezone(timezone.utc)

            return int(val.timestamp())

        # --------------------------------------------------
        # NUMERICO
        # --------------------------------------------------
        if isinstance(val, (int, float)):
            ts = int(val)

        # --------------------------------------------------
        # STRINGA
        # --------------------------------------------------
        elif isinstance(val, str):
            v = val.strip()

            if not v:
                raise ValueError(f"Timestamp vuoto{label}")

            # Caso numerico puro
            if re.fullmatch(r"\d+", v):
                ts = int(v)
            else:
                # Normalizzazione ISO
                # gestisce Z → +00:00
                if v.endswith("Z"):
                    v = v[:-1] + "+00:00"

                try:
                    dt = datetime.fromisoformat(v)

                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)

                    return int(dt.timestamp())

                except Exception:
                    raise ValueError(f"Formato timestamp non riconosciuto{label}: {val}")

        else:
            raise ValueError(
                f"Tipo non gestito in to_timestamp{label}: {type(val)} -> {val}"
            )


        # --------------------------------------------------
        # NORMALIZZAZIONE NUMERICA
        # --------------------------------------------------
        digits = len(str(abs(ts)))

        if digits == 10:          # secondi
            return ts
        elif digits == 13:        # millisecondi
            return ts // 1000
        elif digits == 16:        # microsecondi
            return ts // 1_000_000
        elif digits > 16:
            # fallback: riduce fino a 10 cifre
            while len(str(abs(ts))) > 10:
                ts //= 10
            return ts

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
            
    def isTax(self, asset):
        return asset.upper() in ('EUR','USD', 'USDT', 'USDC', 'DAI', 'MXN')
      
con_lib = CronverterLib()  