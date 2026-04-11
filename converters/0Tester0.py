import sys
import os
import pandas as pd
import glob
    
LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "core")
sys.path.append(LIBRARY_DIR)

from crypto_tax_lib import tax_lib
from crypto_tax_engine import CryptoTaxEngine

def run(filepaths, progress_callback=None):
    
    # Carica e unisci
    df_list = []

    for file in filepaths:
        df_sing = pd.read_csv(file)
        df_sing["_fileTest"] = file
        df_sing["_lineTest"] = range(len(df_sing))  # indice riga file originale
        df_list.append(df_sing)

    # Unione finale
    df = pd.concat(df_list, ignore_index=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    engine = CryptoTaxEngine(tax_lib)

    Data_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data")
    file_path_deb = os.path.join(Data_DIR,  '_debug.csv')
    tax_lib.reset_result_file(file_path_deb)

    last_balance = {} 
    total = len(df)

    for idx, row in df.iterrows():

        if progress_callback:
            progress_callback(idx, total)

        result = engine.process_event(row)


        save_row = {
            'idx': idx,
            **row,
            **result,
            '_lineTest': row._lineTest,
            '_fileTest': row._fileTest
        }

        tax_lib.append_event_to_csv(file_path_deb, save_row)
        

    print(last_balance)

    engine.finalize()
    report = engine.build_report()
    engine.debug_state()

    df_finale = pd.DataFrame(report)

    print("\n### TABELLA RIASSUNTO ###")
    print(df_finale.to_markdown(index=False))

    tax_lib.report_status()

    if progress_callback:
        progress_callback(idx, total)
        
if __name__ == "__main__":
        
    CexName = "Kraken"

    paths = tax_lib.get_cex_paths(CexName)

    pattern = os.path.join(paths["events"], CexName+"*_event.csv")

    # Trova tutti i file
    files = glob.glob(pattern)

    print("File trovati:")
    for f in files:
        print(f)
    
    run(files)  
        