import sys
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import importlib

from tkinterdnd2 import DND_FILES, TkinterDnD


# ====== DETECTORS ======
DETECTORS = {
    "Delta": ["id", "Asset ID", "Asset Symbol", "Amount", "Balance", "Transaction type", "Contract/Fund", "Reference ID", "Date"],
    "Kraken": ["txid", "refid", "time", "type", "subtype", "aclass", "subclass", "asset", "wallet", "amount", "fee", "balance", "address"],
}


def detect_cex(file):
    import pandas as pd

    df = pd.read_csv(file, nrows=1)
    cols = [c.strip() for c in df.columns]

    for cex, expected in DETECTORS.items():
        if cols[:len(expected)] == expected:
            return cex

    print("❌ Header non riconosciuto:", cols)
    return None


# ====== GLOBAL ======
selected_files = []


# ====== LOG ======
def log(msg):
    log_box.insert(tk.END, msg + "\n")
    log_box.see(tk.END)


class RedirectText:
    def __init__(self, widget):
        self.widget = widget

    def write(self, string):
        self.widget.insert(tk.END, string)
        self.widget.see(tk.END)

    def flush(self):
        pass


# ====== PROGRESS ======
def update_progress(value, total):
    try:
        percent = int((value / total) * 100)
        progress["value"] = percent
        root.update_idletasks()
    except:
        pass


# ====== FILE MANAGEMENT ======
def add_files(files):
    for f in files:
        if f not in selected_files:
            cex = detect_cex(f) or "?"
            selected_files.append(f)
            tree.insert("", "end", values=(os.path.basename(f), cex))


def scegli_file():
    files = filedialog.askopenfilenames(filetypes=[("CSV", "*.csv")])
    add_files(files)


def drop(event):
    files = root.tk.splitlist(event.data)
    add_files(files)


def remove_selected():
    selected = tree.selection()
    for item in selected:
        values = tree.item(item, "values")
        filename = values[0]

        for i, f in enumerate(selected_files):
            if os.path.basename(f) == filename:
                del selected_files[i]
                break

        tree.delete(item)


# ====== RUN ======
def start_conversion():
    if not selected_files:
        messagebox.showwarning("Attenzione", "Nessun file selezionato")
        return

    try:
        progress["value"] = 0

        # raggruppa per CEX
        groups = {}

        for f in selected_files:
            cex = detect_cex(f)
            if not cex:
                raise Exception(f"CEX non riconosciuto: {os.path.basename(f)}")

            groups.setdefault(cex, []).append(f)

        # esegui uno per volta
        for cex, files in groups.items():
            log(f"🚀 Avvio {cex} ({len(files)} file)")

            mod = importlib.import_module(f"converters.{cex}")

            output = mod.run(files, progress_callback=update_progress)

            log(f"✅ {cex} completato: {output}")

        progress["value"] = 100

    except Exception as e:
        log(f"❌ {e}")
        messagebox.showerror("Errore", str(e))


# ====== GUI ======
root = TkinterDnD.Tk()
root.title("TaxEngine")
root.geometry("750x550")

tk.Label(root, text="TaxEngine Converter", font=("Arial", 14)).pack(pady=10)

# drag area
drop_frame = tk.Label(root, text="Trascina qui i file CSV", relief="groove", height=3)
drop_frame.pack(fill="x", padx=10, pady=5)

drop_frame.drop_target_register(DND_FILES)
drop_frame.dnd_bind("<<Drop>>", drop)

# ====== TREEVIEW (File + CEX) ======
tree = ttk.Treeview(root, columns=("file", "cex"), show="headings", height=10)

tree.heading("file", text="File")
tree.heading("cex", text="CEX")

tree.column("file", width=500)
tree.column("cex", width=100)

tree.pack(fill="x", padx=10, pady=5)

# buttons
frame_btn = tk.Frame(root)
frame_btn.pack(pady=5)

tk.Button(frame_btn, text="Aggiungi file", command=scegli_file).pack(side="left", padx=5)
tk.Button(frame_btn, text="Rimuovi", command=remove_selected).pack(side="left", padx=5)
tk.Button(frame_btn, text="START", command=start_conversion).pack(side="left", padx=5)

# progress
progress = ttk.Progressbar(root, orient="horizontal", length=650, mode="determinate")
progress.pack(pady=10)

# log
log_box = tk.Text(root, height=12)
log_box.pack(fill="both", expand=True, padx=10, pady=10)

sys.stdout = RedirectText(log_box)
sys.stderr = RedirectText(log_box)

root.mainloop()