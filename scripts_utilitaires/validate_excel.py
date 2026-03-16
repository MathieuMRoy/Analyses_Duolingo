import pandas as pd
import glob
from datetime import datetime
import sys

# Force UTF-8 encoding for standard output to avoid emoji crashes on Windows
sys.stdout.reconfigure(encoding='utf-8')

date = datetime.now().strftime('%Y-%m-%d')
files = glob.glob(f'S:/Analyses_Duolingo/rapports_donnees/rapport_{date}.xlsx')

if not files:
    print("Fichier non trouvé")
    sys.exit(1)

xlsx = files[0]
excel = pd.ExcelFile(xlsx)

print(f"FICHIER : {xlsx}")
print(f"FEUILLES : {excel.sheet_names}\n")

print("--- DONNÉES DE LA FEUILLE 1 ---")
df1 = pd.read_excel(xlsx, sheet_name=excel.sheet_names[0])
print(df1.to_markdown())

print("\n--- DONNÉES DE LA FEUILLE 2 (Aperçu) ---")
df2 = pd.read_excel(xlsx, sheet_name=excel.sheet_names[1])
print(df2.head(10))
