
import openpyxl
from pathlib import Path

excel_path = Path("s:/Analyses_Duolingo/rapports_donnees/rapport_2026-03-15.xlsx")
if excel_path.exists():
    wb = openpyxl.load_workbook(excel_path)
    print(wb.sheetnames)
