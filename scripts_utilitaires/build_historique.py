from __future__ import annotations

from pathlib import Path
import shutil
import pandas as pd

from duolingo_analyzer.config import REPORT_DIR
from duolingo_analyzer.excel_dashboard import refresh_trends_dashboard
from duolingo_analyzer.stats import SUMMARY_SHEET, _normalize_summary_df
from scripts_utilitaires.restyle_report import restyle_report


HISTO_FILE = REPORT_DIR / "rapport_historique.xlsx"


def build_historique() -> None:
    report_files = sorted(REPORT_DIR.glob("rapport_*.xlsx"))
    report_files = [p for p in report_files if p.name != HISTO_FILE.name]

    if not report_files:
        print("Aucun rapport journalier trouvé.")
        return

    latest_report = report_files[-1]

    if not HISTO_FILE.exists():
        shutil.copyfile(latest_report, HISTO_FILE)

    frames: list[pd.DataFrame] = []
    for report_path in report_files:
        try:
            df = pd.read_excel(report_path, sheet_name=SUMMARY_SHEET)
            if df is not None and not df.empty:
                frames.append(_normalize_summary_df(df))
        except Exception:
            continue

    if not frames:
        print("Aucune feuille de résumé trouvée.")
        return

    df_all = pd.concat(frames, ignore_index=True)
    df_all = _normalize_summary_df(df_all)

    with pd.ExcelWriter(HISTO_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_all.to_excel(writer, sheet_name=SUMMARY_SHEET, index=False)

    # Appliquer le style pro (convertit aussi les valeurs texte en nombres)
    restyle_report(HISTO_FILE)
    refresh_trends_dashboard(HISTO_FILE)

    print(f"OK: {HISTO_FILE}")


if __name__ == "__main__":
    build_historique()
