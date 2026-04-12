"""
Module de configuration global pour le projet Duolingo Engagement.
"""
import sys
import os
import requests
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# ─── Compatibilité Windows (encodage UTF-8 pour la console) ─────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─── Chargement de l'environnement ──────────────────────────────────────────
load_dotenv()

# ─── Chemins des fichiers ───────────────────────────────────────────────────
# BASE_DIR pointe vers le dossier parent (S:\Analyses_Duolingo)
BASE_DIR = Path(__file__).resolve().parent.parent
REPORT_DIR = BASE_DIR / "rapports_donnees"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_USERS_FILE = BASE_DIR / "target_users.csv"
DAILY_LOG_FILE = BASE_DIR / "daily_streaks_log.csv"
ALTERNATIVE_DATA_INPUT_FILE = BASE_DIR / "alternative_data_inputs.csv"
APP_TIMEZONE = ZoneInfo("America/Toronto")
DAILY_LOG_RETENTION_DAYS = int(os.getenv("DAILY_LOG_RETENTION_DAYS", "120"))
DISCOVERY_ACTIVE_LOOKBACK_DAYS = int(os.getenv("DISCOVERY_ACTIVE_LOOKBACK_DAYS", "7"))


def now_toronto() -> datetime:
    return datetime.now(APP_TIMEZONE)


date_str = now_toronto().strftime("%Y-%m-%d")
DAILY_RAPPORT_EXCEL_FILE = REPORT_DIR / f"rapport_{date_str}.xlsx"
# Fichier unique qui s'enrichit chaque jour (historique)
RAPPORT_EXCEL_FILE = REPORT_DIR / "rapport_historique.xlsx"
ALTERNATIVE_DATA_HISTORY_FILE = REPORT_DIR / "alternative_data_history.csv"
MISSING_TARGET_USERS_FILE = REPORT_DIR / "missing_target_users.csv"

# ─── Constantes et Clés API ─────────────────────────────────────────────────
CEO_USERNAME = "luis"
OBJECTIF_PAR_COHORTE = 3334
OBJECTIF_UTILISATEURS = 10000

DUOLINGO_PROFILE_API = "https://www.duolingo.com/2017-06-30/users"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
DUOLINGO_JWT = os.getenv("DUOLINGO_JWT")
GOOGLE_DRIVE_REPORT_DIR = os.getenv("GOOGLE_DRIVE_REPORT_DIR")

# ─── Session HTTP & Headers ─────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json; charset=UTF-8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.duolingo.com/",
    "X-Requested-With": "XMLHttpRequest",
    "DNT": "1",
}

# Session HTTP réutilisable
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
if DUOLINGO_JWT:
    SESSION.headers.update({"Authorization": f"Bearer {DUOLINGO_JWT}"})
