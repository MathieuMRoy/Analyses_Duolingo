"""
Definitions centralisees des colonnes, noms de feuilles et constantes partagees.

Ce module est le point unique pour ajouter, renommer ou supprimer
une colonne du suivi quotidien ou une feuille Excel.
"""

import pandas as pd


# Noms de feuilles Excel
SUMMARY_SHEET = "Suivi Quotidien"
AI_SHEET = "🤖 Analyse Strategique"
GLOSSAIRE_SHEET = "📖 Dictionnaire des KPIs"
GLOSSAIRE_RAW_SHEET = "Dictionnaire des KPIs - Raw"
TRENDS_SHEET = "📈 Tendances Mensuelles"
CHART_DATA_SHEET = "📊 Donnees Graphique"

SIGNALS_SHEET = "Signaux Financiers"
SIGNALS_RAW_SHEET = "Signaux Financiers - Raw"
QUARTERLY_SHEET = "Nowcast Trimestriel"
QUARTERLY_RAW_SHEET = "Nowcast Trimestriel - Raw"
ALT_DATA_SHEET = "Alternative Data"
ALT_DATA_RAW_SHEET = "Alternative Data - Raw"
DCF_SHEET = "Valorisation DCF"

LEGACY_SHEET_NAMES = {
    "📊 Resume Financier Q1",
    "Briefing IA",
}

BAD_SHEET_NAMES = {
    "Ã°Å¸â€œÅ  RÃƒÂ©sumÃƒÂ© Financier Q1",
    "Ã°Å¸Â¤â€“ Analyse StratÃƒÂ©gique",
    "Ã°Å¸â€œÅ  DonnÃƒÂ©es Graphique",
}


# Colonnes du suivi quotidien
SUMMARY_COLUMNS = [
    "Date",
    "Serie Moyenne (Jours)",
    "Evol. vs Veille",
    "Apprentissage (XP/j)",
    "Taux Abonn. Super",
    "Taux d'Abandon Global",
    "Abandons vs Veille",
    "Reactivations vs Veille",
    "Score d'Engagement",
    "Panel Total",
]

PERCENT_COLUMNS = {
    "Taux Abonn. Super",
    "Taux d'Abandon Global",
    "Score d'Engagement",
}

SUMMARY_COLUMN_ALIASES = {
    "Date": "Date",
    "Moyenne Streak (J)": "Serie Moyenne (Jours)",
    "Serie Moyenne (Jours)": "Serie Moyenne (Jours)",
    "Série Moyenne (Jours)": "Serie Moyenne (Jours)",
    "Evolution vs Hier": "Evol. vs Veille",
    "Evol. vs Veille": "Evol. vs Veille",
    "Évol. vs Veille": "Evol. vs Veille",
    "Évolution vs Hier": "Evol. vs Veille",
    "Delta XP (Intensite)": "Apprentissage (XP/j)",
    "Apprentissage (XP/j)": "Apprentissage (XP/j)",
    "Conversion Premium": "Taux Abonn. Super",
    "Taux Abonn. Super": "Taux Abonn. Super",
    "Taux Abonn. Max": "Taux Abonn. Max",
    "Churn Global": "Taux d'Abandon Global",
    "Taux d'Abandon Global": "Taux d'Abandon Global",
    "Taux d'Attrition Global": "Taux d'Abandon Global",
    "Abandons vs Veille": "Abandons vs Veille",
    "Reactivations vs Veille": "Reactivations vs Veille",
    "Score Sante Global": "Score d'Engagement",
    "Score Santé Global": "Score d'Engagement",
    "Score d'Engagement": "Score d'Engagement",
    "Total Profils": "Panel Total",
    "Panel Total": "Panel Total",
}

DAILY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]

SUMMARY_MIN_RELIABLE_DATE = pd.Timestamp("2026-03-16")
