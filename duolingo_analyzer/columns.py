"""
Shared sheet names, column names, and workbook constants.

This module is the single place to update a visible tab name or a
daily-tracking column.
"""

import pandas as pd


# Sheet names
SUMMARY_SHEET = "Suivi Quotidien"
AI_SHEET = "🤖 Analyse Strategique"
BRIEFING_SHEET = "Briefing IA"
GLOSSAIRE_SHEET = BRIEFING_SHEET
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
    "Resume Financier Q1",
    "Dictionnaire des KPIs",
    "Guide des KPIs",
    "📊 Resume Financier Q1",
    "📖 Dictionnaire des KPIs",
}

BAD_SHEET_NAMES = {
    "Ã°Å¸â€œÅ  RÃƒÂ©sumÃƒÂ© Financier Q1",
    "Ã°Å¸Â¤â€“ Analyse StratÃƒÂ©gique",
    "Ã°Å¸â€œÅ  DonnÃƒÂ©es Graphique",
}


# Daily summary columns
SUMMARY_COLUMNS = [
    "Date",
    "Série Moyenne (Jours)",
    "Évol. vs Veille",
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
    "Moyenne Streak (J)": "Série Moyenne (Jours)",
    "Serie Moyenne (Jours)": "Série Moyenne (Jours)",
    "Série Moyenne (Jours)": "Série Moyenne (Jours)",
    "Evolution vs Hier": "Évol. vs Veille",
    "Evol. vs Veille": "Évol. vs Veille",
    "Évol. vs Veille": "Évol. vs Veille",
    "Évolution vs Hier": "Évol. vs Veille",
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
