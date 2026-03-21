"""
Définitions centralisées des colonnes, noms de feuilles et constantes partagées.

Ce module est le point unique pour ajouter, renommer ou supprimer
une colonne du suivi quotidien ou une feuille Excel.
"""

import pandas as pd

# ─── Noms de feuilles Excel ──────────────────────────────────────────────────

SUMMARY_SHEET = "Suivi Quotidien"
AI_SHEET = "🤖 Analyse Stratégique"
GLOSSAIRE_SHEET = "📖 Dictionnaire des KPIs"
GLOSSAIRE_RAW_SHEET = "Dictionnaire des KPIs - Raw"
TRENDS_SHEET = "📈 Tendances Mensuelles"
CHART_DATA_SHEET = "📊 Données Graphique"

SIGNALS_SHEET = "Signaux Financiers"
SIGNALS_RAW_SHEET = "Signaux Financiers - Raw"
QUARTERLY_SHEET = "Nowcast Trimestriel"
QUARTERLY_RAW_SHEET = "Nowcast Trimestriel - Raw"
DCF_SHEET = "Valorisation DCF"

LEGACY_SHEET_NAMES = {
    "📊 Résumé Financier Q1",
}

BAD_SHEET_NAMES = {
    'ðŸ“Š RÃ©sumÃ© Financier Q1',
    'ðŸ¤– Analyse StratÃ©gique',
    'ðŸ“Š DonnÃ©es Graphique',
}

# ─── Colonnes du suivi quotidien ─────────────────────────────────────────────

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
    "Série Moyenne (Jours)": "Série Moyenne (Jours)",
    "Évolution vs Hier": "Évol. vs Veille",
    "Évol. vs Veille": "Évol. vs Veille",
    "Delta XP (Intensité)": "Apprentissage (XP/j)",
    "Apprentissage (XP/j)": "Apprentissage (XP/j)",
    "Conversion Premium": "Taux Abonn. Super",
    "Taux Abonn. Super": "Taux Abonn. Super",
    "Taux Abonn. Max": "Taux Abonn. Max",
    "Churn Global": "Taux d'Abandon Global",
    "Taux d'Abandon Global": "Taux d'Abandon Global",
    "Taux d'Attrition Global": "Taux d'Abandon Global",
    "Abandons vs Veille": "Abandons vs Veille",
    "Reactivations vs Veille": "Reactivations vs Veille",
    "Score Santé Global": "Score d'Engagement",
    "Score d'Engagement": "Score d'Engagement",
    "Total Profils": "Panel Total",
    "Panel Total": "Panel Total",
}

DAILY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]

SUMMARY_MIN_RELIABLE_DATE = pd.Timestamp("2026-03-16")
