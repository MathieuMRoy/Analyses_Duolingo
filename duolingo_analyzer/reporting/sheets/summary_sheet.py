"""Helpers pour l'onglet Suivi Quotidien."""

from __future__ import annotations

import numbers
from datetime import datetime

import pandas as pd


def build_summary_today_df(stats: dict) -> pd.DataFrame:
    date_jour = stats.get("date_jour")
    date_obj = None
    if date_jour:
        try:
            date_obj = datetime.strptime(date_jour, "%Y-%m-%d")
        except ValueError:
            date_obj = None

    moyenne_streak_jour = round(stats.get("moyenne_streak_jour", 0), 1)
    moyenne_streak_hier = stats.get("moyenne_streak_hier")
    delta_streak = None
    if moyenne_streak_hier is not None:
        delta_streak = round(moyenne_streak_jour - moyenne_streak_hier, 1)

    return pd.DataFrame(
        [
            {
                "Date": date_obj or date_jour,
                "Serie Moyenne (Jours)": moyenne_streak_jour,
                "Evol. vs Veille": delta_streak,
                "Apprentissage (XP/j)": round(stats.get("delta_xp_moyen", 0), 0),
                "Taux Abonn. Super": round(stats["taux_conversion_plus"] / 100, 6)
                if isinstance(stats.get("taux_conversion_plus"), numbers.Number)
                else None,
                "Taux d'Abandon Global": round(stats.get("taux_churn", 0) / 100, 6),
                "Abandons vs Veille": stats.get("streaks_tombes_zero", 0),
                "Reactivations vs Veille": stats.get("reactivations_veille", 0),
                "Score d'Engagement": round(stats.get("score_sante_jour", 0) / 100, 6),
                "Panel Total": stats.get("nb_profils_jour"),
            }
        ]
    )


def merge_summary_history(
    df_resume: pd.DataFrame | None,
    df_today: pd.DataFrame,
    normalize_summary_df,
) -> pd.DataFrame:
    df_today = normalize_summary_df(df_today)

    if df_resume is None or df_resume.empty:
        merged = df_today.copy()
    else:
        merged = normalize_summary_df(df_resume)
        if "Date" in merged.columns:
            merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
        df_today["Date"] = pd.to_datetime(df_today["Date"], errors="coerce")
        date_value = df_today.loc[0, "Date"] if "Date" in df_today.columns else None
        if "Date" in merged.columns and pd.notna(date_value):
            merged = merged[merged["Date"] != date_value]
        merged = pd.concat([merged, df_today], ignore_index=True, sort=False)

    if "Date" in merged.columns:
        merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
        merged = merged.sort_values("Date").reset_index(drop=True)

    return merged
