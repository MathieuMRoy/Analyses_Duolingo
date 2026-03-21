"""
Partie 2 : Statistiques
Calcule les statistiques d'engagement via Pandas.
"""
import csv
from datetime import datetime, timedelta
import numbers
import pandas as pd

from .columns import (
    DAILY_LOG_COLUMNS,
    PERCENT_COLUMNS,
    SUMMARY_COLUMN_ALIASES,
    SUMMARY_COLUMNS,
    SUMMARY_MIN_RELIABLE_DATE,
)
from .config import DAILY_LOG_FILE, now_toronto
from .subscription_detection import (
    apply_max_overrides,
    compute_max_count,
    compute_super_observable_count,
    parse_bool,
    parse_optional_bool,
)
from .utils import parse_float


def _normalize_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    normalized = pd.DataFrame(index=df.index)

    for column in SUMMARY_COLUMNS:
        normalized[column] = pd.Series(index=df.index, dtype="object")

    for source_column in df.columns:
        target_column = SUMMARY_COLUMN_ALIASES.get(str(source_column).strip())
        if not target_column:
            continue
        if target_column not in normalized.columns:
            continue

        source_series = df[source_column]

        if target_column == "Date":
            parsed_series = pd.to_datetime(source_series, errors="coerce")
        else:
            parsed_series = source_series.apply(parse_float)

            if target_column in PERCENT_COLUMNS:
                parsed_series = parsed_series.apply(
                    lambda x: (x / 100) if isinstance(x, numbers.Number) and abs(x) > 1 else x
                )

        fill_mask = normalized[target_column].isna()
        normalized.loc[fill_mask, target_column] = parsed_series[fill_mask]

    if "Date" in normalized.columns:
        normalized = normalized[normalized["Date"].notna()]
        normalized = normalized[normalized["Date"] >= SUMMARY_MIN_RELIABLE_DATE]
    if "Panel Total" in normalized.columns:
        panel_total_numeric = pd.to_numeric(normalized["Panel Total"], errors="coerce")
        normalized = normalized[panel_total_numeric.fillna(0) > 0]
        normalized["Panel Total"] = panel_total_numeric.loc[normalized.index]
    if "Date" in normalized.columns:
        normalized["_panel_total_rank"] = pd.to_numeric(normalized.get("Panel Total"), errors="coerce").fillna(-1)
        normalized["_completeness_rank"] = normalized.notna().sum(axis=1)
        normalized = (
            normalized.sort_values(["Date", "_panel_total_rank", "_completeness_rank"])
            .drop_duplicates(subset=["Date"], keep="last")
            .drop(columns=["_panel_total_rank", "_completeness_rank"], errors="ignore")
        )

    return normalized.reset_index(drop=True)


def _load_daily_log_df() -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    with open(DAILY_LOG_FILE, "r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if header is None:
            return pd.DataFrame(columns=DAILY_LOG_COLUMNS)

        for row in reader:
            if not row:
                continue
            if row == DAILY_LOG_COLUMNS or row[:6] == DAILY_LOG_COLUMNS[:6]:
                continue

            normalized_row = row[:]
            if len(normalized_row) == 6:
                normalized_row.append("")
            elif len(normalized_row) < 6:
                continue
            elif len(normalized_row) > 7:
                normalized_row = normalized_row[:7]

            if len(normalized_row) < 7:
                normalized_row.extend([""] * (7 - len(normalized_row)))

            rows.append(dict(zip(DAILY_LOG_COLUMNS, normalized_row)))

    df = pd.DataFrame(rows, columns=DAILY_LOG_COLUMNS)
    if df.empty:
        return df

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Streak"] = pd.to_numeric(df["Streak"], errors="coerce").fillna(0)
    df["TotalXP"] = pd.to_numeric(df["TotalXP"], errors="coerce").fillna(0)
    df["HasPlus"] = df["HasPlus"].apply(parse_bool)
    df["HasMax"] = df["HasMax"].apply(parse_optional_bool)
    df = apply_max_overrides(df)
    df = df[df["Date"].notna()]
    return df


def _compute_super_rate_pct(df_jour: pd.DataFrame) -> float | None:
    if df_jour.empty or "HasPlus" not in df_jour.columns:
        return None
    super_count = compute_super_observable_count(df_jour["HasPlus"], df_jour.get("HasMax"))
    return (super_count / len(df_jour)) * 100 if len(df_jour) else None


def _compute_max_rate_pct(df_jour: pd.DataFrame) -> float | None:
    if df_jour.empty or "HasMax" not in df_jour.columns:
        return None
    max_count = compute_max_count(df_jour["HasMax"])
    if max_count is None:
        return None
    return (max_count / len(df_jour)) * 100 if len(df_jour) else None


def _build_summary_history_from_log(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None

    df = df.copy()
    df = df[~df["Username"].str.contains("Aggregated", na=False)]
    df = df[df["Cohort"] != "Global"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()]
    if df.empty:
        return None

    df = df.sort_values(["Date", "Username"]).drop_duplicates(subset=["Date", "Username"], keep="last")
    available_dates = sorted(df["Date"].dt.strftime("%Y-%m-%d").unique().tolist())
    if not available_dates:
        return None

    rows: list[dict[str, object]] = []
    for date_str in available_dates:
        date_obj = pd.to_datetime(date_str, errors="coerce")
        prev_date_str = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d") if pd.notna(date_obj) else None

        df_jour = df[df["Date"].dt.strftime("%Y-%m-%d") == date_str].copy()
        df_hier = df[df["Date"].dt.strftime("%Y-%m-%d") == prev_date_str].copy() if prev_date_str else pd.DataFrame()

        moyenne_streak_jour = float(df_jour["Streak"].mean()) if not df_jour.empty else 0.0
        moyenne_streak_hier = float(df_hier["Streak"].mean()) if not df_hier.empty else None
        delta_streak = round(moyenne_streak_jour - moyenne_streak_hier, 1) if moyenne_streak_hier is not None else None

        taux_super = _compute_super_rate_pct(df_jour)
        taux_max = _compute_max_rate_pct(df_jour)

        delta_xp_moyen = 0.0
        taux_churn = 0.0
        abandons_veille = 0
        reactivations_veille = 0
        if not df_jour.empty and not df_hier.empty:
            merged = df_hier.merge(
                df_jour,
                on="Username",
                suffixes=("_hier", "_jour"),
                how="inner",
            )
            if "TotalXP_hier" in merged.columns and "TotalXP_jour" in merged.columns and not merged.empty:
                merged["Delta_XP"] = merged["TotalXP_jour"] - merged["TotalXP_hier"]
                merged.loc[merged["Delta_XP"] < 0, "Delta_XP"] = 0
                delta_xp_moyen = float(merged["Delta_XP"].mean()) if not merged.empty else 0.0

            tombes_a_zero = merged[(merged["Streak_hier"] > 0) & (merged["Streak_jour"] == 0)]
            actifs_hier = int(len(df_hier[df_hier["Streak"] > 0]))
            taux_churn = ((len(tombes_a_zero) / actifs_hier) * 100) if actifs_hier > 0 else 0.0
            abandons_veille = int(len(tombes_a_zero))
            reactivations_veille = int(((merged["Streak_hier"] == 0) & (merged["Streak_jour"] > 0)).sum())

        utilisateurs_actifs = int(len(df_jour[df_jour["Streak"] > 0]))
        score_engagement = ((utilisateurs_actifs / len(df_jour)) * 100) if len(df_jour) > 0 else 0.0

        rows.append(
            {
                "Date": date_obj,
                "Série Moyenne (Jours)": round(moyenne_streak_jour, 1),
                "Évol. vs Veille": delta_streak,
                "Apprentissage (XP/j)": round(delta_xp_moyen, 0),
                "Taux Abonn. Super": round(taux_super / 100, 6) if isinstance(taux_super, numbers.Number) else None,
                "Taux d'Abandon Global": round(taux_churn / 100, 6),
                "Abandons vs Veille": abandons_veille,
                "Reactivations vs Veille": reactivations_veille,
                "Score d'Engagement": round(score_engagement / 100, 6),
                "Panel Total": int(len(df_jour)),
            }
        )

    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def calculer_statistiques() -> dict | None:
    """
    PARTIE 2 : Calcule les statistiques d'engagement pour aujourd'hui
    et extrait les tendances par rapport à la veille.
    """
    print("============================================================")
    print("  PARTIE 2 — CALCUL DES STATISTIQUES")
    print("============================================================")

    if not DAILY_LOG_FILE.exists():
        print("  ❌ Aucun fichier de données trouvé.")
        return None

    try:
        df = _load_daily_log_df()
    except Exception as e:
        print(f"  ❌ Erreur de lecture du CSV : {e}")
        return None

    if df.empty:
        print("  ⚠️ Le fichier est vide.")
        return None

    df = df[~df["Username"].str.contains("Aggregated", na=False)]
    df = df[df["Cohort"] != "Global"]

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.sort_values(["Date", "Username"]).drop_duplicates(subset=["Date", "Username"], keep="last")

    now_local = now_toronto()
    aujourdhui = now_local.strftime("%Y-%m-%d")
    hier = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    df_jour = df[df["Date"] == aujourdhui]
    df_hier = df[df["Date"] == hier]

    stats = {
        "date_jour": aujourdhui,
        "date_hier": hier,
        "moyenne_streak_jour": 0.0,
        "moyenne_streak_hier": None,
        "utilisateurs_actifs": 0,
        "streaks_tombes_zero": 0,
        "reactivations_veille": 0,
        "progression_debutants_vers_standard": 0,
        "taux_progression_debutants_vers_standard": 0.0,
        "nb_profils_jour": len(df_jour),
        "nb_profils_hier": len(df_hier),
        "taux_conversion_plus": None,
        "taux_conversion_max": None,
        "delta_xp_moyen": 0.0,
        "cohortes": {
            "Debutants": {
                "actifs": 0,
                "total": 0,
                "retention": 0.0,
                "churn": 0.0,
                "tombes_zero": 0,
                "transitions_sortantes": 0,
                "transition_vers_standard": 0.0,
            },
            "Standard": {
                "actifs": 0,
                "total": 0,
                "retention": 0.0,
                "churn": 0.0,
                "tombes_zero": 0,
                "transitions_sortantes": 0,
                "transition_vers_standard": 0.0,
            },
            "Super-Actifs": {
                "actifs": 0,
                "total": 0,
                "retention": 0.0,
                "churn": 0.0,
                "tombes_zero": 0,
                "transitions_sortantes": 0,
                "transition_vers_standard": 0.0,
            },
        },
    }

    if not df_jour.empty:
        stats["moyenne_streak_jour"] = float(df_jour["Streak"].mean())
        stats["utilisateurs_actifs"] = int(len(df_jour[df_jour["Streak"] > 0]))
        stats["score_sante_jour"] = (
            (stats["utilisateurs_actifs"] / stats["nb_profils_jour"]) * 100
            if stats["nb_profils_jour"] > 0
            else 0
        )

        if stats["nb_profils_jour"] > 0:
            stats["taux_conversion_max"] = _compute_max_rate_pct(df_jour)
            stats["taux_conversion_plus"] = _compute_super_rate_pct(df_jour)

        if "Cohort" in df_jour.columns:
            for cohorte in stats["cohortes"].keys():
                df_c = df_jour[df_jour["Cohort"] == cohorte]
                stats["cohortes"][cohorte]["total"] = int(len(df_c))
                stats["cohortes"][cohorte]["actifs"] = int(len(df_c[df_c["Streak"] > 0]))

    if not df_hier.empty and not df_jour.empty:
        stats["moyenne_streak_hier"] = float(df_hier["Streak"].mean())

        merged = df_hier.merge(
            df_jour,
            on="Username",
            suffixes=("_hier", "_jour"),
            how="inner",
        )

        if "TotalXP_hier" in merged.columns and "TotalXP_jour" in merged.columns:
            merged["Delta_XP"] = merged["TotalXP_jour"] - merged["TotalXP_hier"]
            merged.loc[merged["Delta_XP"] < 0, "Delta_XP"] = 0
            stats["delta_xp_moyen"] = float(merged["Delta_XP"].mean()) if not merged.empty else 0.0

        tombes_a_zero = merged[(merged["Streak_hier"] > 0) & (merged["Streak_jour"] == 0)]
        stats["streaks_tombes_zero"] = int(len(tombes_a_zero))

        reactivated_mask = (merged["Streak_hier"] == 0) & (merged["Streak_jour"] > 0)
        stats["reactivations_veille"] = int(reactivated_mask.sum())

        actifs_hier = int(len(df_hier[df_hier["Streak"] > 0]))
        stats["taux_retention"] = (
            ((actifs_hier - stats["streaks_tombes_zero"]) / actifs_hier * 100)
            if actifs_hier > 0
            else 0
        )
        stats["taux_churn"] = (
            (stats["streaks_tombes_zero"] / actifs_hier * 100)
            if actifs_hier > 0
            else 0
        )

        if "Cohort_hier" in merged.columns:
            for cohorte in stats["cohortes"].keys():
                merged_c = merged[merged["Cohort_hier"] == cohorte]
                tombes_c = merged_c[(merged_c["Streak_hier"] > 0) & (merged_c["Streak_jour"] == 0)]
                actifs_hier_c = (
                    int(len(df_hier[(df_hier["Cohort"] == cohorte) & (df_hier["Streak"] > 0)]))
                    if "Cohort" in df_hier.columns
                    else 0
                )

                stats["cohortes"][cohorte]["tombes_zero"] = int(len(tombes_c))
                if actifs_hier_c > 0:
                    stats["cohortes"][cohorte]["churn"] = (len(tombes_c) / actifs_hier_c) * 100
                    stats["cohortes"][cohorte]["retention"] = ((actifs_hier_c - len(tombes_c)) / actifs_hier_c) * 100

            if "Cohort_jour" in merged.columns:
                transitions_deb_standard = merged[
                    (merged["Cohort_hier"] == "Debutants")
                    & (merged["Streak_hier"] > 0)
                    & (merged["Streak_jour"] > 0)
                    & (merged["Cohort_jour"] == "Standard")
                ]
                actifs_hier_debutants = (
                    int(len(df_hier[(df_hier["Cohort"] == "Debutants") & (df_hier["Streak"] > 0)]))
                    if "Cohort" in df_hier.columns
                    else 0
                )
                stats["progression_debutants_vers_standard"] = int(len(transitions_deb_standard))
                stats["cohortes"]["Debutants"]["transitions_sortantes"] = int(len(transitions_deb_standard))
                if actifs_hier_debutants > 0:
                    taux_transition = (len(transitions_deb_standard) / actifs_hier_debutants) * 100
                    stats["taux_progression_debutants_vers_standard"] = taux_transition
                    stats["cohortes"]["Debutants"]["transition_vers_standard"] = taux_transition
    else:
        print("  ⚠️ Aucune donnée pour hier — comparaison impossible.\n")
        stats["taux_retention"] = 0
        stats["taux_churn"] = 0
        stats["score_sante_jour"] = stats.get("score_sante_jour", 0)

    print(f"  📊 Statistiques du {aujourdhui} :")
    print(f"     • Série Moyenne (Streak) : {stats['moyenne_streak_jour']:.1f} j")
    if stats["moyenne_streak_hier"] is not None:
        evolution = stats["moyenne_streak_jour"] - stats["moyenne_streak_hier"]
        signe = "+" if evolution > 0 else ""
        print(f"     • Évolution de la série : {signe}{evolution:.1f} j vs hier")
        print(f"     • Intensité d'Apprentissage : +{stats['delta_xp_moyen']:.0f} XP/jour")

    print(f"     • Utilisateurs Actifs : {stats['utilisateurs_actifs']}")
    taux_super = stats.get("taux_conversion_plus")
    print(f"     • Pénétration Super Duolingo : {f'{taux_super:.1f}%' if isinstance(taux_super, numbers.Number) else 'N/D'}")
    print(f"     • Ruptures de Série (abandons) : {stats['streaks_tombes_zero']}")
    print(f"     • Taux d'Abandon Global : {stats.get('taux_churn', 0):.1f}%")
    print(f"     • Reactivations vs veille : {stats.get('reactivations_veille', 0)}")
    print(
        "     • Progression Débutants -> Standard : "
        f"{stats.get('progression_debutants_vers_standard', 0)} "
        f"({stats.get('taux_progression_debutants_vers_standard', 0):.1f}%)"
    )

    print("\n     • Analyse par Segment (Cohorte) :")
    for nom, donnees in stats["cohortes"].items():
        print(
            f"       - {nom:12s} : {donnees['actifs']}/{donnees['total']} actifs | "
            f"Taux d'abandon: {donnees['churn']:.1f}%"
        )

    print(f"\n     • Profils collectés aujourd'hui : {stats['nb_profils_jour']}\n")

    return stats
