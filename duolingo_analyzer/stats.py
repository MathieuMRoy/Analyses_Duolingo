"""
Partie 2 : Statistiques
Calcule les statistiques d'engagement via Pandas.
"""
import csv
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import numbers
import re
import pandas as pd

from .config import DAILY_LOG_FILE, GOOGLE_DRIVE_REPORT_DIR, RAPPORT_EXCEL_FILE, REPORT_DIR, now_toronto
from .excel_dashboard import refresh_trends_dashboard
from .financial_signals import build_financial_signal_sheet_df
from .quarterly_nowcast import build_quarterly_nowcast_raw_df
from .reporting.sheets.financial_nowcast_sheet import render_financial_nowcast_sheet
from .reporting.sheets.quarterly_nowcast_sheet import render_quarterly_nowcast_sheet
from .reporting.styles import build_style_context
from .subscription_detection import (
    apply_max_overrides,
    compute_max_count,
    compute_super_observable_count,
    parse_bool,
    parse_optional_bool,
)

SUMMARY_SHEET = "📊 Résumé Financier Q1"
AI_SHEET = "🤖 Analyse Stratégique"
GLOSSAIRE_SHEET = "📖 Dictionnaire des KPIs"
TRENDS_SHEET = "📈 Tendances Mensuelles"
CHART_DATA_SHEET = "📊 Données Graphique"

SIGNALS_SHEET = "Signaux Financiers"
SIGNALS_RAW_SHEET = "Signaux Financiers - Raw"
QUARTERLY_SHEET = "Nowcast Trimestriel"
QUARTERLY_RAW_SHEET = "Nowcast Trimestriel - Raw"

PERCENT_COLUMNS = {
    "Taux Abonn. Super",
    "Taux d'Abandon Global",
    "Score d'Engagement",
}

BAD_SHEET_NAMES = {
    "ðŸ“Š RÃ©sumÃ© Financier Q1",
    "ðŸ¤– Analyse StratÃ©gique",
    "ðŸ“– Dictionnaire des KPIs",
    "ðŸ“ˆ Tendances Mensuelles",
    "ðŸ“Š DonnÃ©es Graphique",
}

SUMMARY_COLUMNS = [
    "Date",
    "Série Moyenne (Jours)",
    "Évol. vs Veille",
    "Apprentissage (XP/j)",
    "Taux Abonn. Super",
    "Taux d'Abandon Global",
    "Reactivations vs Veille",
    "Score d'Engagement",
    "Panel Total",
]

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
    "Reactivations vs Veille": "Reactivations vs Veille",
    "Score Santé Global": "Score d'Engagement",
    "Score d'Engagement": "Score d'Engagement",
    "Total Profils": "Panel Total",
    "Panel Total": "Panel Total",
}

DAILY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]
SUMMARY_MIN_RELIABLE_DATE = pd.Timestamp("2026-03-16")


def _parse_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, numbers.Number):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.upper() in {"N/A", "NA"}:
            return None
        cleaned = raw.replace(",", ".")
        cleaned = cleaned.replace("XP", "").replace("xp", "")
        cleaned = cleaned.replace("%", "")
        cleaned = cleaned.replace("+", "")
        cleaned = cleaned.replace("−", "-")
        cleaned = "".join(ch for ch in cleaned if ch.isdigit() or ch in ".-")
        if cleaned in {"", "-", "."}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _normalize_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    normalized = pd.DataFrame(index=df.index)

    for column in SUMMARY_COLUMNS:
        normalized[column] = pd.Series(index=df.index, dtype="object")

    for source_column in df.columns:
        target_column = SUMMARY_COLUMN_ALIASES.get(str(source_column).strip())
        if not target_column:
            continue

        source_series = df[source_column]

        if target_column == "Date":
            parsed_series = pd.to_datetime(source_series, errors="coerce")
        else:
            parsed_series = source_series.apply(_parse_float)

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
                "Reactivations vs Veille": reactivations_veille,
                "Score d'Engagement": round(score_engagement / 100, 6),
                "Panel Total": int(len(df_jour)),
            }
        )

    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def _load_summary_sheet(report_path: Path) -> pd.DataFrame | None:
    try:
        workbook = pd.ExcelFile(report_path)
    except Exception:
        return None

    for sheet_name in [SUMMARY_SHEET, *BAD_SHEET_NAMES]:
        if sheet_name not in workbook.sheet_names:
            continue
        try:
            df_summary = pd.read_excel(report_path, sheet_name=sheet_name)
            if df_summary is not None and not df_summary.empty:
                return df_summary
        except Exception:
            continue

    for sheet_name in workbook.sheet_names:
        try:
            df_preview = pd.read_excel(report_path, sheet_name=sheet_name, nrows=5)
        except Exception:
            continue

        normalized_headers = {
            SUMMARY_COLUMN_ALIASES.get(str(column).strip())
            for column in df_preview.columns
        }
        if {"Date", "Panel Total"}.issubset(normalized_headers):
            try:
                df_summary = pd.read_excel(report_path, sheet_name=sheet_name)
                if df_summary is not None and not df_summary.empty:
                    return df_summary
            except Exception:
                continue

    return None


def _charger_resume_historique() -> pd.DataFrame | None:
    frames: list[pd.DataFrame] = []

    try:
        df_log = _load_daily_log_df()
        df_from_log = _build_summary_history_from_log(df_log)
        if df_from_log is not None and not df_from_log.empty:
            frames.append(df_from_log)
    except Exception:
        pass

    if RAPPORT_EXCEL_FILE.exists():
        try:
            df_resume = _load_summary_sheet(RAPPORT_EXCEL_FILE)
            if df_resume is not None and not df_resume.empty:
                frames.append(df_resume)
        except Exception:
            pass

    try:
        daily_reports = sorted(REPORT_DIR.glob("rapport_*.xlsx"))
    except Exception:
        daily_reports = []

    for report_path in daily_reports:
        if report_path == RAPPORT_EXCEL_FILE:
            continue
        try:
            df_tmp = _load_summary_sheet(report_path)
            if df_tmp is not None and not df_tmp.empty:
                frames.append(df_tmp)
        except Exception:
            continue

    if not frames:
        return None

    df_resume = pd.concat(frames, ignore_index=True)
    return _normalize_summary_df(df_resume)


def _copier_rapport_vers_google_drive(report_path: Path) -> Path | None:
    if not GOOGLE_DRIVE_REPORT_DIR:
        return None

    destination_dir = Path(GOOGLE_DRIVE_REPORT_DIR).expanduser()
    try:
        destination_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        print(f"  ⚠️ Impossible de créer le dossier Google Drive : {destination_dir} ({exc})")
        return None

    destination_file = destination_dir / report_path.name
    try:
        shutil.copy2(report_path, destination_file)
    except Exception as exc:
        print(f"  ⚠️ Copie Google Drive impossible : {destination_file} ({exc})")
        return None

    return destination_file


def _pretty_fr_number(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        numeric = float(value)
    except Exception:
        return str(value)

    if digits == 0:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.{digits}f}".replace(",", " ").replace(".", ",")


def _pretty_ratio_pct(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value) * 100:.{digits}f}%".replace(".", ",")
    except Exception:
        return "N/D"


def _pretty_delta_pts(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value) * 100:+.{digits}f} pts".replace(".", ",")
    except Exception:
        return "N/D"


def _pretty_score(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value):.{digits}f} / 100".replace(".", ",")
    except Exception:
        return "N/D"


def _format_estimation_vs_guidance_note(
    estimated_value: object,
    guidance_value: object,
    *,
    prefix: str = "Est.",
    guidance_label: str = "Guidance",
) -> str:
    estimated_text = (
        f"{_pretty_fr_number(estimated_value, 1)} M$"
        if isinstance(estimated_value, numbers.Number)
        else "N/D"
    )
    guidance_text = (
        f"{_pretty_fr_number(guidance_value, 1)} M$"
        if isinstance(guidance_value, numbers.Number)
        else "N/D"
    )
    return f"{prefix} {estimated_text} vs {guidance_label} {guidance_text}"


def _build_quarterly_model_explainer(
    *,
    revenue_prob: object,
    guidance_prob: object,
    revenue_reference: object,
    drivers: list[object],
    risks: list[object],
) -> str:
    primary_driver = str(drivers[0]).lstrip("- ").rstrip(".") if drivers else "la dynamique récente du panel"
    primary_risk = str(risks[0]).lstrip("- ").rstrip(".") if risks else "les limites actuelles de calibration"

    if isinstance(revenue_reference, numbers.Number):
        reference_text = f"{_pretty_fr_number(revenue_reference, 1)} M$"
        revenue_sentence = (
            f"La probabilité de beat revenus ressort à {_pretty_ratio_pct(revenue_prob, 1)} ; "
            f"elle mesure la chance implicite de dépasser la référence interne du trimestre, "
            f"actuellement ancrée sur la guidance management de {reference_text}."
        )
        guidance_sentence = (
            f"La probabilité de guidance raise ressort à {_pretty_ratio_pct(guidance_prob, 1)} et reste surtout portée par {primary_driver}."
            if _parse_float(guidance_prob) is not None and _parse_float(guidance_prob) >= 0.5
            else f"La probabilité de guidance raise ressort à {_pretty_ratio_pct(guidance_prob, 1)} et reste freinée par {primary_risk}."
        )
        return (
            "Notre modèle trimestriel combine la monétisation, l'engagement, la rétention, le churn, "
            "les réactivations et la couverture du panel. "
            + revenue_sentence
            + " "
            + guidance_sentence
        )

    return (
        "Notre modèle trimestriel combine la monétisation, l'engagement, la rétention, le churn, "
        "les réactivations et la couverture du panel. "
        f"La probabilité de beat revenus ressort à {_pretty_ratio_pct(revenue_prob, 1)} et s'appuie encore sur une référence interne de transition. "
        f"La probabilité de guidance raise ressort à {_pretty_ratio_pct(guidance_prob, 1)} et reste pour l'instant surtout influencée par {primary_risk}."
    )


def _build_quarterly_model_explainer(
    *,
    revenue_prob: object,
    guidance_prob: object,
    revenue_reference: object,
    drivers: list[object],
    risks: list[object],
) -> str:
    primary_driver = str(drivers[0]).lstrip("- ").rstrip(".") if drivers else "la dynamique récente du panel"
    primary_risk = str(risks[0]).lstrip("- ").rstrip(".") if risks else "les limites actuelles de calibration"

    base_sentence = (
        "Notre modèle trimestriel combine la monétisation, l'engagement, la rétention, "
        "le churn, les réactivations et la couverture du panel."
    )

    if isinstance(revenue_reference, numbers.Number):
        reference_text = f"{_pretty_fr_number(revenue_reference, 1)} M$"
        revenue_sentence = (
            f"La probabilité implicite de battre les revenus du trimestre ressort à {_pretty_ratio_pct(revenue_prob, 1)} ; "
            f"elle compare notre estimation à la référence interne du trimestre, actuellement ancrée sur la guidance management de {reference_text}."
        )
    else:
        revenue_sentence = (
            f"La probabilité implicite de battre les revenus du trimestre ressort à {_pretty_ratio_pct(revenue_prob, 1)} ; "
            "elle s'appuie encore sur une référence interne de transition, faute de guidance exploitable dans l'historique."
        )

    guidance_sentence = (
        f"La probabilité implicite d'un relèvement de guidance ressort à {_pretty_ratio_pct(guidance_prob, 1)} et reste surtout portée par {primary_driver}."
        if _parse_float(guidance_prob) is not None and _parse_float(guidance_prob) >= 0.5
        else f"La probabilité implicite d'un relèvement de guidance ressort à {_pretty_ratio_pct(guidance_prob, 1)} et reste freinée par {primary_risk}."
    )

    return base_sentence + " " + revenue_sentence + " " + guidance_sentence


def _label_signal_bias(value: object) -> str:
    mapping = {
        "favorable": "Favorable",
        "neutral": "Neutre",
        "unfavorable": "Defavorable",
    }
    return mapping.get(str(value or "").strip().lower(), "N/D")


def _label_confidence(value: object) -> str:
    mapping = {
        "high": "Elevee",
        "medium": "Moyenne",
        "low": "Faible",
    }
    return mapping.get(str(value or "").strip().lower(), "N/D")


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.replace("**", "")
    text = str(value).replace("\r", "\n").replace("•", "-")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _truncate_text(text: str, max_chars: int) -> str:
    clean = _normalize_text(text)
    if len(clean) <= max_chars:
        return clean
    shortened = clean[: max_chars - 1].rstrip(" ,;:-")
    return f"{shortened}…"


def _compact_summary_text(text: object, max_sentences: int = 2, max_chars: int = 180) -> str:
    clean = _normalize_text(text)
    if not clean:
        return "-"
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
    if not sentences:
        return clean

    selected: list[str] = []
    current_length = 0
    for sentence in sentences:
        candidate_length = current_length + (1 if selected else 0) + len(sentence)
        if selected and candidate_length > max_chars:
            break
        selected.append(sentence)
        current_length = candidate_length
        if len(selected) >= max_sentences:
            break

    return " ".join(selected) if selected else sentences[0]


def _compact_bullet_text(text: object, max_items: int = 2, max_chars: int = 95) -> str:
    clean = _normalize_text(text)
    if not clean:
        return "-"

    items: list[str] = []
    for raw_line in clean.splitlines():
        candidate = raw_line.lstrip("-* ").strip()
        if candidate:
            items.append(candidate)

    if not items:
        items = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]

    compact_items: list[str] = []
    current_length = 0
    for item in items:
        candidate_length = current_length + (1 if compact_items else 0) + len(item)
        if compact_items and candidate_length > max_chars:
            break
        if item and item not in compact_items:
            compact_items.append(item)
            current_length = candidate_length
        if len(compact_items) >= max_items:
            break

    if not compact_items:
        return "-"
    return "\n".join(f"- {item}" for item in compact_items)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.replace("**", "")
    text = str(value).replace("\r", "\n")
    text = text.replace("•", "-").replace("â€¢", "-")
    text = text.replace("…", "...").replace("â€¦", "...")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _truncate_text(text: str, max_chars: int) -> str:
    clean = _normalize_text(text)
    if len(clean) <= max_chars:
        return clean
    shortened = clean[: max_chars - 3].rstrip(" ,;:-")
    return f"{shortened}..."


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

    # Un seul enregistrement par utilisateur par jour
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

        # Tant que HasMax n'est pas observe de facon fiable,
        # le taux Super reste un taux premium observable base sur HasPlus.
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

        tombes_a_zero = merged[
            (merged["Streak_hier"] > 0) & (merged["Streak_jour"] == 0)
        ]
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
                tombes_c = merged_c[
                    (merged_c["Streak_hier"] > 0) & (merged_c["Streak_jour"] == 0)
                ]
                actifs_hier_c = (
                    int(len(df_hier[(df_hier["Cohort"] == cohorte) & (df_hier["Streak"] > 0)]))
                    if "Cohort" in df_hier.columns
                    else 0
                )

                stats["cohortes"][cohorte]["tombes_zero"] = int(len(tombes_c))
                if actifs_hier_c > 0:
                    stats["cohortes"][cohorte]["churn"] = (len(tombes_c) / actifs_hier_c) * 100
                    stats["cohortes"][cohorte]["retention"] = (
                        (actifs_hier_c - len(tombes_c)) / actifs_hier_c
                    ) * 100

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


def sauvegarder_rapport_excel(
    stats: dict,
    ia_report: str = None,
    financial_signals: dict | None = None,
    quarterly_nowcast: dict | None = None,
) -> None:
    """
    Exporte les données d'engagement et les statistiques dans un fichier Excel (.xlsx)
    avec un design premium et l'analyse de l'IA.
    """
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.chart import LineChart, Reference
    from openpyxl.chart.layout import Layout, ManualLayout

    print("  📂 Génération du rapport Excel Premium...")

    if not DAILY_LOG_FILE.exists():
        return

    try:
        df = _load_daily_log_df()
        df = df[~df["Username"].str.contains("Aggregated", na=False)]
        df = df[df["Cohort"] != "Global"]
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.sort_values(["Date", "Username"]).drop_duplicates(subset=["Date", "Username"], keep="last")

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

        df_stats = pd.DataFrame([{
            "Date": date_obj or date_jour,
            "Série Moyenne (Jours)": moyenne_streak_jour,
            "Évol. vs Veille": delta_streak,
            "Apprentissage (XP/j)": round(stats.get("delta_xp_moyen", 0), 0),
            "Taux Abonn. Super": round(stats["taux_conversion_plus"] / 100, 6)
                if isinstance(stats.get("taux_conversion_plus"), numbers.Number) else None,
            "Taux d'Abandon Global": round(stats.get("taux_churn", 0) / 100, 6),
            "Reactivations vs Veille": stats.get("reactivations_veille", 0),
            "Score d'Engagement": round(stats.get("score_sante_jour", 0) / 100, 6),
            "Panel Total": stats.get("nb_profils_jour"),
        }])

        df_resume = _charger_resume_historique()
        if df_resume is None or df_resume.empty:
            df_resume = df_stats.copy()
        else:
            df_resume = _normalize_summary_df(df_resume)
            if "Date" in df_resume.columns:
                df_resume["Date"] = pd.to_datetime(df_resume["Date"], errors="coerce")
            df_stats["Date"] = pd.to_datetime(df_stats["Date"], errors="coerce")
            date_value = df_stats.loc[0, "Date"] if "Date" in df_stats.columns else None
            if "Date" in df_resume.columns and pd.notna(date_value):
                df_resume = df_resume[df_resume["Date"] != date_value]
            df_resume = pd.concat([df_resume, df_stats], ignore_index=True, sort=False)

        if "Date" in df_resume.columns:
            df_resume["Date"] = pd.to_datetime(df_resume["Date"], errors="coerce")
            df_resume = df_resume.sort_values("Date").reset_index(drop=True)

        writer_mode = "a" if RAPPORT_EXCEL_FILE.exists() else "w"
        writer_kwargs = {"if_sheet_exists": "replace"} if writer_mode == "a" else {}

        with pd.ExcelWriter(RAPPORT_EXCEL_FILE, engine="openpyxl", mode=writer_mode, **writer_kwargs) as writer:
            df_resume.to_excel(writer, sheet_name=SUMMARY_SHEET, index=False)

            if financial_signals:
                signal_sheet_df = build_financial_signal_sheet_df(financial_signals)
                pd.DataFrame().to_excel(writer, sheet_name=SIGNALS_SHEET, index=False)
                signal_sheet_df.to_excel(writer, sheet_name=SIGNALS_RAW_SHEET, index=False)
            if quarterly_nowcast:
                quarterly_sheet_df = build_quarterly_nowcast_raw_df(quarterly_nowcast)
                pd.DataFrame().to_excel(writer, sheet_name=QUARTERLY_SHEET, index=False)
                quarterly_sheet_df.to_excel(writer, sheet_name=QUARTERLY_RAW_SHEET, index=False)

            df_glossaire = pd.DataFrame([
                {"KPI": "Moyenne Streak (J)", "Définition": "Longueur moyenne de la série de jours consécutifs d'utilisation. Mesure la fidélité à long terme."},
                {"KPI": "Apprentissage (XP/j)", "Définition": "Gain moyen de points d'expérience (XP) depuis hier. Mesure l'effort d'apprentissage quotidien."},
                {"KPI": "Taux Abonn. Super", "Définition": "Part observable du panel premium via hasPlus. Tant que Max n'est pas détecté de façon fiable, ce taux peut encore inclure une partie des comptes Max."},
                {"KPI": "Taux d'Abandon Global", "Définition": "Pourcentage d'utilisateurs actifs hier qui ne le sont plus aujourd'hui (streak retombe à 0)."},
                {"KPI": "Reactivations vs Veille", "Définition": "Nombre d'utilisateurs inactifs hier (streak à 0) redevenus actifs aujourd'hui."},
                {"KPI": "Progression Débutants vers Standard", "Définition": "Part des Débutants actifs hier qui sont encore actifs aujourd'hui et ont progressé vers la cohorte Standard."},
                {"KPI": "Abandon Débutants", "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Débutants et actifs hier, puis sont tombés à une streak de 0 aujourd'hui."},
                {"KPI": "Abandon Standard", "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Standard et actifs hier, puis sont tombés à une streak de 0 aujourd'hui."},
                {"KPI": "Abandon Super-Actifs", "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Super-Actifs et actifs hier, puis sont tombés à une streak de 0 aujourd'hui."},
                {"KPI": "Score Santé Global", "Définition": "Pourcentage des utilisateurs suivis qui ont un streak > 0 aujourd'hui."},
            ])
            df_glossaire["Méthode / calcul"] = df_glossaire["KPI"].map({
                "Moyenne Streak (J)": "Moyenne simple du streak observé sur le panel du jour.",
                "Apprentissage (XP/j)": "Pour chaque profil, delta de TotalXP vs veille avec plancher à 0, puis moyenne sur le panel.",
                "Taux Abonn. Super": "Nombre de profils premium observables / nombre total de profils observés dans le panel.",
                "Taux d'Abandon Global": "Utilisateurs actifs hier devenus inactifs aujourd'hui / utilisateurs actifs hier.",
                "Reactivations vs Veille": "Comptage simple des profils passés de streak 0 à streak > 0 entre hier et aujourd'hui.",
                "Progression Débutants vers Standard": "Nombre de Débutants actifs hier observés aujourd'hui en Standard / Débutants actifs hier.",
                "Abandon Débutants": "Débutants actifs hier devenus inactifs aujourd'hui / Débutants actifs hier.",
                "Abandon Standard": "Standard actifs hier devenus inactifs aujourd'hui / Standard actifs hier.",
                "Abandon Super-Actifs": "Super-Actifs actifs hier devenus inactifs aujourd'hui / Super-Actifs actifs hier.",
                "Score Santé Global": "Utilisateurs avec streak > 0 / panel observé du jour.",
            }).fillna("")
            df_glossaire = pd.concat(
                [
                    df_glossaire,
                    pd.DataFrame([
                        {
                            "KPI": "Confiance (trimestriel)",
                            "Définition": "Niveau de fiabilité du nowcast trimestriel.",
                            "Méthode / calcul": "Élevée si couverture moyenne >= 75% et jours observés >= 20 ; Moyenne si couverture >= 45% et jours >= 10 ; sinon Faible.",
                        },
                        {
                            "KPI": "Score trimestre",
                            "Définition": "Score synthétique 0-100 du trimestre, utilisé pour lire le biais global du nowcast.",
                            "Méthode / calcul": "50 + 50 x (0,28 monétisation + 0,18 engagement + 0,16 momentum Super + 0,08 momentum Max + 0,15 churn + 0,08 réactivations + 0,07 rétention high-value), puis ajustement par breadth factor.",
                        },
                        {
                            "KPI": "Breadth factor",
                            "Définition": "Facteur qui pénalise un trimestre encore peu couvert ou trop jeune.",
                            "Méthode / calcul": "min(couverture moyenne / 70%, 1) x min(jours observés / 21, 1). Il réduit l'amplitude des scores si le trimestre est encore peu observé.",
                        },
                        {
                            "KPI": "Prob. beat revenus",
                            "Définition": "Probabilité implicite, non supervisée, de battre les revenus du trimestre.",
                            "Méthode / calcul": "On calcule d'abord un score revenus, puis base_prob = 10% + 80% x score/100. Cette base est ensuite ramenée vers 50% selon la confiance : Élevée 1,0 ; Moyenne 0,7 ; Faible 0,45.",
                        },
                        {
                            "KPI": "Prob. beat EBITDA",
                            "Définition": "Probabilité implicite, non supervisée, de battre l'EBITDA trimestriel.",
                            "Méthode / calcul": "Même transformation que pour les revenus, mais à partir d'un score EBITDA construit à partir de monétisation, engagement, churn, réactivations, rétention high-value et momentum Max.",
                        },
                        {
                            "KPI": "Prob. guidance raise",
                            "Définition": "Probabilité implicite, non supervisée, d'un relèvement de guidance sur le trimestre suivant.",
                            "Méthode / calcul": "Même transformation score -> probabilité, appliquée à un score guidance reposant surtout sur monétisation, momentum Super, engagement, churn et réactivations.",
                        },
                        {
                            "KPI": "Revenus estimés",
                            "Définition": "Estimation interne des revenus trimestriels cohérente avec le signal du panel et les références historiques.",
                            "Méthode / calcul": "Si une guidance revenus de référence existe, estimation = guidance x (1 + beat implicite). Le beat implicite part du beat historique médian et est ajusté par la prob. revenus, avec bornes [-3%, +8%]. Sinon fallback sur le CA réel du trimestre précédent avec une croissance QoQ implicite bornée [-5%, +15%].",
                        },
                        {
                            "KPI": "EBITDA estimé",
                            "Définition": "Estimation interne de l'EBITDA ajusté du trimestre.",
                            "Méthode / calcul": "EBITDA estimé = revenus estimés x marge implicite. La marge implicite part de la marge EBITDA historique médiane et est ajustée par la prob. EBITDA, avec bornes [18%, 40%].",
                        },
                        {
                            "KPI": "Guide N+1 estimé",
                            "Définition": "Estimation interne de la guidance revenus du trimestre suivant.",
                            "Méthode / calcul": "Guide N+1 estimé = revenus estimés x ratio implicite. Le ratio part du ratio historique médian guidance N+1 / revenus et est ajusté par la prob. guidance raise, avec bornes [0,98x, 1,12x].",
                        },
                    ]),
                ],
                ignore_index=True,
            )
            definition_columns = [column for column in df_glossaire.columns if "finition" in str(column)]
            if definition_columns:
                primary_definition_column = definition_columns[0]
                for extra_column in definition_columns[1:]:
                    primary_series = df_glossaire[primary_definition_column]
                    fill_mask = primary_series.isna() | (primary_series.astype(str).str.strip() == "") | (primary_series.astype(str) == "None")
                    df_glossaire.loc[fill_mask, primary_definition_column] = df_glossaire.loc[fill_mask, extra_column]
                df_glossaire = df_glossaire.drop(columns=definition_columns[1:], errors="ignore")
                df_glossaire = df_glossaire.rename(columns={primary_definition_column: "Définition"})

            method_columns = [column for column in df_glossaire.columns if "thode" in str(column).lower() or "ethode" in str(column).lower()]
            if method_columns:
                primary_method_column = method_columns[0]
                for extra_column in method_columns[1:]:
                    primary_series = df_glossaire[primary_method_column]
                    fill_mask = primary_series.isna() | (primary_series.astype(str).str.strip() == "") | (primary_series.astype(str) == "None")
                    df_glossaire.loc[fill_mask, primary_method_column] = df_glossaire.loc[fill_mask, extra_column]
                df_glossaire = df_glossaire.drop(columns=method_columns[1:], errors="ignore")
                df_glossaire = df_glossaire.rename(columns={primary_method_column: "Méthode / calcul"})

            ordered_glossary_columns = [column for column in ["KPI", "Définition", "Méthode / calcul"] if column in df_glossaire.columns]
            df_glossaire = df_glossaire[ordered_glossary_columns]
            df_glossaire.to_excel(writer, sheet_name=GLOSSAIRE_SHEET, index=False)

        from openpyxl import load_workbook

        try:
            df_cleaned = df.copy()
            df_cleaned["Month"] = pd.to_datetime(df_cleaned["Date"]).dt.to_period("M").astype(str)

            df_sorted = df_cleaned.sort_values(["Username", "Date"])
            df_sorted["Prev_XP"] = df_sorted.groupby("Username")["TotalXP"].shift(1)
            df_sorted["Daily_Delta"] = df_sorted["TotalXP"] - df_sorted["Prev_XP"]
            df_sorted.loc[df_sorted["Daily_Delta"] < 0, "Daily_Delta"] = 0

            cohort_column = "Cohorte" if "Cohorte" in df_sorted.columns else "Cohort"

            monthly_cohorts = df_sorted.groupby(["Month", cohort_column]).agg({
                "Streak": "mean",
                "HasPlus": "mean",
                "Daily_Delta": "mean",
            }).reset_index()
            monthly_cohorts.columns = ["Mois", "Cohorte", "Série Moy. (j)", "Taux Super (%)", "Activité (XP/j)"]

            monthly_global = df_sorted.groupby(["Month"]).agg({
                "Streak": "mean",
                "HasPlus": "mean",
                "Daily_Delta": "mean",
            }).reset_index()
            monthly_global["Cohorte"] = "Global"
            monthly_global = monthly_global[["Month", "Cohorte", "Streak", "HasPlus", "Daily_Delta"]]
            monthly_global.columns = ["Mois", "Cohorte", "Série Moy. (j)", "Taux Super (%)", "Activité (XP/j)"]

            monthly_stats = pd.concat([monthly_cohorts, monthly_global], ignore_index=True)
            monthly_stats = monthly_stats.sort_values(["Cohorte", "Mois"])

            for col in ["Série Moy. (j)", "Taux Super (%)", "Activité (XP/j)"]:
                new_col = f"Δ {col} (MoM)"
                monthly_stats[new_col] = monthly_stats.groupby("Cohorte")[col].diff()

            monthly_stats = monthly_stats.sort_values(["Mois", "Cohorte"])
            monthly_stats["Taux Super (%)"] = monthly_stats["Taux Super (%)"].round(6)
            monthly_stats["Δ Taux Super (%) (MoM)"] = monthly_stats["Δ Taux Super (%) (MoM)"].round(6)

        except Exception as e:
            print(f"  ⚠️ Erreur calcul mensuel : {e}")
            monthly_stats = pd.DataFrame()

        with pd.ExcelWriter(RAPPORT_EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            if not monthly_stats.empty:
                monthly_stats.to_excel(writer, sheet_name=TRENDS_SHEET, index=False)

            try:
                date_counts = df.groupby("Date").size()
                valid_dates = date_counts[date_counts > 1000].index
                df_filtered = df[df["Date"].isin(valid_dates)]

                if df_filtered.empty:
                    df_filtered = df

                df_daily_conv = df_filtered.groupby(["Date", "Cohort"])["HasPlus"].mean().unstack() * 100
                df_daily_conv["Global"] = df_filtered.groupby("Date")["HasPlus"].mean() * 100
                df_daily_conv = df_daily_conv.reset_index()

                numeric_cols = df_daily_conv.select_dtypes(include="number").columns
                df_daily_conv[numeric_cols] = df_daily_conv[numeric_cols].round(1)

                for c in ["Debutants", "Standard", "Super-Actifs"]:
                    if c not in df_daily_conv.columns:
                        df_daily_conv[c] = 0.0

                cols_map = {
                    "Date": "Date",
                    "Global": "Moyenne Panel Global",
                    "Debutants": "Cohorte Débutants (<1k XP)",
                    "Standard": "Cohorte Standard (1k-5k XP)",
                    "Super-Actifs": "Cohorte Super-Actifs (>5k XP)",
                }
                df_daily_conv = df_daily_conv[list(cols_map.keys())].rename(columns=cols_map)
                df_daily_conv["Date"] = pd.to_datetime(df_daily_conv["Date"], errors="coerce")

                df_daily_conv.to_excel(writer, sheet_name=CHART_DATA_SHEET, index=False)

            except Exception as e:
                print(f"  ⚠️ Erreur préparation données graphique : {e}")

        wb = load_workbook(RAPPORT_EXCEL_FILE)

        for bad_name in BAD_SHEET_NAMES:
            if bad_name in wb.sheetnames:
                del wb[bad_name]

        if AI_SHEET in wb.sheetnames:
            del wb[AI_SHEET]

        if SIGNALS_RAW_SHEET in wb.sheetnames:
            wb[SIGNALS_RAW_SHEET].sheet_state = "hidden"
        if QUARTERLY_RAW_SHEET in wb.sheetnames:
            wb[QUARTERLY_RAW_SHEET].sheet_state = "hidden"

        style_ctx = build_style_context()
        DUO_GREEN = style_ctx["DUO_GREEN"]
        DUO_BLUE = style_ctx["DUO_BLUE"]
        NAVY = style_ctx["NAVY"]
        LIGHT_GREY = style_ctx["LIGHT_GREY"]
        GREEN_SOFT = style_ctx["GREEN_SOFT"]
        AMBER_SOFT = style_ctx["AMBER_SOFT"]
        RED_SOFT = style_ctx["RED_SOFT"]
        WHITE = style_ctx["WHITE"]
        BASE_FONT_NAME = style_ctx["BASE_FONT_NAME"]
        header_fill = style_ctx["header_fill"]
        header_font = style_ctx["header_font"]
        zebra_fill = style_ctx["zebra_fill"]
        success_fill = style_ctx["success_fill"]
        warning_fill = style_ctx["warning_fill"]
        alert_fill = style_ctx["alert_fill"]
        white_fill = style_ctx["white_fill"]
        base_font = style_ctx["base_font"]
        center_align = style_ctx["center_align"]
        left_align = style_ctx["left_align"]
        thin_border = style_ctx["thin_border"]

        render_helpers = {
            "label_signal_bias": _label_signal_bias,
            "label_confidence": _label_confidence,
            "pretty_fr_number": _pretty_fr_number,
            "pretty_ratio_pct": _pretty_ratio_pct,
            "pretty_score": _pretty_score,
            "pretty_delta_pts": _pretty_delta_pts,
            "compact_summary_text": _compact_summary_text,
            "compact_bullet_text": _compact_bullet_text,
        }

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws.sheet_view.showGridLines = False

            if sheet_name == AI_SHEET:
                sections = {
                    "TITRE": "Tableau de Bord Stratégique",
                    "RESUME": "Pas d'analyse disponible.",
                    "TENDANCES": "-",
                    "ATTENTION": "-",
                    "CONSEILS": "-",
                }

                if ia_report:
                    import re
                    for key in sections.keys():
                        match = re.search(f"\\[{key}\\](.*?)(?=\\[|$)", ia_report, re.DOTALL)
                        if match:
                            sections[key] = match.group(1).strip()

                ws.column_dimensions["A"].width = 45
                ws.column_dimensions["B"].width = 45
                ws.column_dimensions["C"].width = 45

                ws.merge_cells("A1:C2")
                cell_title = ws["A1"]
                cell_title.value = f"🦉 {sections['TITRE']}"
                cell_title.font = Font(name=BASE_FONT_NAME, size=18, bold=True, color=WHITE)
                cell_title.alignment = center_align
                cell_title.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")

                headers = [
                    ("A4", "📝 RÉSUMÉ EXÉCUTIF", DUO_BLUE),
                    ("B4", "📈 TENDANCES CLÉS", NAVY),
                    ("C4", "⚠️ POINTS D'ATTENTION", RED_SOFT),
                ]
                for cell_ref, title, color in headers:
                    cell = ws[cell_ref]
                    cell.value = title
                    cell.font = Font(
                        name=BASE_FONT_NAME,
                        size=11,
                        bold=True,
                        color=WHITE if color != RED_SOFT else "000000",
                    )
                    cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                    cell.alignment = center_align
                    cell.border = thin_border

                contents = [
                    ("A5", sections["RESUME"]),
                    ("B5", sections["TENDANCES"]),
                    ("C5", sections["ATTENTION"]),
                ]
                ws.row_dimensions[5].height = 200

                for cell_ref, value in contents:
                    cell = ws[cell_ref]
                    cell.value = value
                    cell.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                    cell.font = Font(name=BASE_FONT_NAME, size=10)
                    cell.border = thin_border
                    cell.fill = PatternFill(start_color=WHITE, end_color=WHITE, fill_type="solid")

                ws.merge_cells("A7:C7")
                ws["A7"].value = "💡 RECOMMANDATIONS STRATÉGIQUES ET ACTIONS CONCRÈTES"
                ws["A7"].font = Font(name=BASE_FONT_NAME, size=11, bold=True, color=WHITE)
                ws["A7"].fill = PatternFill(start_color=DUO_GREEN, end_color=DUO_GREEN, fill_type="solid")
                ws["A7"].alignment = center_align
                ws["A7"].border = thin_border

                ws.merge_cells("A8:C12")
                ws["A8"].value = sections["CONSEILS"]
                ws["A8"].alignment = Alignment(wrap_text=True, vertical="top", indent=2)
                ws["A8"].font = Font(name=BASE_FONT_NAME, size=11, bold=True)
                ws["A8"].border = thin_border
                ws.row_dimensions[8].height = 120

                ws.sheet_view.showGridLines = False
                continue

            if sheet_name == SIGNALS_SHEET and financial_signals:
                render_financial_nowcast_sheet(ws, financial_signals, ia_report, style_ctx, render_helpers)
                ws.freeze_panes = "A5"
                continue

            if sheet_name == QUARTERLY_SHEET and quarterly_nowcast:
                render_quarterly_nowcast_sheet(ws, quarterly_nowcast, wb, QUARTERLY_RAW_SHEET, style_ctx)
                ws.freeze_panes = "A5"
                continue

            if sheet_name == TRENDS_SHEET:
                try:
                    data_ws = wb[CHART_DATA_SHEET]
                    chart = LineChart()
                    chart.title = "Tendances de Monétisation : Taux d'Abonnement Super par Cohorte"
                    chart.style = 13
                    chart.y_axis.title = "\nTaux de Conversion (%)"
                    chart.x_axis.title = "\nCalendrier des Relevés"
                    chart.height = 14
                    chart.width = 25

                    chart.x_axis.delete = False
                    chart.y_axis.delete = False
                    chart.x_axis.tickLblPos = "low"
                    chart.y_axis.tickLblPos = "low"
                    chart.x_axis.majorTickMark = "out"
                    chart.y_axis.majorTickMark = "out"

                    chart.plot_area.layout = Layout(
                        manualLayout=ManualLayout(
                            x=0.2,
                            y=0.05,
                            h=0.7,
                            w=0.6,
                            xMode="edge",
                            yMode="edge",
                        )
                    )

                    max_row = data_ws.max_row
                    dates = Reference(data_ws, min_col=1, min_row=2, max_row=max_row)

                    for i in range(2, 6):
                        values = Reference(data_ws, min_col=i, min_row=1, max_row=max_row)
                        chart.add_data(values, titles_from_data=True)

                    chart.set_categories(dates)
                    chart.legend.position = "r"

                    colors = ["1CB0F6", "FF4B4B", "FFC800", "58CC02"]
                    for i, s in enumerate(chart.series):
                        s.graphicalProperties.line.solidFill = colors[i % len(colors)]
                        s.graphicalProperties.line.width = 25000 if i == 0 else 15000
                        s.marker.symbol = "circle"
                        s.marker.size = 5
                        s.marker.graphicalProperties.solidFill = colors[i % len(colors)]
                        s.marker.graphicalProperties.line.solidFill = colors[i % len(colors)]

                    ws.add_chart(chart, "J2")
                    data_ws.sheet_state = "hidden"

                except Exception as e:
                    print(f"  ⚠️ Erreur ajout graphique : {e}")

            ws.row_dimensions[1].height = 22
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = center_align
                cell.border = thin_border

            for row_idx, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row), start=2):
                is_zebra = row_idx % 2 == 0
                for cell in row:
                    cell.border = thin_border
                    header_value = ws.cell(1, cell.column).value
                    header_key = str(header_value).strip().lower() if header_value is not None else ""

                    is_delta = ("Δ" in str(header_value)) or ("delta" in header_key) or ("evol" in header_key)
                    is_percent = ("%" in str(header_value)) or any(
                        k in header_key for k in ["taux", "attrition", "abandon", "score", "pénétration", "penetration"]
                    )
                    is_xp = "xp" in header_key
                    is_streak = ("série" in header_key) or ("serie" in header_key) or ("streak" in header_key)
                    is_panel = any(k in header_key for k in ["panel", "total", "profils", "actifs", "reactiv"])

                    if isinstance(cell.value, numbers.Number) and cell.value != cell.value:
                        cell.value = None

                    if isinstance(cell.value, datetime):
                        cell.alignment = center_align
                        cell.font = base_font
                        cell.number_format = "yyyy-mm-dd"

                    elif isinstance(cell.value, numbers.Number):
                        cell.alignment = center_align
                        cell.font = base_font

                        if is_delta and is_percent:
                            if sheet_name == CHART_DATA_SHEET:
                                cell.number_format = '+0.0"%" ;-0.0"%" ;0.0"%"'
                            else:
                                cell.number_format = "+0.0%;-0.0%;0.0%"
                        elif is_percent:
                            if sheet_name == CHART_DATA_SHEET:
                                cell.number_format = '0.0"%"'
                            else:
                                cell.number_format = "0.0%"
                        elif is_delta and is_xp:
                            cell.number_format = '+#,##0" XP";-#,##0" XP";0" XP"'
                        elif is_xp:
                            cell.number_format = '#,##0" XP"'
                        elif is_delta:
                            cell.number_format = "+0.0;-0.0;0.0"
                        elif is_streak:
                            cell.number_format = "0.0"
                        elif is_panel:
                            cell.number_format = "#,##0"

                        if is_delta:
                            if cell.value > 0:
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="008000", bold=True)
                            elif cell.value < 0:
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="C00000", bold=True)

                    elif cell.value is None and is_delta:
                        cell.value = "N/A"
                        cell.alignment = center_align
                        cell.font = base_font

                    else:
                        cell.alignment = left_align
                        cell.font = base_font
                        if "définition" in header_key or "definition" in header_key:
                            cell.alignment = Alignment(wrap_text=True, vertical="top", indent=1)

                    if is_zebra:
                        cell.fill = zebra_fill

                    if sheet_name == SUMMARY_SHEET and any(k in header_key for k in ["attrition", "abandon", "churn"]):
                        try:
                            metric_value = float(cell.value)
                            if metric_value <= 0.02:
                                cell.fill = success_fill
                            elif metric_value <= 0.05:
                                cell.fill = warning_fill
                            else:
                                cell.fill = alert_fill
                        except Exception:
                            pass

                    elif sheet_name == SUMMARY_SHEET and "reactiv" in header_key:
                        try:
                            if float(cell.value) > 0:
                                cell.fill = success_fill
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="008000", bold=True)
                        except Exception:
                            pass

                    elif sheet_name == SUMMARY_SHEET and "progression" in header_key:
                        try:
                            if float(cell.value) > 0:
                                cell.fill = success_fill
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="008000", bold=True)
                        except Exception:
                            pass

            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except Exception:
                        pass

                adjusted_width = max_length + 4
                max_width = 70 if sheet_name == GLOSSAIRE_SHEET else 60
                ws.column_dimensions[column].width = min(adjusted_width, max_width)

            if ws.max_row > 1:
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions

        ordered_sheet_names = [
            SUMMARY_SHEET,
            SIGNALS_SHEET,
            QUARTERLY_SHEET,
            TRENDS_SHEET,
            GLOSSAIRE_SHEET,
            SIGNALS_RAW_SHEET,
            QUARTERLY_RAW_SHEET,
            CHART_DATA_SHEET,
        ]
        wb._sheets = sorted(
            wb._sheets,
            key=lambda sheet: ordered_sheet_names.index(sheet.title)
            if sheet.title in ordered_sheet_names
            else len(ordered_sheet_names),
        )

        wb.save(RAPPORT_EXCEL_FILE)
        refresh_trends_dashboard(RAPPORT_EXCEL_FILE)
        wb = load_workbook(RAPPORT_EXCEL_FILE)
        if SIGNALS_RAW_SHEET in wb.sheetnames:
            wb[SIGNALS_RAW_SHEET].sheet_state = "hidden"
        if QUARTERLY_RAW_SHEET in wb.sheetnames:
            wb[QUARTERLY_RAW_SHEET].sheet_state = "hidden"
        if CHART_DATA_SHEET in wb.sheetnames:
            wb[CHART_DATA_SHEET].sheet_state = "hidden"
        ordered_sheet_names = [
            SUMMARY_SHEET,
            SIGNALS_SHEET,
            QUARTERLY_SHEET,
            TRENDS_SHEET,
            GLOSSAIRE_SHEET,
            SIGNALS_RAW_SHEET,
            QUARTERLY_RAW_SHEET,
            CHART_DATA_SHEET,
        ]
        wb._sheets = sorted(
            wb._sheets,
            key=lambda sheet: ordered_sheet_names.index(sheet.title)
            if sheet.title in ordered_sheet_names
            else len(ordered_sheet_names),
        )
        wb.save(RAPPORT_EXCEL_FILE)
        google_drive_file = _copier_rapport_vers_google_drive(RAPPORT_EXCEL_FILE)

        print("  ✅ Rapport Excel Premium sauvegardé dans :")
        print(f"     → {RAPPORT_EXCEL_FILE}")
        if google_drive_file is not None:
            print(f"     → Copie Google Drive : {google_drive_file}")

    except Exception as e:
        import traceback
        print(f"  ❌ Erreur lors de la sauvegarde Excel : {e}")
        traceback.print_exc()
