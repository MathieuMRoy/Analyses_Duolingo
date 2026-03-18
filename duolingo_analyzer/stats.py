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

SUMMARY_SHEET = "📊 Résumé Financier Q1"
AI_SHEET = "🤖 Analyse Stratégique"
GLOSSAIRE_SHEET = "📖 Dictionnaire des KPIs"
TRENDS_SHEET = "📈 Tendances Mensuelles"
CHART_DATA_SHEET = "📊 Données Graphique"

SIGNALS_SHEET = "Signaux Financiers"
SIGNALS_RAW_SHEET = "Signaux Financiers - Raw"

PERCENT_COLUMNS = {
    "Taux Abonn. Super",
    "Taux Abonn. Max",
    "Taux d'Abandon Global",
    "Abandon Débutants",
    "Abandon Standard",
    "Abandon Super-Actifs",
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
    "Taux Abonn. Max",
    "Taux d'Abandon Global",
    "Reactivations vs Veille",
    "Abandon Débutants",
    "Abandon Standard",
    "Abandon Super-Actifs",
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
    "Churn Débutants": "Abandon Débutants",
    "Abandon Débutants": "Abandon Débutants",
    "Churn Standard": "Abandon Standard",
    "Churn Standard ": "Abandon Standard",
    "Abandon Standard": "Abandon Standard",
    "Abandon Standard ": "Abandon Standard",
    "Churn Super-Actifs": "Abandon Super-Actifs",
    "Abandon Super-Actifs": "Abandon Super-Actifs",
    "Score Santé Global": "Score d'Engagement",
    "Score d'Engagement": "Score d'Engagement",
    "Total Profils": "Panel Total",
    "Panel Total": "Panel Total",
}

DAILY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]


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
        normalized = normalized.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")

    return normalized.reset_index(drop=True)


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"true", "1", "yes", "y", "vrai"}


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
                normalized_row.append("False")
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
    df["HasPlus"] = df["HasPlus"].apply(_parse_bool)
    df["HasMax"] = df["HasMax"].apply(_parse_bool)
    df = df[df["Date"].notna()]
    return df


def _charger_resume_historique() -> pd.DataFrame | None:
    if RAPPORT_EXCEL_FILE.exists():
        try:
            df_resume = pd.read_excel(RAPPORT_EXCEL_FILE, sheet_name=SUMMARY_SHEET)
            if df_resume is not None and not df_resume.empty:
                return _normalize_summary_df(df_resume)
        except Exception:
            pass

    try:
        daily_reports = sorted(REPORT_DIR.glob("rapport_*.xlsx"))
    except Exception:
        daily_reports = []

    frames: list[pd.DataFrame] = []
    for report_path in daily_reports:
        if report_path == RAPPORT_EXCEL_FILE:
            continue
        try:
            df_tmp = pd.read_excel(report_path, sheet_name=SUMMARY_SHEET)
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
        "nb_profils_jour": len(df_jour),
        "nb_profils_hier": len(df_hier),
        "taux_conversion_plus": None,
        "taux_conversion_max": None,
        "delta_xp_moyen": 0.0,
        "cohortes": {
            "Debutants": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0},
            "Standard": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0},
            "Super-Actifs": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0},
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

        # Super = HasPlus hors Max
        # Max = HasMax
        if stats["nb_profils_jour"] > 0:
            if "HasMax" in df_jour.columns:
                abonnes_max = int(len(df_jour[df_jour["HasMax"] == True]))
                stats["taux_conversion_max"] = (abonnes_max / stats["nb_profils_jour"]) * 100
            else:
                abonnes_max = 0
                stats["taux_conversion_max"] = None

            if "HasPlus" in df_jour.columns:
                if "HasMax" in df_jour.columns:
                    abonnes_super = int(len(df_jour[(df_jour["HasPlus"] == True) & (df_jour["HasMax"] != True)]))
                else:
                    abonnes_super = int(len(df_jour[df_jour["HasPlus"] == True]))
                stats["taux_conversion_plus"] = (abonnes_super / stats["nb_profils_jour"]) * 100
            else:
                stats["taux_conversion_plus"] = None

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

        if "Cohort_jour" in merged.columns:
            for cohorte in stats["cohortes"].keys():
                merged_c = merged[merged["Cohort_jour"] == cohorte]
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
    taux_max = stats.get("taux_conversion_max")
    print(f"     • Pénétration Super Duolingo : {f'{taux_super:.1f}%' if isinstance(taux_super, numbers.Number) else 'N/D'}")
    print(f"     • Pénétration Duolingo Max    : {f'{taux_max:.1f}%' if isinstance(taux_max, numbers.Number) else 'N/D'}")
    print(f"     • Ruptures de Série (abandons) : {stats['streaks_tombes_zero']}")
    print(f"     • Taux d'Abandon Global : {stats.get('taux_churn', 0):.1f}%")
    print(f"     • Reactivations vs veille : {stats.get('reactivations_veille', 0)}")

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
            "Taux Abonn. Max": round(stats["taux_conversion_max"] / 100, 6)
                if isinstance(stats.get("taux_conversion_max"), numbers.Number) else None,
            "Taux d'Abandon Global": round(stats.get("taux_churn", 0) / 100, 6),
            "Reactivations vs Veille": stats.get("reactivations_veille", 0),
            "Abandon Débutants": round(stats.get("cohortes", {}).get("Debutants", {}).get("churn", 0) / 100, 6),
            "Abandon Standard": round(stats.get("cohortes", {}).get("Standard", {}).get("churn", 0) / 100, 6),
            "Abandon Super-Actifs": round(stats.get("cohortes", {}).get("Super-Actifs", {}).get("churn", 0) / 100, 6),
            "Score d'Engagement": round(stats.get("score_sante_jour", 0) / 100, 6),
            "Panel Total": stats.get("nb_profils_jour"),
        }])

        df_resume = _charger_resume_historique()
        if df_resume is None or df_resume.empty:
            df_resume = df_stats.copy()
        else:
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

            df_glossaire = pd.DataFrame([
                {"KPI": "Moyenne Streak (J)", "Définition": "Longueur moyenne de la série de jours consécutifs d'utilisation. Mesure la fidélité à long terme."},
                {"KPI": "Apprentissage (XP/j)", "Définition": "Gain moyen de points d'expérience (XP) depuis hier. Mesure l'effort d'apprentissage quotidien."},
                {"KPI": "Taux Abonn. Super", "Définition": "Pourcentage d'utilisateurs possédant un abonnement 'Super Duolingo' (hasPlus) hors Duolingo Max."},
                {"KPI": "Taux Abonn. Max", "Définition": "Pourcentage d'utilisateurs possédant un abonnement 'Duolingo Max' (AI features)."},
                {"KPI": "Taux d'Abandon Global", "Définition": "Pourcentage d'utilisateurs actifs hier qui ne le sont plus aujourd'hui (streak retombe à 0)."},
                {"KPI": "Reactivations vs Veille", "Définition": "Nombre d'utilisateurs inactifs hier (streak à 0) redevenus actifs aujourd'hui."},
                {"KPI": "Abandon Débutants", "Définition": "Taux d'abandon spécifique aux utilisateurs ayant moins de 1000 XP au total."},
                {"KPI": "Abandon Standard", "Définition": "Taux d'abandon spécifique aux utilisateurs ayant entre 1000 et 5000 XP."},
                {"KPI": "Abandon Super-Actifs", "Définition": "Taux d'abandon spécifique à l'élite ayant plus de 5000 XP."},
                {"KPI": "Score Santé Global", "Définition": "Pourcentage des utilisateurs suivis qui ont un streak > 0 aujourd'hui."},
            ])
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

        DUO_GREEN = "58CC02"
        DUO_BLUE = "1CB0F6"
        NAVY = "1F4E78"
        LIGHT_GREY = "F2F2F2"
        GREEN_SOFT = "E2F0D9"
        AMBER_SOFT = "FFF2CC"
        RED_SOFT = "FFC7CE"
        WHITE = "FFFFFF"

        BASE_FONT_NAME = "Calibri"
        header_fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
        header_font = Font(name=BASE_FONT_NAME, color=WHITE, bold=True, size=11)
        zebra_fill = PatternFill(start_color=LIGHT_GREY, end_color=LIGHT_GREY, fill_type="solid")
        success_fill = PatternFill(start_color=GREEN_SOFT, end_color=GREEN_SOFT, fill_type="solid")
        warning_fill = PatternFill(start_color=AMBER_SOFT, end_color=AMBER_SOFT, fill_type="solid")
        alert_fill = PatternFill(start_color=RED_SOFT, end_color=RED_SOFT, fill_type="solid")
        white_fill = PatternFill(start_color=WHITE, end_color=WHITE, fill_type="solid")
        base_font = Font(name=BASE_FONT_NAME, size=11, color="000000")

        center_align = Alignment(horizontal="center", vertical="center")
        left_align = Alignment(horizontal="left", vertical="center", indent=1)

        thin_border = Border(
            left=Side(style="thin", color="DDDDDD"),
            right=Side(style="thin", color="DDDDDD"),
            top=Side(style="thin", color="DDDDDD"),
            bottom=Side(style="thin", color="DDDDDD"),
        )

        def render_financial_nowcast_sheet(ws, signal_package: dict) -> None:
            for merged_range in list(ws.merged_cells.ranges):
                ws.unmerge_cells(str(merged_range))
            if ws.max_row:
                ws.delete_rows(1, ws.max_row)

            ws.sheet_view.showGridLines = False
            for column_letter, width in {
                "A": 18,
                "B": 18,
                "C": 18,
                "D": 18,
                "E": 18,
                "F": 18,
                "G": 18,
                "H": 18,
            }.items():
                ws.column_dimensions[column_letter].width = width

            metadata = signal_package.get("metadata", {})
            panel = signal_package.get("panel", {})
            business = signal_package.get("business_signals", {})
            proxy = signal_package.get("financial_proxy_signals", {})
            assumptions = signal_package.get("assumptions", [])
            ai_sections = {
                "RESUME": None,
                "TENDANCES": None,
                "ATTENTION": None,
                "CONSEILS": None,
            }

            if ia_report:
                import re
                for key in ai_sections.keys():
                    match = re.search(rf"\[{key}\](.*?)(?=\[|$)", ia_report, re.DOTALL)
                    if match:
                        ai_sections[key] = match.group(1).strip()

            def write_box(range_ref: str, value: str, *, fill: str = WHITE, font_color: str = "000000",
                          size: int = 11, bold: bool = False, align: Alignment | None = None) -> None:
                ws.merge_cells(range_ref)
                cell = ws[range_ref.split(":")[0]]
                cell.value = value
                cell.fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")
                cell.font = Font(name=BASE_FONT_NAME, size=size, bold=bold, color=font_color)
                cell.alignment = align or center_align
                cell.border = thin_border

            def write_card(start_col: str, end_col: str, title_row: int, title: str, value: str, note: str,
                           accent: str, value_color: str = "000000") -> None:
                write_box(
                    f"{start_col}{title_row}:{end_col}{title_row}",
                    title,
                    fill=accent,
                    font_color=WHITE,
                    size=10,
                    bold=True,
                )
                write_box(
                    f"{start_col}{title_row + 1}:{end_col}{title_row + 2}",
                    value,
                    fill=WHITE,
                    font_color=value_color,
                    size=17,
                    bold=True,
                )
                write_box(
                    f"{start_col}{title_row + 3}:{end_col}{title_row + 3}",
                    note,
                    fill=LIGHT_GREY,
                    font_color="555555",
                    size=9,
                    align=Alignment(horizontal="center", vertical="center", wrap_text=True),
                )

            bias_label = _label_signal_bias(proxy.get("signal_bias"))
            confidence_label = _label_confidence(proxy.get("confidence_level"))
            coverage_ratio = panel.get("coverage_ratio")
            observed_users = panel.get("observed_users_today")
            target_users = panel.get("target_panel_size")
            summary_lines = [
                f"Date de reference : {metadata.get('as_of_date', 'N/D')}",
                f"Panel observe : {_pretty_fr_number(observed_users, 0)} / {_pretty_fr_number(target_users, 0)} "
                f"({_pretty_ratio_pct(coverage_ratio, 1)})",
                f"Signal global : {bias_label} | Confiance : {confidence_label}",
            ]

            drivers = proxy.get("main_drivers") or ["Aucun driver majeur identifie pour l'instant."]
            risks = proxy.get("main_risks") or ["Aucun risque majeur identifie pour l'instant."]
            summary_text = (
                f"Signal {bias_label.lower()} avec confiance {confidence_label.lower()}. "
                f"Monetisation {_pretty_score(proxy.get('monetization_momentum_index'))}, "
                f"engagement {_pretty_score(proxy.get('engagement_quality_index'))}, "
                f"premium 14j {_pretty_delta_pts(proxy.get('premium_momentum_14d'))}."
            )
            if ai_sections["RESUME"]:
                summary_text = ai_sections["RESUME"]
            summary_text = _compact_summary_text(summary_text, max_sentences=2, max_chars=170)

            bias_fill = {
                "Favorable": DUO_GREEN,
                "Neutre": "F4C542",
                "Defavorable": "FF6B6B",
            }.get(bias_label, NAVY)
            confidence_fill = {
                "Elevee": DUO_GREEN,
                "Moyenne": "F4C542",
                "Faible": "FF8A65",
            }.get(confidence_label, NAVY)

            write_box("A1:H2", "NOWCAST FINANCIER", fill=NAVY, font_color=WHITE, size=18, bold=True)
            write_box(
                "A3:H3",
                " | ".join(summary_lines),
                fill=LIGHT_GREY,
                font_color="333333",
                size=10,
                align=Alignment(horizontal="center", vertical="center"),
            )

            write_card("A", "B", 5, "Signal global", bias_label, "Lecture actuelle du signal", bias_fill)
            write_card("C", "D", 5, "Confiance", confidence_label, "Couverture + profondeur historique", confidence_fill)
            write_card(
                "E",
                "F",
                5,
                "Couverture panel",
                _pretty_ratio_pct(coverage_ratio, 1),
                "Part du panel observee aujourd'hui",
                DUO_BLUE,
            )
            write_card(
                "G",
                "H",
                5,
                "Panel observe",
                _pretty_fr_number(observed_users, 0),
                f"Cible : {_pretty_fr_number(target_users, 0)}",
                NAVY,
            )

            write_card(
                "A",
                "B",
                10,
                "Momentum monetisation",
                _pretty_score(proxy.get("monetization_momentum_index")),
                "Indice composite de monetisation",
                DUO_GREEN,
            )
            write_card(
                "C",
                "D",
                10,
                "Qualite engagement",
                _pretty_score(proxy.get("engagement_quality_index")),
                "Indice composite d'engagement",
                DUO_BLUE,
            )
            write_card(
                "E",
                "F",
                10,
                "Premium momentum 14j",
                _pretty_delta_pts(proxy.get("premium_momentum_14d")),
                "Variation recente du taux Super",
                NAVY,
            )
            churn_value = proxy.get("churn_trend_14d")
            churn_color = DUO_GREEN if isinstance(churn_value, numbers.Number) and churn_value <= 0 else "FF6B6B"
            write_card(
                "G",
                "H",
                10,
                "Churn trend 14j",
                _pretty_delta_pts(churn_value),
                "Variation recente du taux d'abandon",
                churn_color,
            )

            write_box("A15:H15", "Lecture investisseur", fill=NAVY, font_color=WHITE, size=11, bold=True)
            write_box(
                "A16:H18",
                summary_text,
                fill=WHITE,
                font_color="000000",
                size=11,
                align=Alignment(horizontal="left", vertical="top", wrap_text=True),
            )

            left_title = "Tendances IA" if ai_sections["TENDANCES"] else "Main Drivers"
            right_title = "Points d'attention" if ai_sections["ATTENTION"] else "Main Risks"
            left_body = _compact_bullet_text(
                ai_sections["TENDANCES"] or "\n".join(f"- {item}" for item in drivers),
                max_items=2,
                max_chars=90,
            )
            right_body = _compact_bullet_text(
                ai_sections["ATTENTION"] or "\n".join(f"- {item}" for item in risks),
                max_items=2,
                max_chars=90,
            )

            write_box("A20:D20", left_title, fill=DUO_GREEN, font_color=WHITE, size=11, bold=True)
            write_box("E20:H20", right_title, fill="FF6B6B", font_color=WHITE, size=11, bold=True)
            write_box(
                "A21:D24",
                left_body,
                fill=WHITE,
                font_color="000000",
                size=10,
                align=Alignment(horizontal="left", vertical="top", wrap_text=True),
            )
            write_box(
                "E21:H24",
                right_body,
                fill=WHITE,
                font_color="000000",
                size=10,
                align=Alignment(horizontal="left", vertical="top", wrap_text=True),
            )

            if ai_sections["CONSEILS"]:
                conclusion_text = _compact_summary_text(ai_sections["CONSEILS"], max_sentences=2, max_chars=160)
                write_box("A26:H26", "Conclusion IA", fill=DUO_BLUE, font_color=WHITE, size=11, bold=True)
                write_box(
                    "A27:H29",
                    conclusion_text,
                    fill=LIGHT_GREY,
                    font_color="000000",
                    size=10,
                    align=Alignment(horizontal="left", vertical="top", wrap_text=True),
                )
                model_header_row = 31
                model_start_row = 32
            else:
                model_header_row = 26
                model_start_row = 27

            write_box(f"A{model_header_row}:H{model_header_row}", "Etat du modele", fill=NAVY, font_color=WHITE, size=11, bold=True)
            model_rows = [
                ("Revenue Beat Probability", "N/D (Phase 2)"),
                ("EBITDA Beat Probability", "N/D (Phase 2)"),
                ("Guidance Raise Probability", "N/D (Phase 2)"),
                ("Reactivation trend 7j", _pretty_delta_pts(proxy.get("reactivation_trend_7d"))),
                ("High-value retention", _pretty_delta_pts(proxy.get("high_value_retention_trend"))),
                ("Super rate", _pretty_ratio_pct(business.get("super_rate"), 1)),
                ("Max rate", _pretty_ratio_pct(business.get("max_rate"), 1)),
                (
                    "Model readiness",
                    "Pret pour proxys explicables, labels financiers a brancher pour les probabilites.",
                ),
            ]
            row_cursor = model_start_row
            for label, value in model_rows:
                row_fill = zebra_fill if row_cursor % 2 == 0 else white_fill
                ws[f"A{row_cursor}"] = label
                ws[f"A{row_cursor}"].fill = row_fill
                ws[f"A{row_cursor}"].font = Font(name=BASE_FONT_NAME, size=10, bold=True)
                ws[f"A{row_cursor}"].alignment = left_align
                ws[f"A{row_cursor}"].border = thin_border
                ws.merge_cells(f"B{row_cursor}:H{row_cursor}")
                value_cell = ws[f"B{row_cursor}"]
                value_cell.value = value
                value_cell.fill = row_fill
                value_cell.font = Font(name=BASE_FONT_NAME, size=10)
                value_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                value_cell.border = thin_border
                for merged_col in range(3, 9):
                    ws.cell(row=row_cursor, column=merged_col).border = thin_border
                    ws.cell(row=row_cursor, column=merged_col).fill = row_fill
                row_cursor += 1

            hypotheses_row = row_cursor + 1
            write_box(f"A{hypotheses_row}:H{hypotheses_row}", "Hypotheses & notes", fill=NAVY, font_color=WHITE, size=11, bold=True)
            assumptions_text = "\n".join(f"- {item}" for item in assumptions) if assumptions else "- Aucune hypothese specifique."
            write_box(
                f"A{hypotheses_row + 1}:H{hypotheses_row + 4}",
                assumptions_text,
                fill=LIGHT_GREY,
                font_color="333333",
                size=10,
                align=Alignment(horizontal="left", vertical="top", wrap_text=True),
            )

            for row_idx in [6, 11]:
                ws.row_dimensions[row_idx].height = 28
                ws.row_dimensions[row_idx + 1].height = 26
                ws.row_dimensions[row_idx + 2].height = 22
            for row_idx in range(16, 19):
                ws.row_dimensions[row_idx].height = 34
            for row_idx in range(21, 25):
                ws.row_dimensions[row_idx].height = 30
            if ai_sections["CONSEILS"]:
                for row_idx in range(27, 30):
                    ws.row_dimensions[row_idx].height = 28
            for row_idx in range(hypotheses_row + 1, hypotheses_row + 5):
                ws.row_dimensions[row_idx].height = 22

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
                render_financial_nowcast_sheet(ws, financial_signals)
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
            TRENDS_SHEET,
            GLOSSAIRE_SHEET,
            SIGNALS_RAW_SHEET,
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
        if CHART_DATA_SHEET in wb.sheetnames:
            wb[CHART_DATA_SHEET].sheet_state = "hidden"
        ordered_sheet_names = [
            SUMMARY_SHEET,
            SIGNALS_SHEET,
            TRENDS_SHEET,
            GLOSSAIRE_SHEET,
            SIGNALS_RAW_SHEET,
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
