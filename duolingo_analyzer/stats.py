"""
Partie 2 : Statistiques
Calcule les statistiques d'engagement via Pandas.
"""
import csv
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import numbers
import pandas as pd

from .config import DAILY_LOG_FILE, GOOGLE_DRIVE_REPORT_DIR, RAPPORT_EXCEL_FILE, REPORT_DIR
from .excel_dashboard import refresh_trends_dashboard

SUMMARY_SHEET = "ðŸ“Š RÃ©sumÃ© Financier Q1"
AI_SHEET = "ðŸ¤– Analyse StratÃ©gique"
GLOSSAIRE_SHEET = "ðŸ“– Dictionnaire des KPIs"
SUMMARY_COLUMNS = [
    "Date",
    "SÃ©rie Moyenne (Jours)",
    "Ã‰vol. vs Veille",
    "Apprentissage (XP/j)",
    "Taux Abonn. Super",
    "Taux Abonn. Max",
    "Taux d'Abandon Global",
    "Abandon DÃ©butants",
    "Abandon Standard",
    "Abandon Super-Actifs",
    "Score d'Engagement",
    "Panel Total",
]
SUMMARY_COLUMN_ALIASES = {
    "Date": "Date",
    "Moyenne Streak (J)": "SÃ©rie Moyenne (Jours)",
    "SÃ©rie Moyenne (Jours)": "SÃ©rie Moyenne (Jours)",
    "Ã‰volution vs Hier": "Ã‰vol. vs Veille",
    "Ã‰vol. vs Veille": "Ã‰vol. vs Veille",
    "Delta XP (IntensitÃ©)": "Apprentissage (XP/j)",
    "Apprentissage (XP/j)": "Apprentissage (XP/j)",
    "Conversion Premium": "Taux Abonn. Super",
    "Taux Abonn. Super": "Taux Abonn. Super",
    "Taux Abonn. Max": "Taux Abonn. Max",
    "Churn Global": "Taux d'Abandon Global",
    "Taux d'Abandon Global": "Taux d'Abandon Global",
    "Taux d'Attrition Global": "Taux d'Abandon Global",
    "Churn DÃ©butants": "Abandon DÃ©butants",
    "Abandon DÃ©butants": "Abandon DÃ©butants",
    "Churn Standard": "Abandon Standard",
    "Churn Standard ": "Abandon Standard",
    "Abandon Standard": "Abandon Standard",
    "Abandon Standard ": "Abandon Standard",
    "Churn Super-Actifs": "Abandon Super-Actifs",
    "Abandon Super-Actifs": "Abandon Super-Actifs",
    "Score SantÃ© Global": "Score d'Engagement",
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
        cleaned = cleaned.replace("âˆ’", "-")
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
    # 1) Si le fichier historique existe, on l'utilise
    if RAPPORT_EXCEL_FILE.exists():
        try:
            df_resume = pd.read_excel(RAPPORT_EXCEL_FILE, sheet_name=SUMMARY_SHEET)
            if df_resume is not None and not df_resume.empty:
                return _normalize_summary_df(df_resume)
        except Exception:
            pass

    # 2) Sinon, on tente de reconstruire l'historique Ã  partir des anciens rapports journaliers
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
        print(f"  âš ï¸ Impossible de crÃ©er le dossier Google Drive : {destination_dir} ({exc})")
        return None

    destination_file = destination_dir / report_path.name
    try:
        shutil.copy2(report_path, destination_file)
    except Exception as exc:
        print(f"  âš ï¸ Copie Google Drive impossible : {destination_file} ({exc})")
        return None

    return destination_file


def calculer_statistiques() -> dict | None:
    """
    PARTIE 2 : Calcule les statistiques d'engagement pour aujourd'hui
    et extrait les tendances par rapport Ã  la veille.
    """
    print("============================================================")
    print("  PARTIE 2 â€” CALCUL DES STATISTIQUES")
    print("============================================================")

    if not DAILY_LOG_FILE.exists():
        print("  âŒ Aucun fichier M de donnÃ©es trouvÃ©.")
        return None

    try:
        df = _load_daily_log_df()
    except Exception as e:
        print(f"  âŒ Erreur de lecture du CSV : {e}")
        return None

    if df.empty:
        print("  âš ï¸  Le fichier est vide.")
        return None

    # --- NETTOYAGE DES DONNÃ‰ES ---
    # On ignore les lignes d'agrÃ©gation "Aggregated" qui peuvent polluer les calculs bruts
    df = df[~df["Username"].str.contains("Aggregated", na=False)]
    df = df[df["Cohort"] != "Global"]

    aujourdhui = datetime.now().strftime("%Y-%m-%d")
    hier = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    df_jour = df[df["Date"] == aujourdhui]
    df_hier = df[df["Date"] == hier]

    stats = {
        "date_jour": aujourdhui,
        "date_hier": hier,
        "moyenne_streak_jour": 0.0,
        "moyenne_streak_hier": None,
        "utilisateurs_actifs": 0,
        "streaks_tombes_zero": 0,
        "nb_profils_jour": len(df_jour),
        "nb_profils_hier": len(df_hier),
        "taux_conversion_plus": 0.0,
        "taux_conversion_max": None,
        "delta_xp_moyen": 0.0,
        "cohortes": {
            "Debutants": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0},
            "Standard": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0},
            "Super-Actifs": {"actifs": 0, "total": 0, "retention": 0.0, "churn": 0.0, "tombes_zero": 0}
        }
    }

    if not df_jour.empty:
        stats["moyenne_streak_jour"] = df_jour["Streak"].mean()
        stats["utilisateurs_actifs"] = len(df_jour[df_jour["Streak"] > 0])
        stats["score_sante_jour"] = (stats["utilisateurs_actifs"] / stats["nb_profils_jour"]) * 100 if stats["nb_profils_jour"] > 0 else 0
        
        # Taux de Conversion Super & Max (MonÃ©tisation)
        if "HasPlus" in df_jour.columns:
            abonnes_plus = len(df_jour[df_jour["HasPlus"] == True])
            stats["taux_conversion_plus"] = (abonnes_plus / stats["nb_profils_jour"]) * 100 if stats["nb_profils_jour"] > 0 else 0
        
        if "HasMax" in df_jour.columns:
            abonnes_max = len(df_jour[df_jour["HasMax"] == True])
            if abonnes_max > 0:
                stats["taux_conversion_max"] = (abonnes_max / stats["nb_profils_jour"]) * 100 if stats["nb_profils_jour"] > 0 else 0
            
        # Remplissage initial des cohortes du jour
        if "Cohort" in df_jour.columns:
            for cohorte in stats["cohortes"].keys():
                df_c = df_jour[df_jour["Cohort"] == cohorte]
                stats["cohortes"][cohorte]["total"] = len(df_c)
                stats["cohortes"][cohorte]["actifs"] = len(df_c[df_c["Streak"] > 0])

    if not df_hier.empty and not df_jour.empty:
        stats["moyenne_streak_hier"] = df_hier["Streak"].mean()
        
        merged = df_hier.merge(
            df_jour, 
            on="Username", 
            suffixes=("_hier", "_jour")
        )
        
        # Calcul du Delta XP (IntensitÃ©)
        if "TotalXP_hier" in merged.columns and "TotalXP_jour" in merged.columns:
            merged["Delta_XP"] = merged["TotalXP_jour"] - merged["TotalXP_hier"]
            merged.loc[merged["Delta_XP"] < 0, "Delta_XP"] = 0 # Ignorer les bugs d'API oÃ¹ l'XP baisse
            stats["delta_xp_moyen"] = merged["Delta_XP"].mean()
        
        tombes_a_zero = merged[
            (merged["Streak_hier"] > 0) & (merged["Streak_jour"] == 0)
        ]
        stats["streaks_tombes_zero"] = len(tombes_a_zero)
        
        actifs_hier = len(df_hier[df_hier["Streak"] > 0])
        stats["taux_retention"] = ((actifs_hier - stats["streaks_tombes_zero"]) / actifs_hier * 100) if actifs_hier > 0 else 0
        stats["taux_churn"] = (stats["streaks_tombes_zero"] / actifs_hier * 100) if actifs_hier > 0 else 0
        
        # Calcul Churn/RÃ©tention par Cohorte
        if "Cohort_jour" in merged.columns:
            for cohorte in stats["cohortes"].keys():
                merged_c = merged[merged["Cohort_jour"] == cohorte]
                tombes_c = merged_c[(merged_c["Streak_hier"] > 0) & (merged_c["Streak_jour"] == 0)]
                actifs_hier_c = len(df_hier[(df_hier["Cohort"] == cohorte) & (df_hier["Streak"] > 0)]) if "Cohort" in df_hier.columns else 0
                
                stats["cohortes"][cohorte]["tombes_zero"] = len(tombes_c)
                if actifs_hier_c > 0:
                    stats["cohortes"][cohorte]["churn"] = (len(tombes_c) / actifs_hier_c) * 100
                    stats["cohortes"][cohorte]["retention"] = ((actifs_hier_c - len(tombes_c)) / actifs_hier_c) * 100
    else:
        print("  âš ï¸  Aucune donnÃ©e pour hier â€” comparaison impossible.\n")
        stats["taux_retention"] = 0
        stats["taux_churn"] = 0

    print(f"  ðŸ“Š Statistiques du {aujourdhui} :")
    print(f"     â€¢ SÃ©rie Moyenne (Streak) : {stats['moyenne_streak_jour']:.1f} j")
    if stats["moyenne_streak_hier"] is not None:
        evolution = stats["moyenne_streak_jour"] - stats["moyenne_streak_hier"]
        signe = "+" if evolution > 0 else ""
        print(f"     â€¢ Ã‰volution de la sÃ©rie : {signe}{evolution:.1f} j vs hier")
        print(f"     â€¢ IntensitÃ© d'Apprentissage : +{stats['delta_xp_moyen']:.0f} XP/jour")
    
    print(f"     â€¢ Utilisateurs Actifs : {stats['utilisateurs_actifs']}")
    print(f"     â€¢ PÃ©nÃ©tration Super Duolingo : {stats['taux_conversion_plus']:.1f}%")
    taux_max = stats.get("taux_conversion_max")
    print(f"     â€¢ PÃ©nÃ©tration Duolingo Max    : {f'{taux_max:.1f}%' if isinstance(taux_max, numbers.Number) else 'N/D'}")
    print(f"     â€¢ Ruptures de SÃ©rie (abandons) : {stats['streaks_tombes_zero']}")
    print(f"     â€¢ Taux d'Abandon Global : {stats.get('taux_churn', 0):.1f}%")
    
    print(f"\n     â€¢ Analyse par Segment (Cohorte) :")
    for nom, donnees in stats["cohortes"].items():
        print(f"       - {nom:12s} : {donnees['actifs']}/{donnees['total']} actifs | Taux d'abandon: {donnees['churn']:.1f}%")
        
    print(f"\n     â€¢ Profils collectÃ©s aujourd'hui : {stats['nb_profils_jour']}\n")

    return stats


def sauvegarder_rapport_excel(stats: dict, ia_report: str = None) -> None:
    """
    Exporte les donnÃ©es d'engagement et les statistiques dans un fichier Excel (.xlsx) 
    avec un design premium et l'analyse de l'IA.
    """
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.chart import LineChart, Reference
    from openpyxl.chart.layout import Layout, ManualLayout
    from openpyxl.chart.label import DataLabelList

    print(f"  ðŸ“‚ GÃ©nÃ©ration du rapport Excel Premium...")
    
    if not DAILY_LOG_FILE.exists():
        return

    try:
        # Lire les donnÃ©es brutes
        df = _load_daily_log_df()
        aujourdhui = stats.get("date_jour")
        df_jour = df[df["Date"] == aujourdhui] if aujourdhui else pd.DataFrame()

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

        # CrÃ©er la ligne du jour
        df_stats = pd.DataFrame([{
                "Date": date_obj or date_jour,
                "SÃ©rie Moyenne (Jours)": moyenne_streak_jour,
                "Ã‰vol. vs Veille": delta_streak,
                "Apprentissage (XP/j)": round(stats.get('delta_xp_moyen', 0), 0),
                "Taux Abonn. Super": round(stats.get('taux_conversion_plus', 0), 1),
                "Taux Abonn. Max": round(stats["taux_conversion_max"], 1) if isinstance(stats.get("taux_conversion_max"), numbers.Number) else None,
                "Taux d'Abandon Global": round(stats.get('taux_churn', 0), 2),
                "Abandon DÃ©butants": round(stats.get('cohortes', {}).get('Debutants', {}).get('churn', 0), 1),
                "Abandon Standard": round(stats.get('cohortes', {}).get('Standard', {}).get('churn', 0), 1),
                "Abandon Super-Actifs": round(stats.get('cohortes', {}).get('Super-Actifs', {}).get('churn', 0), 1),
                "Score d'Engagement": round(stats.get('score_sante_jour', 0), 1),
                "Panel Total": stats.get("nb_profils_jour")
            }])

        # Charger l'historique existant et y ajouter la ligne du jour
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

        # CrÃ©er un ExcelWriter
        with pd.ExcelWriter(RAPPORT_EXCEL_FILE, engine='openpyxl', mode=writer_mode, **writer_kwargs) as writer:
            # 1. RÃ‰SUMÃ‰ STATISTIQUES (Sheet 1)
            df_resume.to_excel(writer, sheet_name=SUMMARY_SHEET, index=False)
            
            # 2. ANALYSE IA (Sheet 2) - On crÃ©e juste la feuille, on la remplira aprÃ¨s
            if ia_report:
                pd.DataFrame().to_excel(writer, sheet_name=AI_SHEET, index=False)

            # 3. GLOSSAIRE DES KPIs (Sheet 3)
            df_glossaire = pd.DataFrame([
                {"KPI": "Moyenne Streak (J)", "DÃ©finition": "Longueur moyenne de la sÃ©rie de jours consÃ©cutifs d'utilisation. Mesure la fidÃ©litÃ© Ã  long terme."},
                {"KPI": "Apprentissage (XP/j)", "DÃ©finition": "Gain moyen de points d'expÃ©rience (XP) depuis hier. Mesure l'effort d'apprentissage quotidien."},
                {"KPI": "Taux Abonn. Super", "DÃ©finition": "Pourcentage d'utilisateurs possÃ©dant un abonnement 'Super Duolingo' (hasPlus)."},
                {"KPI": "Taux Abonn. Max", "DÃ©finition": "Pourcentage d'utilisateurs possÃ©dant un abonnement 'Duolingo Max' (AI features)."},
                {"KPI": "Taux d'Abandon Global", "DÃ©finition": "Pourcentage d'utilisateurs actifs hier qui ne le sont plus aujourd'hui (streak retombe a 0)."},
                {"KPI": "Abandon DÃ©butants", "DÃ©finition": "Taux d'abandon spÃ©cifique aux utilisateurs ayant moins de 1000 XP au total."},
                {"KPI": "Abandon Standard", "DÃ©finition": "Taux d'abandon spÃ©cifique aux utilisateurs ayant entre 1000 et 5000 XP."},
                {"KPI": "Abandon Super-Actifs", "DÃ©finition": "Taux d'abandon spÃ©cifique Ã  l'Ã©lite ayant plus de 5000 XP."},
                {"KPI": "Score SantÃ© Global", "DÃ©finition": "Pourcentage des utilisateurs suivis qui ont un streak > 0 aujourd'hui."}
            ])
            df_glossaire.to_excel(writer, sheet_name=GLOSSAIRE_SHEET, index=False)

        # â”€â”€â”€ Post-Traitement : Styles Premium avec Openpyxl â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        from openpyxl import load_workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

        # --- CALCUL DES TENDANCES MENSUELLES ---
        try:
            # CrÃ©er une copie pour le calcul mensuel sans modifier le df global si besoin
            df_cleaned = df[~df["Username"].str.contains("Aggregated", na=False)].copy()
            df_cleaned = df_cleaned[df_cleaned["Cohort"] != "Global"]

            df_cleaned['Month'] = pd.to_datetime(df_cleaned['Date']).dt.to_period('M').astype(str)
            # Calculer le Delta XP quotidien d'abord pour pouvoir l'agrÃ©ger
            df_sorted = df_cleaned.sort_values(['Username', 'Date'])
            df_sorted['Prev_XP'] = df_sorted.groupby('Username')['TotalXP'].shift(1)
            df_sorted['Daily_Delta'] = df_sorted['TotalXP'] - df_sorted['Prev_XP']
            df_sorted.loc[df_sorted['Daily_Delta'] < 0, 'Daily_Delta'] = 0
            
            # 1. Calculer les moyennes par Cohorte
            monthly_cohorts = df_sorted.groupby(['Month', 'Cohorte' if 'Cohorte' in df_sorted.columns else 'Cohort']).agg({
                'Streak': 'mean',
                'HasPlus': 'mean',
                'Daily_Delta': 'mean'
            }).reset_index()
            monthly_cohorts.columns = ['Mois', 'Cohorte', 'SÃ©rie Moy. (j)', 'Taux Super (%)', 'ActivitÃ© (XP/j)']
            
            # 2. Calculer le poids 'Global' (vrai moyenne pondÃ©rÃ©e)
            monthly_global = df_sorted.groupby(['Month']).agg({
                'Streak': 'mean',
                'HasPlus': 'mean',
                'Daily_Delta': 'mean'
            }).reset_index()
            monthly_global['Cohorte'] = 'Global'
            monthly_global = monthly_global[['Month', 'Cohorte', 'Streak', 'HasPlus', 'Daily_Delta']]
            monthly_global.columns = ['Mois', 'Cohorte', 'SÃ©rie Moy. (j)', 'Taux Super (%)', 'ActivitÃ© (XP/j)']
            
            # Fusionner les deux
            monthly_stats = pd.concat([monthly_cohorts, monthly_global], ignore_index=True)
            monthly_stats = monthly_stats.sort_values(['Cohorte', 'Mois'])
            
            # 3. Calculer les Deltas MoM
            for col in ['SÃ©rie Moy. (j)', 'Taux Super (%)', 'ActivitÃ© (XP/j)']:
                new_col = f"Î” {col} (MoM)"
                monthly_stats[new_col] = monthly_stats.groupby('Cohorte')[col].diff()
            
            monthly_stats = monthly_stats.sort_values(['Mois', 'Cohorte'])
            monthly_stats['Taux Super (%)'] *= 100
            monthly_stats['Î” Taux Super (%) (MoM)'] *= 100
        except Exception as e:
            print(f"  âš ï¸ Erreur calcul mensuel : {e}")
            monthly_stats = pd.DataFrame()

        with pd.ExcelWriter(RAPPORT_EXCEL_FILE, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            if not monthly_stats.empty:
                monthly_stats.to_excel(writer, sheet_name="ðŸ“ˆ Tendances Mensuelles", index=False)

            # --- 4. DATA FOR CHART (Daily Premium Conversion) ---
            try:
                # Filtrage : On ne garde que les dates avec > 1000 utilisateurs pour Ã©viter le bruit
                date_counts = df.groupby('Date').size()
                valid_dates = date_counts[date_counts > 1000].index
                df_filtered = df[df['Date'].isin(valid_dates)]

                if df_filtered.empty:
                    # Si aucune date n'a > 50 users (ex: dÃ©but de projet), on prend tout quand mÃªme
                    df_filtered = df

                # Calcul conversion journaliÃ¨re par cohorte
                df_daily_conv = df_filtered.groupby(['Date', 'Cohort'])['HasPlus'].mean().unstack() * 100
                df_daily_conv['Global'] = df_filtered.groupby('Date')['HasPlus'].mean() * 100
                df_daily_conv = df_daily_conv.reset_index()
                numeric_cols = df_daily_conv.select_dtypes(include='number').columns
                df_daily_conv[numeric_cols] = df_daily_conv[numeric_cols].round(1)
                
                # S'assurer que toutes les cohortes sont lÃ  pour Ã©viter les erreurs de colonne
                for c in ["Debutants", "Standard", "Super-Actifs"]:
                    if c not in df_daily_conv.columns:
                        df_daily_conv[c] = 0.0
                
                # RÃ©organiser et renommer pour une lÃ©gende claire
                cols_map = {
                    'Date': 'Date',
                    'Global': 'Moyenne Panel Global',
                    'Debutants': 'Cohorte DÃ©butants (<1k XP)',
                    'Standard': 'Cohorte Standard (1k-5k XP)',
                    'Super-Actifs': 'Cohorte Super-Actifs (>5k XP)'
                }
                df_daily_conv = df_daily_conv[list(cols_map.keys())].rename(columns=cols_map)
                df_daily_conv['Date'] = pd.to_datetime(df_daily_conv['Date'], errors='coerce')
                
                df_daily_conv.to_excel(writer, sheet_name="ðŸ“Š DonnÃ©es Graphique", index=False)
            except Exception as e:
                print(f"  âš ï¸ Erreur prÃ©paration donnÃ©es graphique : {e}")

        wb = load_workbook(RAPPORT_EXCEL_FILE)
        
        # --- Palette de Couleurs ---
        DUO_GREEN = "58CC02"    # Vert Duolingo
        DUO_BLUE  = "1CB0F6"    # Bleu Duolingo
        NAVY      = "1F4E78"    # Bleu Marine Pro
        LIGHT_GREY= "F2F2F2"    # ZÃ©brures
        RED_SOFT  = "FFC7CE"    # Alerte
        WHITE     = "FFFFFF"
        
        # --- Styles de Base ---
        BASE_FONT_NAME = "Calibri"
        header_fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
        header_font = Font(name=BASE_FONT_NAME, color=WHITE, bold=True, size=11)
        zebra_fill  = PatternFill(start_color=LIGHT_GREY, end_color=LIGHT_GREY, fill_type="solid")
        alert_fill  = PatternFill(start_color=RED_SOFT, end_color=RED_SOFT, fill_type="solid")
        base_font   = Font(name=BASE_FONT_NAME, size=11, color="000000")
        
        center_align = Alignment(horizontal="center", vertical="center")
        left_align   = Alignment(horizontal="left", vertical="center", indent=1)
        
        thin_border = Border(
            left=Side(style='thin', color="DDDDDD"), 
            right=Side(style='thin', color="DDDDDD"), 
            top=Side(style='thin', color="DDDDDD"), 
            bottom=Side(style='thin', color="DDDDDD")
        )

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            ws.sheet_view.showGridLines = False
            
            # --- 1. SPECIAL : ANALYSE IA (HORIZONTAL DASHBOARD) ---
            if "Analyse StratÃ©gique" in sheet_name:
                # Parsing des sections de l'IA
                sections = {
                    "TITRE": "Tableau de Bord StratÃ©gique",
                    "RESUME": "Pas d'analyse disponible.",
                    "TENDANCES": "-",
                    "ATTENTION": "-",
                    "CONSEILS": "-"
                }
                if ia_report:
                    import re
                    for key in sections.keys():
                        match = re.search(f"\\[{key}\\](.*?)(?=\\[|$)", ia_report, re.DOTALL)
                        if match:
                            sections[key] = match.group(1).strip()

                # Configuration de la page (3 colonnes larges)
                ws.column_dimensions['A'].width = 45
                ws.column_dimensions['B'].width = 45
                ws.column_dimensions['C'].width = 45
                
                # --- Titre Principal (Banner) ---
                ws.merge_cells('A1:C2')
                cell_title = ws['A1']
                cell_title.value = f"ðŸ¦‰ {sections['TITRE']}"
                cell_title.font = Font(name=BASE_FONT_NAME, size=18, bold=True, color=WHITE)
                cell_title.alignment = center_align
                cell_title.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")

                # --- Row 4: Les En-tÃªtes des 3 Colonnes ---
                headers = [
                    ("A4", "ðŸ“ RÃ‰SUMÃ‰ EXÃ‰CUTIF", DUO_BLUE),
                    ("B4", "ðŸ“ˆ TENDANCES CLÃ‰S", NAVY),
                    ("C4", "âš ï¸ POINTS D'ATTENTION", RED_SOFT)
                ]
                for cell_ref, title, color in headers:
                    cell = ws[cell_ref]
                    cell.value = title
                    cell.font = Font(name=BASE_FONT_NAME, size=11, bold=True, color=WHITE if color != RED_SOFT else "000000")
                    cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                    cell.alignment = center_align
                    cell.border = thin_border

                # --- Row 5: Le Contenu (Horizontal) ---
                contents = [
                    ("A5", sections['RESUME']),
                    ("B5", sections['TENDANCES']),
                    ("C5", sections['ATTENTION'])
                ]
                # On ajuste la hauteur de la ligne 5 pour que tout soit visible
                ws.row_dimensions[5].height = 200 # Valeur gÃ©nÃ©reuse pour Ã©viter la coupe
                
                for cell_ref, value in contents:
                    cell = ws[cell_ref]
                    cell.value = value
                    cell.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                    cell.font = Font(name=BASE_FONT_NAME, size=10)
                    cell.border = thin_border
                    # Fond blanc pour contraster avec le fond gris auto du reste
                    cell.fill = PatternFill(start_color=WHITE, end_color=WHITE, fill_type="solid")

                # --- Row 7: Recommandations (Largeur Totale) ---
                ws.merge_cells('A7:C7')
                ws['A7'].value = "ðŸ’¡ RECOMMANDATIONS STRATÃ‰GIQUES ET ACTIONS CONCRÃˆTES"
                ws['A7'].font = Font(name=BASE_FONT_NAME, size=11, bold=True, color=WHITE)
                ws['A7'].fill = PatternFill(start_color=DUO_GREEN, end_color=DUO_GREEN, fill_type="solid")
                ws['A7'].alignment = center_align
                ws['A7'].border = thin_border
                
                ws.merge_cells('A8:C12')
                ws['A8'].value = sections['CONSEILS']
                ws['A8'].alignment = Alignment(wrap_text=True, vertical="top", indent=2)
                ws['A8'].font = Font(name=BASE_FONT_NAME, size=11, bold=True)
                ws['A8'].border = thin_border
                ws.row_dimensions[8].height = 120

                # Masquer le ruban de grille pour un look plus "App"
                ws.sheet_view.showGridLines = False
                continue

            # --- 3. SPECIAL : GRAPHIQUE DE CONVERSION (Sheet Tendances) ---
            if "Tendances Mensuelles" in sheet_name:
                try:
                    data_ws = wb["ðŸ“Š DonnÃ©es Graphique"]
                    chart = LineChart()
                    chart.title = "Tendances de MonÃ©tisation : Taux d'Abonnement Super par Cohorte"
                    chart.style = 13
                    chart.y_axis.title = '\nTaux de Conversion (%)'
                    chart.x_axis.title = '\nCalendrier des RelevÃ©s'
                    chart.height = 14
                    chart.width = 25
                    
                    # Force l'affichage des Ã©tiquettes d'axes (Dates et Pourcentages)
                    chart.x_axis.delete = False
                    chart.y_axis.delete = False
                    chart.x_axis.tickLblPos = 'low'
                    chart.y_axis.tickLblPos = 'low'
                    chart.x_axis.majorTickMark = 'out'
                    chart.y_axis.majorTickMark = 'out'
                    
                    # --- Ajustement Manuel de la Zone de TraÃ§age (Margins+) ---
                    # On rÃ©duit encore plus la zone de traÃ§age pour laisser de la place aux titres
                    chart.plot_area.layout = Layout(
                        manualLayout=ManualLayout(
                            x=0.2, y=0.05,    # Plus de marge Ã  gauche (x=0.2)
                            h=0.7, w=0.6,     # Plus de marge en bas (h+y=0.75) et a droite (w+x=0.8)
                            xMode='edge', yMode='edge'
                        )
                    )
                    
                    # RÃ©fÃ©rences aux donnÃ©es (Global, Debutants, Standard, Super-Actifs)
                    # Colonnes B (2) Ã  E (5) dans DonnÃ©es Graphique
                    max_row = data_ws.max_row
                    dates = Reference(data_ws, min_col=1, min_row=2, max_row=max_row)
                    
                    for i in range(2, 6): # B, C, D, E
                        values = Reference(data_ws, min_col=i, min_row=1, max_row=max_row)
                        chart.add_data(values, titles_from_data=True)
                    
                    chart.set_categories(dates)
                    
                    # --- LÃ©gende Ã  droite pour plus de clartÃ© ---
                    chart.legend.position = 'r'
                    
                    # --- Personnalisation AvancÃ©e ---
                    colors = ["1CB0F6", "FF4B4B", "FFC800", "58CC02"] # Bleu, Rouge, Or, Vert
                    for i, s in enumerate(chart.series):
                        # Couleur de la ligne
                        s.graphicalProperties.line.solidFill = colors[i % len(colors)]
                        s.graphicalProperties.line.width = 25000 if i == 0 else 15000 # Global plus Ã©pais
                        
                        # Ajout de marqueurs (cercles) - On les garde car c'est pro
                        s.marker.symbol = "circle"
                        s.marker.size = 5
                        s.marker.graphicalProperties.solidFill = colors[i % len(colors)]
                        s.marker.graphicalProperties.line.solidFill = colors[i % len(colors)]
                        
                        # On supprime les Ã©tiquettes de donnÃ©es (dLbls) qui polluent le graph

                    ws.add_chart(chart, "J2")
                    
                    # Masquer la feuille de donnÃ©es
                    data_ws.sheet_state = 'hidden'
                except Exception as e:
                    print(f"  âš ï¸ Erreur ajout graphique : {e}")

            # --- 2. TABLES DE DONNÃ‰ES (Statistiques et Tendances) ---
            # Headers
            ws.row_dimensions[1].height = 22
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = center_align
                cell.border = thin_border

            # Lignes de donnÃ©es
            for row_idx, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row), start=2):
                is_zebra = row_idx % 2 == 0
                for cell in row:
                    cell.border = thin_border
                    header_value = ws.cell(1, cell.column).value
                    header_key = str(header_value).strip().lower() if header_value is not None else ""
                    is_delta = ("Î”" in str(header_value)) or ("ÃŽâ€" in str(header_value)) or ("delta" in header_key) or ("evol" in header_key)
                    is_percent = ("%" in str(header_value)) or any(k in header_key for k in ["taux", "attrition", "abandon", "score", "pÃ©nÃ©tration", "penetration"])
                    is_xp = "xp" in header_key
                    is_streak = ("sÃ©rie" in header_key) or ("serie" in header_key) or ("streak" in header_key)
                    is_panel = any(k in header_key for k in ["panel", "total", "profils", "actifs"])

                    # Nettoyer les NaN
                    if isinstance(cell.value, numbers.Number) and cell.value != cell.value:
                        cell.value = None

                    # Alignement intelligent
                    if isinstance(cell.value, (datetime, )):
                        cell.alignment = center_align
                        cell.font = base_font
                        cell.number_format = 'yyyy-mm-dd'
                    elif isinstance(cell.value, numbers.Number):
                        cell.alignment = center_align
                        cell.font = base_font
                        # Formatage numÃ©rique par type de KPI
                        if is_delta and is_percent:
                            cell.number_format = '+0.0"%" ;-0.0"%" ;0.0"%"'
                        elif is_percent:
                            cell.number_format = '0.0"%"'
                        elif is_delta and is_xp:
                            cell.number_format = '+#,##0" XP";-#,##0" XP";0" XP"'
                        elif is_xp:
                            cell.number_format = '#,##0" XP"'
                        elif is_delta:
                            cell.number_format = '+0.0;-0.0;0.0'
                        elif is_streak:
                            cell.number_format = '0.0'
                        elif is_panel:
                            cell.number_format = '#,##0'

                        # Accentuer les deltas
                        if is_delta:
                            if cell.value > 0:
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="008000", bold=True) # Vert
                            elif cell.value < 0:
                                cell.font = Font(name=BASE_FONT_NAME, size=11, color="C00000", bold=True) # Rouge
                    elif cell.value is None and is_delta:
                        cell.value = "N/A"
                        cell.alignment = center_align
                        cell.font = base_font
                    else:
                        cell.alignment = left_align
                        cell.font = base_font
                        if "dÃ©finition" in header_key or "definition" in header_key:
                            cell.alignment = Alignment(wrap_text=True, vertical="top", indent=1)
                    
                    # ZÃ©brures
                    if is_zebra:
                        cell.fill = zebra_fill
                    
                    # Mise en forme conditionnelle spÃ©cifique (attrition/churn)
                    if "RÃ©sumÃ© Financier" in sheet_name and any(k in header_key for k in ["attrition", "abandon", "churn"]):
                        try:
                            if float(cell.value) > 0:
                                cell.fill = alert_fill
                        except: pass

            # Auto-ajustement des colonnes
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except: pass
                
                adjusted_width = (max_length + 4)
                max_width = 70 if "Dictionnaire" in sheet_name else 60
                ws.column_dimensions[column].width = min(adjusted_width, max_width)

            # Figer l'en-tÃªte et activer les filtres sur les tableaux de donnÃ©es
            if ws.max_row > 1:
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions

        wb.save(RAPPORT_EXCEL_FILE)
        refresh_trends_dashboard(RAPPORT_EXCEL_FILE)
        google_drive_file = _copier_rapport_vers_google_drive(RAPPORT_EXCEL_FILE)

        print(f"  âœ… Rapport Excel Premium sauvegardÃ© dans :")
        print(f"     â†’ {RAPPORT_EXCEL_FILE}")
        if google_drive_file is not None:
            print(f"     â†’ Copie Google Drive : {google_drive_file}")
        
    except Exception as e:
        import traceback
        print(f"  âŒ Erreur lors de la sauvegarde Excel : {e}")
        traceback.print_exc()

