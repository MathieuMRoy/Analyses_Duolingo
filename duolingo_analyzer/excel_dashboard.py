from __future__ import annotations

from math import ceil, floor
from datetime import datetime
from pathlib import Path
import numbers
import re

import pandas as pd
from openpyxl import load_workbook
from openpyxl.chart import LineChart, Reference
from openpyxl.chart.layout import Layout, ManualLayout
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from .config import DAILY_LOG_FILE

SUMMARY_SHEET = "📊 Résumé Financier Q1"
TREND_SHEET = "📈 Tendances Mensuelles"
CHART_DATA_SHEET = "📊 Données Graphique"


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
        cleaned = re.sub(r"[^0-9\.\-\+]", "", cleaned)
        if cleaned in {"", "+", "-"}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _load_sheet(report_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(report_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _coerce_dates(series: pd.Series) -> pd.Series:
    def _parse_date(value: object):
        if value is None:
            return pd.NaT
        if isinstance(value, pd.Timestamp):
            return value
        if isinstance(value, datetime):
            return pd.Timestamp(value)
        if isinstance(value, str):
            raw = value.strip()
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b"):
                try:
                    return pd.Timestamp(datetime.strptime(raw, fmt))
                except ValueError:
                    continue
            return pd.NaT
        try:
            return pd.Timestamp(value)
        except Exception:
            return pd.NaT

    return series.apply(_parse_date)


def _prepare_global_trend_df(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(columns=["Libelle", "Taux Abonn. Super"])

    df = summary_df.copy()
    df["Date"] = _coerce_dates(df.get("Date"))
    if "Taux Abonn. Super" in df.columns:
        df["Taux Abonn. Super"] = df["Taux Abonn. Super"].apply(_parse_float)
    else:
        df["Taux Abonn. Super"] = None

    df = df.dropna(subset=["Date", "Taux Abonn. Super"]).sort_values("Date")
    df["Libelle"] = df["Date"].dt.strftime("%d %b")
    return df[["Libelle", "Taux Abonn. Super"]]


def _prepare_monthly_comparison_df(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(
            columns=[
                "Mois",
                "Conv. Super moy.",
                "Δ Conv. vs M-1",
                "Score d'eng. moy.",
                "Δ Score vs M-1",
                "Churn moyen",
                "Panel moyen",
            ]
        )

    df = summary_df.copy()
    df["Date"] = _coerce_dates(df.get("Date"))
    df = df.dropna(subset=["Date"]).sort_values("Date")
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Mois",
                "Conv. Super moy.",
                "Δ Conv. vs M-1",
                "Score d'eng. moy.",
                "Δ Score vs M-1",
                "Churn moyen",
                "Panel moyen",
            ]
        )

    for col in ["Taux Abonn. Super", "Score d'Engagement", "Taux d'Attrition Global", "Panel Total"]:
        if col in df.columns:
            df[col] = df[col].apply(_parse_float)
        else:
            df[col] = None

    df["Month"] = df["Date"].dt.to_period("M")
    monthly = (
        df.groupby("Month", as_index=False)
        .agg(
            {
                "Taux Abonn. Super": "mean",
                "Score d'Engagement": "mean",
                "Taux d'Attrition Global": "mean",
                "Panel Total": "mean",
            }
        )
        .sort_values("Month")
    )

    monthly["Mois"] = monthly["Month"].astype(str)
    monthly["Δ Conv. vs M-1"] = monthly["Taux Abonn. Super"].diff()
    monthly["Δ Score vs M-1"] = monthly["Score d'Engagement"].diff()

    monthly = monthly.rename(
        columns={
            "Taux Abonn. Super": "Conv. Super moy.",
            "Score d'Engagement": "Score d'eng. moy.",
            "Taux d'Attrition Global": "Churn moyen",
            "Panel Total": "Panel moyen",
        }
    )

    monthly = monthly[
        [
            "Mois",
            "Conv. Super moy.",
            "Δ Conv. vs M-1",
            "Score d'eng. moy.",
            "Δ Score vs M-1",
            "Churn moyen",
            "Panel moyen",
        ]
    ].tail(6)

    return monthly.reset_index(drop=True)


def _prepare_snapshot_df(chart_df: pd.DataFrame) -> pd.DataFrame:
    if chart_df.empty:
        return pd.DataFrame(columns=["Segment", "Taux"])

    if {"Segment", "Taux (%)"}.issubset(chart_df.columns):
        df = chart_df[["Segment", "Taux (%)"]].copy()
        df["Taux"] = df["Taux (%)"].apply(_parse_float)
        df = df.dropna(subset=["Segment", "Taux"])
        if not df.empty:
            return df[["Segment", "Taux"]].reset_index(drop=True)

    if "Date" not in chart_df.columns:
        return pd.DataFrame(columns=["Segment", "Taux"])

    df = chart_df.copy()
    df["Date"] = _coerce_dates(df["Date"])
    df = df.dropna(subset=["Date"]).sort_values("Date")
    if df.empty:
        return pd.DataFrame(columns=["Segment", "Taux"])

    latest = df.iloc[-1]
    mapping = [
        ("Panel global", "Moyenne Panel Global"),
        ("Débutants", "Cohorte Débutants (<1k XP)"),
        ("Standard", "Cohorte Standard (1k-5k XP)"),
        ("Super-Actifs", "Cohorte Super-Actifs (>5k XP)"),
    ]

    rows: list[dict[str, float | str]] = []
    for label, column in mapping:
        value = _parse_float(latest.get(column))
        if value is not None:
            rows.append({"Segment": label, "Taux": value})

    return pd.DataFrame(rows)


def _prepare_snapshot_df_from_log() -> pd.DataFrame:
    if not DAILY_LOG_FILE.exists():
        return pd.DataFrame(columns=["Segment", "Taux"])

    try:
        df = pd.read_csv(DAILY_LOG_FILE, usecols=["Date", "Username", "Cohort", "HasPlus"])
    except Exception:
        return pd.DataFrame(columns=["Segment", "Taux"])

    if df.empty:
        return pd.DataFrame(columns=["Segment", "Taux"])

    df = df[~df["Username"].astype(str).str.contains("Aggregated", na=False)].copy()
    df = df[df["Cohort"] != "Global"]
    if df.empty:
        return pd.DataFrame(columns=["Segment", "Taux"])

    counts = df.groupby("Date").size()
    valid_dates = counts[counts >= 100].index.tolist()
    latest_date = valid_dates[-1] if valid_dates else df["Date"].max()
    df = df[df["Date"] == latest_date]
    if df.empty:
        return pd.DataFrame(columns=["Segment", "Taux"])

    rows = [
        {"Segment": "Panel global", "Taux": round(df["HasPlus"].mean() * 100, 1)},
    ]

    label_map = {
        "Debutants": "Débutants",
        "Standard": "Standard",
        "Super-Actifs": "Super-Actifs",
    }
    for cohort, label in label_map.items():
        subset = df[df["Cohort"] == cohort]
        if not subset.empty:
            rows.append({"Segment": label, "Taux": round(subset["HasPlus"].mean() * 100, 1)})

    return pd.DataFrame(rows)


def _sheet_index(wb, reference_name: str, fallback: int) -> int:
    try:
        return wb.sheetnames.index(reference_name)
    except ValueError:
        return fallback


def _write_card(
    ws,
    header_range: str,
    value_range: str,
    title: str,
    value: str,
    header_fill: str,
    value_fill: str,
    white: str,
) -> None:
    ws.merge_cells(header_range)
    ws.merge_cells(value_range)

    thin = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"),
    )

    header = ws[header_range.split(":")[0]]
    header.value = title
    header.fill = PatternFill("solid", start_color=header_fill, end_color=header_fill)
    header.font = Font(name="Calibri", size=10, bold=True, color=white)
    header.alignment = Alignment(horizontal="center", vertical="center")
    header.border = thin

    value_cell = ws[value_range.split(":")[0]]
    value_cell.value = value
    value_cell.fill = PatternFill("solid", start_color=value_fill, end_color=value_fill)
    value_cell.font = Font(name="Calibri", size=16, bold=True, color="1F1F1F")
    value_cell.alignment = Alignment(horizontal="center", vertical="center")
    value_cell.border = thin


def _write_section_header(ws, cell_range: str, title: str, fill_color: str, white: str) -> None:
    ws.merge_cells(cell_range)
    cell = ws[cell_range.split(":")[0]]
    cell.value = title
    cell.fill = PatternFill("solid", start_color=fill_color, end_color=fill_color)
    cell.font = Font(name="Calibri", size=11, bold=True, color=white)
    cell.alignment = Alignment(horizontal="center", vertical="center")


def _format_points(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.1f} pts"


def _format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.1f}%"


def _format_integer(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{int(round(float(value))):,}".replace(",", " ")


def _delta_font_color(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "666666"
    if float(value) > 0:
        return "008000"
    if float(value) < 0:
        return "C00000"
    return "666666"


def refresh_trends_dashboard(report_path: Path) -> None:
    if not report_path.exists():
        raise FileNotFoundError(report_path)

    summary_df = _load_sheet(report_path, SUMMARY_SHEET)
    chart_df = _load_sheet(report_path, CHART_DATA_SHEET)

    global_df = _prepare_global_trend_df(summary_df)
    monthly_df = _prepare_monthly_comparison_df(summary_df)
    snapshot_df = _prepare_snapshot_df(chart_df)
    if snapshot_df.empty:
        snapshot_df = _prepare_snapshot_df_from_log()

    wb = load_workbook(report_path)

    if TREND_SHEET in wb.sheetnames:
        wb.remove(wb[TREND_SHEET])
    if CHART_DATA_SHEET in wb.sheetnames:
        wb.remove(wb[CHART_DATA_SHEET])

    chart_ws = wb.create_sheet(CHART_DATA_SHEET)
    trend_index = _sheet_index(wb, "📖 Dictionnaire des KPIs", len(wb.sheetnames))
    trend_ws = wb.create_sheet(TREND_SHEET, trend_index + 1)

    chart_ws.append(["Date", "Taux Abonn. Super"])
    for row in global_df.itertuples(index=False):
        chart_ws.append([row.Libelle, row[1]])

    chart_ws["D1"] = "Segment"
    chart_ws["E1"] = "Taux (%)"
    for idx, row in enumerate(snapshot_df.itertuples(index=False), start=2):
        chart_ws[f"D{idx}"] = row.Segment
        chart_ws[f"E{idx}"] = row.Taux

    chart_ws.sheet_state = "hidden"

    DUO_GREEN = "58CC02"
    DUO_BLUE = "1CB0F6"
    NAVY = "1F4E78"
    SOFT_BLUE = "EAF6FF"
    SOFT_GREEN = "EEF8E8"
    SOFT_ORANGE = "FFF4E9"
    SOFT_RED = "FFF1F1"
    SOFT_GREY = "F6F7FB"
    WHITE = "FFFFFF"

    trend_ws.sheet_view.showGridLines = False
    for col, width in {
        "A": 14, "B": 14, "C": 14, "D": 14, "E": 14, "F": 14, "G": 14,
        "H": 14, "I": 3, "J": 16, "K": 14, "L": 14, "M": 16, "N": 18,
    }.items():
        trend_ws.column_dimensions[col].width = width

    trend_ws.merge_cells("A1:N2")
    title = trend_ws["A1"]
    title.value = "Monetisation & Tendances"
    title.fill = PatternFill("solid", start_color=NAVY, end_color=NAVY)
    title.font = Font(name="Calibri", size=18, bold=True, color=WHITE)
    title.alignment = Alignment(horizontal="center", vertical="center")

    trend_ws.merge_cells("A3:N3")
    subtitle = trend_ws["A3"]
    subtitle.value = "Vue synthétique des abonnements Super et du dernier snapshot par cohorte."
    subtitle.font = Font(name="Calibri", size=10, italic=True, color="5A5A5A")
    subtitle.alignment = Alignment(horizontal="center", vertical="center")

    latest = None
    previous = None
    if not summary_df.empty and "Date" in summary_df.columns:
        temp = summary_df.copy()
        temp["Date"] = _coerce_dates(temp["Date"])
        temp = temp.dropna(subset=["Date"]).sort_values("Date")
        if not temp.empty:
            latest = temp.iloc[-1]
            if len(temp) > 1:
                previous = temp.iloc[-2]

    if latest is not None:
        latest_date = latest["Date"].strftime("%Y-%m-%d")
        latest_super = _parse_float(latest.get("Taux Abonn. Super"))
        latest_churn = _parse_float(latest.get("Taux d'Attrition Global"))
        latest_panel = _parse_float(latest.get("Panel Total"))
        latest_score = _parse_float(latest.get("Score d'Engagement"))
    else:
        latest_date = "N/A"
        latest_super = None
        latest_churn = None
        latest_panel = None
        latest_score = None

    previous_super = _parse_float(previous.get("Taux Abonn. Super")) if previous is not None else None
    delta_super = (latest_super - previous_super) if latest_super is not None and previous_super is not None else None

    _write_card(trend_ws, "A5:C5", "A6:C7", "Dernier releve", latest_date, NAVY, SOFT_GREY, WHITE)
    _write_card(
        trend_ws,
        "D5:F5",
        "D6:F7",
        "Conv. Super",
        f"{latest_super:.1f}%" if latest_super is not None else "N/A",
        DUO_BLUE,
        SOFT_BLUE,
        WHITE,
    )
    _write_card(
        trend_ws,
        "G5:I5",
        "G6:I7",
        "Var. vs veille",
        _format_points(delta_super),
        "FF8A34",
        SOFT_ORANGE,
        WHITE,
    )
    _write_card(
        trend_ws,
        "J5:L5",
        "J6:L7",
        "Score d'engagement",
        f"{latest_score:.1f}%" if latest_score is not None else "N/A",
        DUO_GREEN,
        SOFT_GREEN,
        WHITE,
    )

    trend_ws.merge_cells("A8:L8")
    summary_band = trend_ws["A8"]
    summary_band.value = (
        f"Panel suivi : {int(latest_panel):,}".replace(",", " ") if latest_panel is not None else "Panel suivi : N/A"
    ) + (
        f"   |   Churn global : {latest_churn:.1f}%"
        if latest_churn is not None else
        "   |   Churn global : N/A"
    )
    summary_band.font = Font(name="Calibri", size=10, bold=True, color="4A4A4A")
    summary_band.alignment = Alignment(horizontal="center", vertical="center")

    _write_section_header(
        trend_ws,
        "A10:H10",
        "Evolution recente",
        NAVY,
        WHITE,
    )
    trend_ws.merge_cells("A11:H11")
    chart_help = trend_ws["A11"]
    chart_help.value = "Ce graphique montre l'evolution jour apres jour du taux d'abonnement Super sur l'ensemble du panel suivi."
    chart_help.font = Font(name="Calibri", size=9, italic=True, color="666666")
    chart_help.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

    if len(global_df) >= 2:
        trend_chart = LineChart()
        trend_chart.style = 2
        trend_chart.height = 6.8
        trend_chart.width = 9.6
        trend_chart.legend = None
        trend_chart.title = None
        trend_chart.y_axis.title = None
        trend_chart.x_axis.title = None
        min_val = float(global_df["Taux Abonn. Super"].min())
        max_val = float(global_df["Taux Abonn. Super"].max())
        trend_chart.y_axis.scaling.min = max(0, floor((min_val - 2) / 5) * 5)
        trend_chart.y_axis.scaling.max = ceil((max_val + 2) / 5) * 5
        trend_chart.x_axis.tickLblPos = "low"
        trend_chart.x_axis.delete = False
        trend_chart.y_axis.delete = False
        trend_chart.x_axis.majorTickMark = "none"
        trend_chart.y_axis.majorTickMark = "none"
        trend_chart.plot_area.layout = Layout(
            manualLayout=ManualLayout(
                x=0.12,
                y=0.08,
                h=0.72,
                w=0.82,
                xMode="edge",
                yMode="edge",
            )
        )

        data = Reference(chart_ws, min_col=2, min_row=1, max_row=len(global_df) + 1)
        categories = Reference(chart_ws, min_col=1, min_row=2, max_row=len(global_df) + 1)
        trend_chart.add_data(data, titles_from_data=True)
        trend_chart.set_categories(categories)

        series = trend_chart.series[0]
        series.graphicalProperties.line.solidFill = DUO_BLUE
        series.graphicalProperties.line.width = 24000
        series.marker.symbol = "circle"
        series.marker.size = 7
        series.marker.graphicalProperties.solidFill = DUO_BLUE
        series.marker.graphicalProperties.line.solidFill = DUO_BLUE

        trend_ws.add_chart(trend_chart, "A13")
    else:
        trend_ws.merge_cells("A13:H18")
        empty_msg = trend_ws["A13"]
        empty_msg.value = "Pas assez d'historique pour afficher une tendance."
        empty_msg.font = Font(name="Calibri", size=11, italic=True, color="666666")
        empty_msg.alignment = Alignment(horizontal="center", vertical="center")

    _write_section_header(
        trend_ws,
        "J10:N10",
        "Lecture par segment",
        DUO_GREEN,
        WHITE,
    )

    panel_rate = None
    if not snapshot_df.empty:
        panel_row = snapshot_df[snapshot_df["Segment"] == "Panel global"]
        panel_rate = panel_row["Taux"].iloc[0] if not panel_row.empty else None

    table_headers = ["Segment", "Taux premium", "Ecart vs panel", "Lecture"]
    for col, title_value in zip(["J", "K", "L", "M"], table_headers):
        cell = trend_ws[f"{col}12"]
        cell.value = title_value
        cell.fill = PatternFill("solid", start_color=SOFT_GREY, end_color=SOFT_GREY)
        cell.font = Font(name="Calibri", size=10, bold=True, color="333333")
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9"),
        )

    if not snapshot_df.empty:
        display_df = snapshot_df.copy()
        if panel_rate is not None:
            display_df["Gap"] = display_df["Taux"] - panel_rate
        else:
            display_df["Gap"] = None

        panel_df = display_df[display_df["Segment"] == "Panel global"]
        cohorts_df = display_df[display_df["Segment"] != "Panel global"].sort_values("Taux", ascending=False)
        display_df = pd.concat([panel_df, cohorts_df], ignore_index=True)

        fill_map = {
            "Panel global": "EAF6FF",
            "Super-Actifs": "EEF8E8",
            "Standard": "FFF7DB",
            "Débutants": "FFF1F1",
        }

        for row_idx, row in enumerate(display_df.itertuples(index=False), start=13):
            segment = row.Segment
            taux = row.Taux
            gap = row.Gap
            if segment == "Panel global":
                lecture = "Reference"
            elif gap is not None and gap >= 1:
                lecture = "Au-dessus du panel"
            elif gap is not None and gap <= -1:
                lecture = "Sous le panel"
            else:
                lecture = "Proche du panel"

            values = [
                segment,
                f"{taux:.1f}%",
                _format_points(gap) if gap is not None else "N/A",
                lecture,
            ]
            for col, value in zip(["J", "K", "L", "M"], values):
                cell = trend_ws[f"{col}{row_idx}"]
                cell.value = value
                cell.fill = PatternFill(
                    "solid",
                    start_color=fill_map.get(segment, WHITE),
                    end_color=fill_map.get(segment, WHITE),
                )
                cell.font = Font(name="Calibri", size=10, bold=(col == "J"), color="333333")
                if col == "L" and gap is not None:
                    color = "008000" if gap > 0 else ("C00000" if gap < 0 else "666666")
                    cell.font = Font(name="Calibri", size=10, bold=True, color=color)
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                cell.border = Border(
                    left=Side(style="thin", color="D9D9D9"),
                    right=Side(style="thin", color="D9D9D9"),
                    top=Side(style="thin", color="D9D9D9"),
                    bottom=Side(style="thin", color="D9D9D9"),
                )
    else:
        trend_ws.merge_cells("J13:N16")
        empty_msg = trend_ws["J13"]
        empty_msg.value = "Pas assez de donnees pour comparer les segments."
        empty_msg.font = Font(name="Calibri", size=11, italic=True, color="666666")
        empty_msg.alignment = Alignment(horizontal="center", vertical="center")

    trend_ws.merge_cells("A27:N27")
    note = trend_ws["A27"]
    note.value = (
        "Lecture: la courbe montre la conversion Super dans le temps. "
        "Le tableau compare le dernier releve complet par segment au panel global."
    )
    note.font = Font(name="Calibri", size=10, italic=True, color="666666")
    note.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

    _write_section_header(
        trend_ws,
        "A29:N29",
        "Comparatif mensuel",
        NAVY,
        WHITE,
    )
    trend_ws.merge_cells("A30:N30")
    monthly_help = trend_ws["A30"]
    monthly_help.value = (
        "Cette section consolide les moyennes mensuelles du resume quotidien. "
        "Les ecarts vs M-1 apparaissent automatiquement des qu'un nouveau mois est disponible."
    )
    monthly_help.font = Font(name="Calibri", size=9, italic=True, color="666666")
    monthly_help.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

    latest_month = monthly_df.iloc[-1] if not monthly_df.empty else None
    latest_month_label = latest_month["Mois"] if latest_month is not None else "N/A"
    latest_month_conv = latest_month["Conv. Super moy."] if latest_month is not None else None
    latest_month_delta_conv = latest_month.iloc[2] if latest_month is not None else None
    latest_month_score = latest_month["Score d'eng. moy."] if latest_month is not None else None
    latest_month_delta_score = latest_month.iloc[4] if latest_month is not None else None
    latest_month_churn = latest_month["Churn moyen"] if latest_month is not None else None
    latest_month_panel = latest_month["Panel moyen"] if latest_month is not None else None

    _write_card(trend_ws, "A32:C32", "A33:C34", "Dernier mois", str(latest_month_label), NAVY, SOFT_GREY, WHITE)
    _write_card(
        trend_ws,
        "D32:F32",
        "D33:F34",
        "Conv. moyenne",
        _format_percent(latest_month_conv),
        DUO_BLUE,
        SOFT_BLUE,
        WHITE,
    )
    _write_card(
        trend_ws,
        "G32:I32",
        "G33:I34",
        "Score moyen",
        _format_percent(latest_month_score),
        DUO_GREEN,
        SOFT_GREEN,
        WHITE,
    )
    _write_card(
        trend_ws,
        "J32:L32",
        "J33:L34",
        "Churn moyen",
        _format_percent(latest_month_churn),
        "FF6B6B",
        SOFT_RED,
        WHITE,
    )
    _write_card(
        trend_ws,
        "M32:N32",
        "M33:N34",
        "Panel moyen",
        _format_integer(latest_month_panel),
        NAVY,
        SOFT_GREY,
        WHITE,
    )

    _write_card(
        trend_ws,
        "A36:F36",
        "A37:F38",
        "Δ Conv. vs M-1",
        _format_points(latest_month_delta_conv) if pd.notna(latest_month_delta_conv) else "N/A",
        "FF8A34",
        SOFT_ORANGE,
        WHITE,
    )
    trend_ws["A37"].font = Font(
        name="Calibri",
        size=16,
        bold=True,
        color=_delta_font_color(latest_month_delta_conv),
    )

    _write_card(
        trend_ws,
        "G36:L36",
        "G37:L38",
        "Δ Score vs M-1",
        _format_points(latest_month_delta_score) if pd.notna(latest_month_delta_score) else "N/A",
        NAVY,
        SOFT_BLUE,
        WHITE,
    )
    trend_ws["G37"].font = Font(
        name="Calibri",
        size=16,
        bold=True,
        color=_delta_font_color(latest_month_delta_score),
    )

    _write_card(
        trend_ws,
        "M36:N36",
        "M37:N38",
        "Mois visibles",
        str(len(monthly_df)) if not monthly_df.empty else "0",
        NAVY,
        SOFT_GREY,
        WHITE,
    )

    trend_ws.merge_cells("A40:N40")
    monthly_table_title = trend_ws["A40"]
    monthly_table_title.value = "Historique mensuel consolide"
    monthly_table_title.fill = PatternFill("solid", start_color=SOFT_GREY, end_color=SOFT_GREY)
    monthly_table_title.font = Font(name="Calibri", size=10, bold=True, color="333333")
    monthly_table_title.alignment = Alignment(horizontal="center", vertical="center")

    monthly_headers = [
        "Mois",
        "Conv. Super moy.",
        "Δ Conv. vs M-1",
        "Score d'eng. moy.",
        "Δ Score vs M-1",
        "Churn moyen",
        "Panel moyen",
    ]
    monthly_cols = ["A", "C", "E", "G", "I", "K", "M"]
    for col, title_value in zip(monthly_cols, monthly_headers):
        start = f"{col}42"
        end = f"{chr(ord(col) + 1)}42"
        trend_ws.merge_cells(f"{start}:{end}")
        cell = trend_ws[start]
        cell.value = title_value
        cell.fill = PatternFill("solid", start_color=SOFT_GREY, end_color=SOFT_GREY)
        cell.font = Font(name="Calibri", size=10, bold=True, color="333333")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9"),
        )

    if not monthly_df.empty:
        latest_month_index = len(monthly_df) - 1
        for idx, row in enumerate(monthly_df.itertuples(index=False)):
            offset = 43 + idx
            conv = row[1]
            delta_conv = row[2]
            score = row[3]
            delta_score = row[4]
            churn = row[5]
            panel = row[6]
            values = [
                row[0],
                _format_percent(conv),
                _format_points(delta_conv) if pd.notna(delta_conv) else "N/A",
                _format_percent(score),
                _format_points(delta_score) if pd.notna(delta_score) else "N/A",
                _format_percent(churn),
                _format_integer(panel),
            ]
            is_latest_month = idx == latest_month_index
            base_fill = SOFT_BLUE if is_latest_month else (WHITE if offset % 2 else SOFT_GREY)
            for col, value in zip(monthly_cols, values):
                start = f"{col}{offset}"
                end = f"{chr(ord(col) + 1)}{offset}"
                trend_ws.merge_cells(f"{start}:{end}")
                cell = trend_ws[start]
                cell.value = value
                cell.fill = PatternFill(
                    "solid",
                    start_color=base_fill,
                    end_color=base_fill,
                )
                cell.font = Font(name="Calibri", size=10, bold=(col == "A" or is_latest_month), color="333333")
                if col in {"E", "I"} and value != "N/A":
                    num = delta_conv if col == "E" else delta_score
                    color = _delta_font_color(num)
                    cell.font = Font(name="Calibri", size=10, bold=True, color=color)
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                cell.border = Border(
                    left=Side(style="thin", color="D9D9D9"),
                    right=Side(style="thin", color="D9D9D9"),
                    top=Side(style="thin", color="D9D9D9"),
                    bottom=Side(style="thin", color="D9D9D9"),
                )
    else:
        trend_ws.merge_cells("A43:N45")
        empty_msg = trend_ws["A43"]
        empty_msg.value = "Le comparatif mensuel apparaitra ici des qu'au moins un mois de resume sera disponible."
        empty_msg.font = Font(name="Calibri", size=10, italic=True, color="666666")
        empty_msg.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    wb.save(report_path)
