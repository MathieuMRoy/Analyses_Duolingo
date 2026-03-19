"""Helpers pour les onglets Tendances Mensuelles et Donnees Graphique."""

from __future__ import annotations

import pandas as pd
from openpyxl.chart import LineChart, Reference
from openpyxl.chart.layout import Layout, ManualLayout


def build_monthly_trends_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        df_cleaned = df.copy()
        df_cleaned["Month"] = pd.to_datetime(df_cleaned["Date"]).dt.to_period("M").astype(str)

        df_sorted = df_cleaned.sort_values(["Username", "Date"])
        df_sorted["Prev_XP"] = df_sorted.groupby("Username")["TotalXP"].shift(1)
        df_sorted["Daily_Delta"] = df_sorted["TotalXP"] - df_sorted["Prev_XP"]
        df_sorted.loc[df_sorted["Daily_Delta"] < 0, "Daily_Delta"] = 0

        cohort_column = "Cohorte" if "Cohorte" in df_sorted.columns else "Cohort"

        monthly_cohorts = (
            df_sorted.groupby(["Month", cohort_column])
            .agg({"Streak": "mean", "HasPlus": "mean", "Daily_Delta": "mean"})
            .reset_index()
        )
        monthly_cohorts.columns = [
            "Mois",
            "Cohorte",
            "Série Moy. (j)",
            "Taux Super (%)",
            "Activité (XP/j)",
        ]

        monthly_global = df_sorted.groupby(["Month"]).agg(
            {"Streak": "mean", "HasPlus": "mean", "Daily_Delta": "mean"}
        ).reset_index()
        monthly_global["Cohorte"] = "Global"
        monthly_global = monthly_global[["Month", "Cohorte", "Streak", "HasPlus", "Daily_Delta"]]
        monthly_global.columns = [
            "Mois",
            "Cohorte",
            "Série Moy. (j)",
            "Taux Super (%)",
            "Activité (XP/j)",
        ]

        monthly_stats = pd.concat([monthly_cohorts, monthly_global], ignore_index=True)
        monthly_stats = monthly_stats.sort_values(["Cohorte", "Mois"])

        for col in ["Série Moy. (j)", "Taux Super (%)", "Activité (XP/j)"]:
            new_col = f"Δ {col} (MoM)"
            monthly_stats[new_col] = monthly_stats.groupby("Cohorte")[col].diff()

        monthly_stats = monthly_stats.sort_values(["Mois", "Cohorte"])
        monthly_stats["Taux Super (%)"] = monthly_stats["Taux Super (%)"].round(6)
        monthly_stats["Δ Taux Super (%) (MoM)"] = monthly_stats["Δ Taux Super (%) (MoM)"].round(6)
    except Exception:
        monthly_stats = pd.DataFrame()

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

        for cohort_name in ["Debutants", "Standard", "Super-Actifs"]:
            if cohort_name not in df_daily_conv.columns:
                df_daily_conv[cohort_name] = 0.0

        cols_map = {
            "Date": "Date",
            "Global": "Moyenne Panel Global",
            "Debutants": "Cohorte Débutants (<1k XP)",
            "Standard": "Cohorte Standard (1k-5k XP)",
            "Super-Actifs": "Cohorte Super-Actifs (>5k XP)",
        }
        df_daily_conv = df_daily_conv[list(cols_map.keys())].rename(columns=cols_map)
        df_daily_conv["Date"] = pd.to_datetime(df_daily_conv["Date"], errors="coerce")
    except Exception:
        df_daily_conv = pd.DataFrame()

    return monthly_stats, df_daily_conv


def add_monthly_trends_chart(ws, wb, chart_data_sheet_name: str) -> None:
    data_ws = wb[chart_data_sheet_name]
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

    for index in range(2, 6):
        values = Reference(data_ws, min_col=index, min_row=1, max_row=max_row)
        chart.add_data(values, titles_from_data=True)

    chart.set_categories(dates)
    chart.legend.position = "r"

    colors = ["1CB0F6", "FF4B4B", "FFC800", "58CC02"]
    for index, series in enumerate(chart.series):
        series.graphicalProperties.line.solidFill = colors[index % len(colors)]
        series.graphicalProperties.line.width = 25000 if index == 0 else 15000
        series.marker.symbol = "circle"
        series.marker.size = 5
        series.marker.graphicalProperties.solidFill = colors[index % len(colors)]
        series.marker.graphicalProperties.line.solidFill = colors[index % len(colors)]

    ws.add_chart(chart, "J2")
    data_ws.sheet_state = "hidden"
