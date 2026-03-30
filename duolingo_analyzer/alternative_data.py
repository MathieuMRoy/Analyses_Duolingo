"""Daily alternative data snapshots and weekly display helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
import re

import pandas as pd

from .config import ALTERNATIVE_DATA_HISTORY_FILE, ALTERNATIVE_DATA_INPUT_FILE, now_toronto


TEMPLATE_ROWS = [
    {"sort_order": 1, "signal_label": "Job Posts", "value": "", "source": "Careers / ATS", "notes": "", "enabled": 1},
    {"sort_order": 2, "signal_label": "Google Trends", "value": "", "source": "Google Trends", "notes": "", "enabled": 1},
    {"sort_order": 3, "signal_label": "Web Traffic", "value": "", "source": "Traffic data", "notes": "", "enabled": 1},
    {"sort_order": 4, "signal_label": "App Downloads", "value": "", "source": "App intelligence", "notes": "", "enabled": 1},
    {"sort_order": 5, "signal_label": "Reddit Mentions", "value": "", "source": "Reddit", "notes": "", "enabled": 1},
    {"sort_order": 6, "signal_label": "Stocktwits Mentions", "value": "", "source": "Stocktwits", "notes": "", "enabled": 1},
    {"sort_order": 7, "signal_label": "Instagram Followers", "value": "", "source": "Instagram", "notes": "", "enabled": 1},
    {"sort_order": 8, "signal_label": "Threads Followers", "value": "", "source": "Threads", "notes": "", "enabled": 1},
]


def _slugify(label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(label).strip().lower())
    return slug.strip("_") or "signal"


def _parse_numeric(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _format_value(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    rounded = round(float(value), 1)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"{int(round(rounded)):,}".replace(",", " ")
    return f"{rounded:,.1f}".replace(",", " ").replace(".", ",")


def _week_start(value: pd.Timestamp) -> pd.Timestamp:
    return value - timedelta(days=value.weekday())


def _week_label(week_start: pd.Timestamp) -> str:
    week_end = week_start + timedelta(days=6)
    return f"{week_start.strftime('%d %b')} - {week_end.strftime('%d %b')}"


def _ensure_input_template() -> None:
    if ALTERNATIVE_DATA_INPUT_FILE.exists():
        return
    pd.DataFrame(TEMPLATE_ROWS).to_csv(ALTERNATIVE_DATA_INPUT_FILE, index=False, encoding="utf-8-sig")


def _normalize_inputs(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["sort_order", "signal_key", "signal_label", "value", "source", "notes", "enabled"])

    normalized = df.copy()
    rename_map = {
        "Signal": "signal_label",
        "KPI": "signal_label",
        "Label": "signal_label",
        "Value": "value",
        "Valeur": "value",
        "Source": "source",
        "Notes": "notes",
        "Commentaire": "notes",
        "Enabled": "enabled",
        "Actif": "enabled",
        "Sort Order": "sort_order",
        "Order": "sort_order",
    }
    normalized = normalized.rename(columns={column: rename_map.get(column, column) for column in normalized.columns})

    if "signal_label" not in normalized.columns:
        return pd.DataFrame(columns=["sort_order", "signal_key", "signal_label", "value", "source", "notes", "enabled"])

    if "signal_key" not in normalized.columns:
        normalized["signal_key"] = normalized["signal_label"].map(_slugify)
    if "sort_order" not in normalized.columns:
        normalized["sort_order"] = range(1, len(normalized) + 1)
    if "source" not in normalized.columns:
        normalized["source"] = ""
    if "notes" not in normalized.columns:
        normalized["notes"] = ""
    if "enabled" not in normalized.columns:
        normalized["enabled"] = 1

    normalized["value"] = normalized["value"].map(_parse_numeric) if "value" in normalized.columns else None
    normalized["enabled"] = normalized["enabled"].fillna(1).astype(str).str.strip().str.lower().isin({"1", "true", "yes", "oui"})
    normalized = normalized[normalized["enabled"]]
    normalized = normalized.dropna(subset=["signal_label"])
    normalized["signal_label"] = normalized["signal_label"].astype(str).str.strip()
    normalized["signal_key"] = normalized["signal_key"].astype(str).str.strip().replace("", pd.NA).fillna(normalized["signal_label"].map(_slugify))
    normalized["sort_order"] = pd.to_numeric(normalized["sort_order"], errors="coerce").fillna(999).astype(int)
    normalized = normalized.dropna(subset=["value"])
    normalized = normalized.sort_values(["sort_order", "signal_label"]).reset_index(drop=True)

    return normalized[["sort_order", "signal_key", "signal_label", "value", "source", "notes", "enabled"]]


def load_alternative_data_inputs() -> pd.DataFrame:
    _ensure_input_template()
    try:
        df = pd.read_csv(ALTERNATIVE_DATA_INPUT_FILE)
    except Exception:
        return pd.DataFrame(columns=["sort_order", "signal_key", "signal_label", "value", "source", "notes", "enabled"])
    return _normalize_inputs(df)


def load_alternative_data_history() -> pd.DataFrame:
    if not ALTERNATIVE_DATA_HISTORY_FILE.exists():
        return pd.DataFrame(
            columns=[
                "date",
                "week_start",
                "week_label",
                "sort_order",
                "signal_key",
                "signal_label",
                "value",
                "value_display",
                "source",
                "notes",
            ]
        )

    try:
        df = pd.read_csv(ALTERNATIVE_DATA_HISTORY_FILE)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df["week_start"] = pd.to_datetime(df.get("week_start"), errors="coerce")
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce")
    if "sort_order" in df.columns:
        df["sort_order"] = pd.to_numeric(df["sort_order"], errors="coerce").fillna(999).astype(int)
    else:
        df["sort_order"] = 999
    for column in ["signal_key", "signal_label", "value_display", "source", "notes", "week_label"]:
        if column not in df.columns:
            df[column] = ""

    df = df.dropna(subset=["date", "signal_key", "value"])
    return df.sort_values(["date", "sort_order", "signal_label"]).reset_index(drop=True)


def refresh_alternative_data_history(as_of_date: str | None = None) -> pd.DataFrame:
    inputs_df = load_alternative_data_inputs()
    history_df = load_alternative_data_history()

    if inputs_df.empty:
        return history_df

    date_value = pd.Timestamp(as_of_date) if as_of_date else pd.Timestamp(now_toronto().strftime("%Y-%m-%d"))
    week_start = _week_start(date_value)
    week_label = _week_label(week_start)

    snapshot_df = inputs_df.copy()
    snapshot_df["date"] = date_value
    snapshot_df["week_start"] = week_start
    snapshot_df["week_label"] = week_label
    snapshot_df["value_display"] = snapshot_df["value"].map(_format_value)

    if history_df.empty:
        history_df = snapshot_df[
            ["date", "week_start", "week_label", "sort_order", "signal_key", "signal_label", "value", "value_display", "source", "notes"]
        ].copy()
    else:
        history_df = history_df[
            ["date", "week_start", "week_label", "sort_order", "signal_key", "signal_label", "value", "value_display", "source", "notes"]
        ].copy()
        history_df["date_str"] = history_df["date"].dt.strftime("%Y-%m-%d")
        current_date_str = date_value.strftime("%Y-%m-%d")
        history_df = history_df[history_df["date_str"] != current_date_str].drop(columns=["date_str"])
        history_df = pd.concat(
            [
                history_df,
                snapshot_df[
                    ["date", "week_start", "week_label", "sort_order", "signal_key", "signal_label", "value", "value_display", "source", "notes"]
                ],
            ],
            ignore_index=True,
        )

    history_df = history_df.sort_values(["date", "sort_order", "signal_label"]).reset_index(drop=True)
    ALTERNATIVE_DATA_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    history_df.to_csv(ALTERNATIVE_DATA_HISTORY_FILE, index=False, encoding="utf-8-sig")
    return history_df


def _latest_weekly_rows(df_history: pd.DataFrame) -> pd.DataFrame:
    if df_history is None or df_history.empty:
        return pd.DataFrame()

    weekly = (
        df_history.sort_values(["signal_key", "date"])
        .groupby(["signal_key", "week_start"], as_index=False)
        .last()
        .sort_values(["signal_key", "week_start"])
    )

    weekly["prev_value"] = weekly.groupby("signal_key")["value"].shift(1)
    weekly["wow_change_pct"] = None
    mask = weekly["prev_value"].notna() & (weekly["prev_value"] != 0)
    weekly.loc[mask, "wow_change_pct"] = (weekly.loc[mask, "value"] / weekly.loc[mask, "prev_value"]) - 1

    latest = weekly.groupby("signal_key", as_index=False).tail(1).copy()
    latest["week_label"] = latest["week_start"].map(_week_label)
    latest["wow_display"] = latest["wow_change_pct"].map(
        lambda value: "N/D" if value is None or pd.isna(value) else f"{value:+.1%}".replace(".", ",")
    )
    latest["value_display"] = latest["value"].map(_format_value)
    return latest.sort_values(["sort_order", "signal_label"]).reset_index(drop=True)


def build_alternative_data_raw_df(package: dict[str, object]) -> pd.DataFrame:
    history_df = package.get("raw_history_df")
    if isinstance(history_df, pd.DataFrame):
        return history_df.copy()
    return pd.DataFrame()


def generate_alternative_data_package(as_of_date: str | None = None) -> dict[str, object]:
    history_df = refresh_alternative_data_history(as_of_date)
    latest_weekly = _latest_weekly_rows(history_df)

    if history_df.empty:
        return {
            "metadata": {
                "as_of_date": as_of_date or now_toronto().strftime("%Y-%m-%d"),
                "has_data": False,
                "latest_snapshot_date": None,
                "latest_week_label": None,
                "signal_count": 0,
                "signals_up": 0,
                "signals_down": 0,
                "signals_flat": 0,
            },
            "rows": [],
            "raw_history_df": history_df,
        }

    latest_snapshot_date = history_df["date"].max()
    signals_up = int((latest_weekly["wow_change_pct"].fillna(0) > 0).sum()) if not latest_weekly.empty else 0
    signals_down = int((latest_weekly["wow_change_pct"].fillna(0) < 0).sum()) if not latest_weekly.empty else 0
    signals_flat = int((latest_weekly["wow_change_pct"].fillna(0) == 0).sum()) if not latest_weekly.empty else 0

    rows = latest_weekly[
        [
            "signal_label",
            "value",
            "value_display",
            "wow_change_pct",
            "wow_display",
            "week_label",
            "source",
            "notes",
        ]
    ].to_dict("records")

    return {
        "metadata": {
            "as_of_date": as_of_date or latest_snapshot_date.strftime("%Y-%m-%d"),
            "has_data": bool(rows),
            "latest_snapshot_date": latest_snapshot_date.strftime("%Y-%m-%d"),
            "latest_week_label": latest_weekly["week_label"].iloc[0] if not latest_weekly.empty else None,
            "signal_count": len(rows),
            "signals_up": signals_up,
            "signals_down": signals_down,
            "signals_flat": signals_flat,
        },
        "rows": rows,
        "raw_history_df": history_df,
    }
