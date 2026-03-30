"""
Alternative data collection, history persistence, and week-over-week packaging.

The goal of this module is to keep a lightweight set of free or manual external
signals that can be snapshotted every day alongside the Duolingo user panel.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import math
import re
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from .config import ALTERNATIVE_DATA_HISTORY_FILE, ALTERNATIVE_DATA_INPUT_FILE


HTTP_TIMEOUT = 8
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
MONTHS_FR = {
    1: "janv.",
    2: "fevr.",
    3: "mars",
    4: "avr.",
    5: "mai",
    6: "juin",
    7: "juil.",
    8: "aout",
    9: "sept.",
    10: "oct.",
    11: "nov.",
    12: "dec.",
}
HISTORY_COLUMNS = [
    "snapshot_date",
    "week_start",
    "week_label",
    "sort_order",
    "signal_key",
    "signal_label",
    "value",
    "source",
    "notes",
    "collection_mode",
]


@dataclass(frozen=True)
class SignalReading:
    signal_key: str
    signal_label: str
    value: float
    source: str
    notes: str
    sort_order: int
    collection_mode: str = "automatic"


def _build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


HTTP = _build_http_session()


def _coerce_date(value: object | None) -> date:
    if value is None or value == "":
        return datetime.now().date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return pd.to_datetime(value).date()


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _week_label(value: date) -> str:
    start = _week_start(value)
    end = start + timedelta(days=6)
    start_month = MONTHS_FR[start.month]
    end_month = MONTHS_FR[end.month]
    if start.month == end.month:
        return f"{start.day} {start_month} - {end.day} {end_month}"
    return f"{start.day} {start_month} - {end.day} {end_month}"


def _normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).strip().lower()).strip("_")


def _clean_numeric(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)

    text = str(value).strip()
    if not text or text.upper() == "N/A":
        return None

    text = text.replace("\u202f", " ").replace("\xa0", " ")
    text = text.replace("$", "").replace("M$", "").replace("%", "")
    text = text.replace(" ", "")
    text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _format_fr_number(value: float | None, decimals: int = 0) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/D"
    fmt = f"{{:,.{decimals}f}}".format(value)
    fmt = fmt.replace(",", " ").replace(".", ",")
    return fmt


def _format_value(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/D"
    decimals = 1 if abs(value) < 100 and not float(value).is_integer() else 0
    return _format_fr_number(value, decimals=decimals)


def _format_wow(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/D"
    sign = "+" if value > 0 else ""
    return f"{sign}{_format_fr_number(value * 100, 1)}%"


def _safe_get_json(url: str, *, params: dict[str, object] | None = None) -> dict | None:
    try:
        response = HTTP.get(url, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _safe_get_text(url: str, *, params: dict[str, object] | None = None) -> str | None:
    try:
        response = HTTP.get(url, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.text
    except Exception:
        return None


def _collect_greenhouse_job_posts(as_of_date: date) -> SignalReading | None:
    payload = _safe_get_json("https://boards-api.greenhouse.io/v1/boards/duolingo/jobs")
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        return None
    return SignalReading(
        signal_key="job_posts",
        signal_label="Job Posts",
        value=float(len(jobs)),
        source="Greenhouse",
        notes="Nombre d'offres publiques ouvertes chez Duolingo.",
        sort_order=1,
    )


def _collect_wikipedia_pageviews(as_of_date: date) -> SignalReading | None:
    start = (as_of_date - timedelta(days=6)).strftime("%Y%m%d")
    end = as_of_date.strftime("%Y%m%d")
    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        "en.wikipedia.org/all-access/user/Duolingo/daily/"
        f"{start}/{end}"
    )
    payload = _safe_get_json(url)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return None
    views = [float(item.get("views", 0)) for item in items if item.get("views") is not None]
    if not views:
        return None
    return SignalReading(
        signal_key="wikipedia_pageviews_7d",
        signal_label="Wikipedia Pageviews (7j)",
        value=float(sum(views) / len(views)),
        source="Wikimedia",
        notes="Moyenne 7 jours des vues sur la page Wikipedia de Duolingo.",
        sort_order=2,
    )


def _collect_google_news_mentions(as_of_date: date) -> SignalReading | None:
    xml_text = _safe_get_text(
        "https://news.google.com/rss/search",
        params={
            "q": "Duolingo when:7d",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        },
    )
    if not xml_text:
        return None

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    items = root.findall(".//item")
    return SignalReading(
        signal_key="google_news_mentions_7d",
        signal_label="Google News Mentions (7j)",
        value=float(len(items)),
        source="Google News RSS",
        notes="Nombre d'articles Google News remontes sur les 7 derniers jours.",
        sort_order=3,
    )


def _collect_reddit_mentions(as_of_date: date) -> SignalReading | None:
    payload = _safe_get_json(
        "https://www.reddit.com/search.json",
        params={
            "q": "Duolingo",
            "sort": "new",
            "t": "week",
            "limit": 100,
            "raw_json": 1,
        },
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    children = data.get("children") if isinstance(data, dict) else None
    if not isinstance(children, list):
        return None
    return SignalReading(
        signal_key="reddit_mentions_7d",
        signal_label="Reddit Mentions (7j)",
        value=float(len(children)),
        source="Reddit",
        notes="Nombre de posts publics remontes par la recherche Reddit sur 7 jours (echantillon public, max 100).",
        sort_order=4,
    )


AUTO_COLLECTORS = [
    _collect_greenhouse_job_posts,
    _collect_wikipedia_pageviews,
    _collect_google_news_mentions,
    _collect_reddit_mentions,
]


def _load_manual_rows() -> list[SignalReading]:
    if not ALTERNATIVE_DATA_INPUT_FILE.exists():
        return []

    try:
        df = pd.read_csv(ALTERNATIVE_DATA_INPUT_FILE)
    except Exception:
        return []

    if df.empty:
        return []

    records: list[SignalReading] = []
    for row in df.to_dict("records"):
        enabled = str(row.get("enabled", "1")).strip().lower()
        if enabled in {"0", "false", "no", "non"}:
            continue

        signal_label = str(row.get("signal_label", "")).strip()
        value = _clean_numeric(row.get("value"))
        if not signal_label or value is None:
            continue

        sort_order = int(_clean_numeric(row.get("sort_order")) or 999)
        source = str(row.get("source", "Manuel")).strip() or "Manuel"
        notes = str(row.get("notes", "")).strip()
        signal_key = str(row.get("signal_key", "")).strip() or _normalize_key(signal_label)

        records.append(
            SignalReading(
                signal_key=signal_key,
                signal_label=signal_label,
                value=value,
                source=source,
                notes=notes,
                sort_order=sort_order,
                collection_mode="manual",
            )
        )

    return records


def collect_daily_alternative_data(as_of_date: object | None = None) -> list[SignalReading]:
    snapshot_date = _coerce_date(as_of_date)
    rows: list[SignalReading] = []

    for collector in AUTO_COLLECTORS:
        try:
            reading = collector(snapshot_date)
        except Exception:
            reading = None
        if reading is not None:
            rows.append(reading)

    rows.extend(_load_manual_rows())

    deduped: dict[str, SignalReading] = {}
    for row in rows:
        deduped[row.signal_key] = row

    return sorted(deduped.values(), key=lambda item: (item.sort_order, item.signal_label.lower()))


def _load_history_df() -> pd.DataFrame:
    if not ALTERNATIVE_DATA_HISTORY_FILE.exists():
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    try:
        df = pd.read_csv(ALTERNATIVE_DATA_HISTORY_FILE)
    except Exception:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    for column in HISTORY_COLUMNS:
        if column not in df.columns:
            df[column] = None

    df = df[HISTORY_COLUMNS].copy()
    if not df.empty:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
        df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def refresh_alternative_data_history(as_of_date: object | None = None) -> pd.DataFrame:
    snapshot_date = _coerce_date(as_of_date)
    current_rows = collect_daily_alternative_data(snapshot_date)
    history_df = _load_history_df()

    if not current_rows:
        return history_df

    today_df = pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date.isoformat(),
                "week_start": _week_start(snapshot_date).isoformat(),
                "week_label": _week_label(snapshot_date),
                "sort_order": row.sort_order,
                "signal_key": row.signal_key,
                "signal_label": row.signal_label,
                "value": row.value,
                "source": row.source,
                "notes": row.notes,
                "collection_mode": row.collection_mode,
            }
            for row in current_rows
        ]
    )

    if history_df.empty:
        updated = today_df.copy()
    else:
        history_df["snapshot_date"] = pd.to_datetime(history_df["snapshot_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        history_df["week_start"] = pd.to_datetime(history_df["week_start"], errors="coerce").dt.strftime("%Y-%m-%d")
        updated = pd.concat([history_df, today_df], ignore_index=True)
        updated = updated.drop_duplicates(subset=["snapshot_date", "signal_key"], keep="last")

    updated = updated.sort_values(["sort_order", "signal_label", "snapshot_date"])
    ALTERNATIVE_DATA_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    updated.to_csv(ALTERNATIVE_DATA_HISTORY_FILE, index=False)
    return updated


def _build_latest_signal_rows(history_df: pd.DataFrame) -> list[dict[str, object]]:
    if history_df.empty:
        return []

    df = history_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["snapshot_date", "signal_key"])

    rows: list[dict[str, object]] = []
    for signal_key, group in df.groupby("signal_key", sort=False):
        group = group.sort_values(["week_start", "snapshot_date"])
        current_row = group.iloc[-1]
        previous_group = group[group["week_start"] < current_row["week_start"]]
        previous_row = previous_group.iloc[-1] if not previous_group.empty else None

        current_value = _clean_numeric(current_row.get("value"))
        previous_value = _clean_numeric(previous_row.get("value")) if previous_row is not None else None

        wow_change_pct = None
        if current_value is not None and previous_value not in (None, 0):
            wow_change_pct = (current_value - previous_value) / abs(previous_value)

        rows.append(
            {
                "sort_order": int(_clean_numeric(current_row.get("sort_order")) or 999),
                "signal_key": signal_key,
                "signal_label": current_row.get("signal_label") or signal_key,
                "value": current_value,
                "value_display": _format_value(current_value),
                "wow_change_pct": wow_change_pct,
                "wow_display": _format_wow(wow_change_pct),
                "week_label": current_row.get("week_label") or "N/D",
                "source": current_row.get("source") or "N/D",
                "notes": current_row.get("notes") or "",
                "snapshot_date": current_row["snapshot_date"].strftime("%Y-%m-%d"),
            }
        )

    return sorted(rows, key=lambda row: (row["sort_order"], str(row["signal_label"]).lower()))


def generate_alternative_data_package(as_of_date: object | None = None) -> dict[str, object]:
    history_df = refresh_alternative_data_history(as_of_date)
    rows = _build_latest_signal_rows(history_df)

    if rows:
        latest_snapshot_date = max(row["snapshot_date"] for row in rows)
        latest_week_label = rows[0].get("week_label") or "N/D"
    else:
        latest_snapshot_date = None
        latest_week_label = None

    signals_up = sum(1 for row in rows if isinstance(row.get("wow_change_pct"), float) and row["wow_change_pct"] > 0)
    signals_down = sum(1 for row in rows if isinstance(row.get("wow_change_pct"), float) and row["wow_change_pct"] < 0)

    return {
        "metadata": {
            "has_data": bool(rows),
            "latest_snapshot_date": latest_snapshot_date,
            "latest_week_label": latest_week_label,
            "signal_count": len(rows),
            "signals_up": signals_up,
            "signals_down": signals_down,
        },
        "rows": rows,
        "history_rows": history_df.to_dict("records") if not history_df.empty else [],
    }


def build_alternative_data_raw_df(package: dict[str, object] | None) -> pd.DataFrame:
    if not package:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    history_rows = package.get("history_rows") or []
    if not history_rows:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    df = pd.DataFrame(history_rows)
    for column in HISTORY_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df[HISTORY_COLUMNS]
