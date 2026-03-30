"""
Alternative data collection, history persistence, and week-over-week packaging.

The goal of this module is to keep a lightweight set of free or manual external
signals that can be snapshotted every day alongside the Duolingo user panel.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
import math
import re
import subprocess
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from .config import ALTERNATIVE_DATA_HISTORY_FILE, ALTERNATIVE_DATA_INPUT_FILE

try:
    from pytrends.request import TrendReq
except Exception:  # pragma: no cover - optional dependency at runtime
    TrendReq = None


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


def _parse_compact_number(text: str | None) -> float | None:
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    normalized = (
        raw.replace("\u202f", " ")
        .replace("\xa0", " ")
        .replace(" ", " ")
        .strip()
    )
    lower = normalized.lower()

    word_match = re.search(
        r"([-+]?\d+(?:[.,]\d+)?)\s*(k|m|b|mille|thousand|million|millions|milliard|milliards|billion|billions)\b",
        lower,
        flags=re.IGNORECASE,
    )
    if word_match:
        number = float(word_match.group(1).replace(",", "."))
        suffix = word_match.group(2).lower()
        multipliers = {
            "k": 1_000.0,
            "m": 1_000_000.0,
            "b": 1_000_000_000.0,
            "mille": 1_000.0,
            "thousand": 1_000.0,
            "million": 1_000_000.0,
            "millions": 1_000_000.0,
            "milliard": 1_000_000_000.0,
            "milliards": 1_000_000_000.0,
            "billion": 1_000_000_000.0,
            "billions": 1_000_000_000.0,
        }
        multiplier = multipliers.get(suffix)
        if multiplier is not None:
            return number * multiplier

    raw = normalized.upper().replace(",", "").replace(" ", "")
    multiplier = 1.0
    if raw.endswith("K"):
        multiplier = 1_000.0
        raw = raw[:-1]
    elif raw.endswith("M"):
        multiplier = 1_000_000.0
        raw = raw[:-1]
    elif raw.endswith("B"):
        multiplier = 1_000_000_000.0
        raw = raw[:-1]

    try:
        return float(raw) * multiplier
    except ValueError:
        return None


def _safe_get_text_via_curl(url: str, *, user_agent: str | None = None) -> str | None:
    agent = user_agent or USER_AGENT
    for executable in ("curl.exe", "curl"):
        try:
            response = subprocess.run(
                [executable, "-L", "--max-time", "25", "-A", agent, url],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=30,
                check=False,
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            continue

        if response.returncode == 0 and response.stdout:
            return response.stdout

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


@lru_cache(maxsize=8)
def _fetch_socialblade_page(url: str) -> str | None:
    return _safe_get_text(url)


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


@lru_cache(maxsize=1)
def _fetch_ios_lookup() -> dict | None:
    payload = _safe_get_json(
        "https://itunes.apple.com/lookup",
        params={"id": "570060128", "country": "us"},
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None
    return results[0]


def _collect_google_trends(as_of_date: date) -> SignalReading | None:
    if TrendReq is None:
        return None

    try:
        trend = TrendReq(hl="en-US", tz=360)
        trend.build_payload(["Duolingo"], timeframe="today 3-m")
        df = trend.interest_over_time()
    except Exception:
        return None

    if df is None or df.empty or "Duolingo" not in df.columns:
        return None

    series = pd.to_numeric(df["Duolingo"], errors="coerce").dropna()
    if series.empty:
        return None

    value = float(series.iloc[-1])
    return SignalReading(
        signal_key="google_trends",
        signal_label="Google Trends",
        value=value,
        source="Google Trends",
        notes="Indice d'interet de recherche Duolingo sur une base 0-100.",
        sort_order=2,
    )


def _collect_ios_rating_score(as_of_date: date) -> SignalReading | None:
    app_data = _fetch_ios_lookup()
    value = _clean_numeric(app_data.get("averageUserRating")) if isinstance(app_data, dict) else None
    if value is None:
        return None
    return SignalReading(
        signal_key="ios_rating_score",
        signal_label="iOS Rating",
        value=value,
        source="Apple App Store",
        notes="Note moyenne iOS publique, utile comme proxy faible de satisfaction.",
        sort_order=3,
    )


def _extract_socialblade_metric(page_text: str | None, metric_name: str) -> float | None:
    if not page_text:
        return None

    patterns = [
        rf"{metric_name}\s*</[^>]+>\s*<[^>]+>\s*([\d.,]+[KMB]?)",
        rf"{metric_name}\s+([\d.,]+[KMB]?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, page_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = _parse_compact_number(match.group(1))
            if value is not None:
                return value
    return None


def _collect_instagram_followers(as_of_date: date) -> SignalReading | None:
    page_text = _safe_get_text("https://www.instagram.com/duolingo/")
    value = None
    source = "Instagram public"
    notes = "Audience Instagram publique de Duolingo."

    texts_to_try = [page_text] if page_text else []
    curl_text = _safe_get_text_via_curl("https://www.instagram.com/duolingo/", user_agent="Mozilla/5.0")
    if curl_text and curl_text != page_text:
        texts_to_try.append(curl_text)

    patterns = [
        (r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)', "exact"),
        (r'content="([\d.,]+[KMB]?) Followers,\s*[\d.,]+ Following,\s*[\d.,]+ Posts', "compact"),
        (r'content="([\d.,]+[KMB]?)\s+Followers', "compact"),
    ]
    for text in texts_to_try:
        for pattern, mode in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            if mode == "exact":
                value = _clean_numeric(match.group(1))
                notes = "Audience Instagram publique de Duolingo, lue depuis la page officielle."
            else:
                value = _parse_compact_number(match.group(1))
                notes = "Audience Instagram publique de Duolingo, lue depuis la meta description publique (valeur arrondie)."
            if value is not None:
                break
        if value is not None:
            break

    if value is None:
        page_text = _fetch_socialblade_page("https://socialblade.com/instagram/user/duolingo")
        value = _extract_socialblade_metric(page_text, "followers")
        source = "SocialBlade / Instagram"
        notes = "Audience Instagram publique de Duolingo via fallback externe."

    if value is None:
        return None
    return SignalReading(
        signal_key="instagram_followers",
        signal_label="Instagram Followers",
        value=value,
        source=source,
        notes=notes,
        sort_order=6,
    )


def _collect_tiktok_followers(as_of_date: date) -> SignalReading | None:
    page_text = _safe_get_text("https://www.tiktok.com/@duolingo")
    value = None

    if page_text:
        patterns = [
            r'"statsV2"\s*:\s*\{[^}]*"followerCount"\s*:\s*"(\d+)"',
            r'"stats"\s*:\s*\{[^}]*"followerCount"\s*:\s*(\d+)',
            r'@duolingo\s+([\d.,]+[KMB]?)\s+Followers',
        ]
        for pattern in patterns:
            match = re.search(pattern, page_text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            if pattern.endswith("Followers"):
                value = _parse_compact_number(match.group(1))
            else:
                value = _clean_numeric(match.group(1))
            if value is not None:
                break

    if value is None:
        page_text = _fetch_socialblade_page("https://socialblade.com/tiktok/user/duolingo")
        value = _extract_socialblade_metric(page_text, "followers")

    if value is None:
        return None
    return SignalReading(
        signal_key="tiktok_followers",
        signal_label="TikTok Followers",
        value=value,
        source="TikTok public",
        notes="Audience TikTok publique de Duolingo, lue sur la page officielle avec fallback si necessaire.",
        sort_order=7,
    )


def _collect_youtube_subscribers(as_of_date: date) -> SignalReading | None:
    page_text = _safe_get_text("https://www.youtube.com/@duolingo")
    value = None
    source = "YouTube public"
    notes = "Abonnes YouTube publics de Duolingo."

    if page_text:
        api_key_match = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', page_text)
        browse_id_match = re.search(r'"externalId":"([^"]+)"', page_text)
        if api_key_match and browse_id_match:
            try:
                response = HTTP.post(
                    f"https://www.youtube.com/youtubei/v1/browse?key={api_key_match.group(1)}",
                    json={
                        "context": {
                            "client": {
                                "clientName": "WEB",
                                "clientVersion": "2.20260325.08.00",
                                "hl": "fr",
                                "gl": "US",
                            }
                        },
                        "browseId": browse_id_match.group(1),
                    },
                    timeout=HTTP_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
                metadata_rows = (
                    payload.get("header", {})
                    .get("pageHeaderRenderer", {})
                    .get("content", {})
                    .get("pageHeaderViewModel", {})
                    .get("metadata", {})
                    .get("contentMetadataViewModel", {})
                    .get("metadataRows", [])
                )
                for row in metadata_rows:
                    for part in row.get("metadataParts", []):
                        text_obj = part.get("text", {})
                        candidates = [
                            text_obj.get("content"),
                            text_obj.get("accessibilityLabel"),
                        ]
                        for candidate in candidates:
                            candidate_value = _parse_compact_number(candidate)
                            if candidate_value is not None and (
                                "abonn" in str(candidate).lower() or "subscriber" in str(candidate).lower()
                            ):
                                value = candidate_value
                                notes = "Abonnes YouTube publics de Duolingo, lus depuis le header officiel du canal."
                                break
                        if value is not None:
                            break
                    if value is not None:
                        break
            except Exception:
                value = None

    if value is None and page_text:
        patterns = [
            r'"subscriberCountText"\s*:\s*\{.*?"simpleText"\s*:\s*"([\d.,]+[KMB]?)\s+(?:subscribers|abonnés?)"',
            r'content="[^"]*?([\d.,]+[KMB]?)\s+(?:subscribers|abonnés?)[^"]*?"\s+itemprop="description"',
        ]
        for pattern in patterns:
            match = re.search(pattern, page_text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            value = _parse_compact_number(match.group(1))
            if value is not None:
                notes = "Abonnes YouTube publics de Duolingo, lus sur la page officielle (valeur potentiellement arrondie)."
                break

    if value is None:
        page_text = _fetch_socialblade_page("https://socialblade.com/youtube/handle/duolingo")
        value = _extract_socialblade_metric(page_text, "subscribers")
        source = "SocialBlade / YouTube"
        notes = "Abonnes YouTube publics de Duolingo via fallback externe."

    if value is None:
        return None
    return SignalReading(
        signal_key="youtube_subscribers",
        signal_label="YouTube Subscribers",
        value=value,
        source=source,
        notes=notes,
        sort_order=8,
    )


def _collect_reddit_mentions(as_of_date: date) -> SignalReading | None:
    xml_text = _safe_get_text(
        "https://www.reddit.com/search.rss",
        params={
            "q": "Duolingo",
            "sort": "new",
            "t": "week",
        },
    )
    if not xml_text:
        return None

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    return SignalReading(
        signal_key="reddit_mentions_7d",
        signal_label="Reddit Mentions (7j)",
        value=float(len(entries)),
        source="Reddit",
        notes="Nombre de resultats publics Reddit sur 7 jours via le flux RSS de recherche.",
        sort_order=5,
    )


AUTO_COLLECTORS = [
    _collect_greenhouse_job_posts,
    _collect_google_trends,
    _collect_ios_rating_score,
    _collect_reddit_mentions,
    _collect_instagram_followers,
    _collect_tiktok_followers,
    _collect_youtube_subscribers,
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

    latest_snapshot = df["snapshot_date"].max()
    if pd.isna(latest_snapshot):
        return []

    current_snapshot_df = df[df["snapshot_date"] == latest_snapshot].copy()
    current_keys = set(current_snapshot_df["signal_key"].dropna().astype(str))

    rows: list[dict[str, object]] = []
    for signal_key, group in df.groupby("signal_key", sort=False):
        if str(signal_key) not in current_keys:
            continue
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
