"""
Helpers for subscription-tier detection.

Current limitation:
- the public Duolingo profile endpoint reliably exposes `hasPlus`
- but does not reliably expose a distinct `hasMax`

Because of that, `HasMax` must be treated as tri-state:
- True: explicitly confirmed as Max
- False: explicitly confirmed as not Max
- None: unknown / not observable from the endpoint
"""
from __future__ import annotations

import pandas as pd

from .config import BASE_DIR

MAX_OVERRIDES_FILE = BASE_DIR / "max_overrides.csv"
MAX_OVERRIDE_COLUMNS = ["Username", "HasMax", "StartDate", "EndDate", "Source", "Notes"]


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"true", "1", "yes", "y", "vrai"}


def parse_optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    raw = str(value).strip().lower()
    if raw in {"", "n/d", "na", "n/a", "none", "null", "unknown", "inconnu"}:
        return None
    if raw in {"true", "1", "yes", "y", "vrai"}:
        return True
    if raw in {"false", "0", "no", "n", "faux"}:
        return False
    return None


def serialize_optional_bool(value: object) -> str:
    parsed = parse_optional_bool(value)
    if parsed is None:
        return ""
    return "True" if parsed else "False"


def detect_has_max_from_user_payload(user: dict) -> bool | None:
    if not isinstance(user, dict):
        return None

    if "hasMax" in user:
        return parse_optional_bool(user.get("hasMax"))

    tier_raw = user.get("subscriptionTier")
    if tier_raw is not None:
        tier = str(tier_raw).strip().lower()
        if tier == "max":
            return True
        if tier:
            return False

    return None


def has_reliable_max_detection(series: pd.Series | list[object]) -> bool:
    values = pd.Series(series, dtype="object")
    if values.empty:
        return False
    return bool(values.notna().all())


def compute_super_observable_count(
    has_plus_series: pd.Series | list[object],
    has_max_series: pd.Series | list[object] | None = None,
) -> int:
    has_plus = pd.Series(has_plus_series, dtype="object").apply(parse_bool)
    if has_max_series is not None and has_reliable_max_detection(has_max_series):
        has_max = pd.Series(has_max_series, dtype="object").apply(parse_optional_bool).fillna(False)
        return int((has_plus & ~has_max.astype(bool)).sum())
    return int(has_plus.sum())


def compute_max_count(has_max_series: pd.Series | list[object] | None) -> int | None:
    if has_max_series is None or not has_reliable_max_detection(has_max_series):
        return None
    has_max = pd.Series(has_max_series, dtype="object").apply(parse_optional_bool).fillna(False)
    return int(has_max.astype(bool).sum())


def _normalize_overrides_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in MAX_OVERRIDE_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    normalized["Username"] = normalized["Username"].astype(str).str.strip().str.lower()
    normalized["HasMax"] = normalized["HasMax"].apply(parse_optional_bool)
    normalized["StartDate"] = pd.to_datetime(normalized["StartDate"], errors="coerce")
    normalized["EndDate"] = pd.to_datetime(normalized["EndDate"], errors="coerce")
    normalized = normalized[normalized["Username"] != ""]
    normalized = normalized[normalized["HasMax"].notna()]
    return normalized[MAX_OVERRIDE_COLUMNS].reset_index(drop=True)


def load_max_overrides_df() -> pd.DataFrame:
    if not MAX_OVERRIDES_FILE.exists():
        return pd.DataFrame(columns=MAX_OVERRIDE_COLUMNS)

    try:
        df = pd.read_csv(MAX_OVERRIDES_FILE)
    except Exception:
        return pd.DataFrame(columns=MAX_OVERRIDE_COLUMNS)

    if df.empty:
        return pd.DataFrame(columns=MAX_OVERRIDE_COLUMNS)
    return _normalize_overrides_df(df)


def apply_max_overrides(
    df: pd.DataFrame,
    *,
    username_column: str = "Username",
    date_column: str = "Date",
    target_column: str = "HasMax",
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    overrides_df = load_max_overrides_df()
    if overrides_df.empty:
        return df

    result = df.copy()
    if target_column not in result.columns:
        result[target_column] = None

    username_key = "__username_key"
    date_key = "__date_key"
    result[username_key] = result[username_column].astype(str).str.strip().str.lower()
    result[date_key] = pd.to_datetime(result[date_column], errors="coerce")

    for _, override in overrides_df.iterrows():
        mask = result[username_key] == override["Username"]
        start_date = override.get("StartDate")
        end_date = override.get("EndDate")

        if pd.notna(start_date):
            mask &= result[date_key] >= start_date
        if pd.notna(end_date):
            mask &= result[date_key] <= end_date

        result.loc[mask, target_column] = override["HasMax"]

    return result.drop(columns=[username_key, date_key], errors="ignore")
