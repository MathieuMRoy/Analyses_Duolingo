"""
Phase 1 financial signal layer.

This module transforms daily user-level behavioral data into:
- user and cohort features
- business aggregations
- explainable financial proxy signals
- a structured package that downstream agents can consume
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean

import pandas as pd

from .config import DAILY_LOG_FILE, REPORT_DIR, TARGET_USERS_FILE, now_toronto

LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]
LEGACY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus"]
COHORTS = ["Debutants", "Standard", "Super-Actifs"]

FINANCIAL_SIGNALS_JSON_FILE = REPORT_DIR / "financial_signals_latest.json"
FINANCIAL_SIGNALS_HISTORY_FILE = REPORT_DIR / "financial_signals_history.csv"


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"true", "1", "yes", "y", "vrai"}


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _safe_mean(values: list[float]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and pd.notna(v)]
    if not cleaned:
        return None
    return float(mean(cleaned))


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _pct_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value) * 100.0, digits)


def _scale_delta(delta: float | None, scale: float) -> float:
    if delta is None or pd.isna(delta):
        return 0.0
    if scale <= 0:
        return 0.0
    return max(-1.0, min(1.0, float(delta) / scale))


def _window_average(series: pd.Series, window: int) -> float | None:
    values = [float(v) for v in series.dropna().tolist()]
    if not values:
        return None
    if len(values) < window:
        return _safe_mean(values)
    return _safe_mean(values[-window:])


def _window_delta(series: pd.Series, window: int) -> float | None:
    values = [float(v) for v in series.dropna().tolist()]
    if len(values) < 2:
        return None
    if len(values) >= window * 2:
        recent = values[-window:]
        previous = values[-(window * 2):-window]
        return _safe_mean(recent) - _safe_mean(previous)

    recent = values[-min(window, len(values)):]
    previous = values[:-len(recent)]
    if not previous:
        return recent[-1] - values[0]
    return _safe_mean(recent) - _safe_mean(previous)


def _subscription_state(has_plus: bool, has_max: bool) -> str:
    if has_max:
        return "max"
    if has_plus:
        return "super"
    return "free"


def _load_target_panel_df() -> pd.DataFrame:
    if not TARGET_USERS_FILE.exists():
        return pd.DataFrame(columns=["Username", "Cohort"])

    try:
        df = pd.read_csv(TARGET_USERS_FILE)
    except Exception:
        return pd.DataFrame(columns=["Username", "Cohort"])

    if "Username" not in df.columns:
        return pd.DataFrame(columns=["Username", "Cohort"])

    df["Username"] = df["Username"].astype(str)
    if "Cohort" not in df.columns:
        df["Cohort"] = None

    return df.drop_duplicates(subset=["Username"]).reset_index(drop=True)


def _load_daily_log_df() -> pd.DataFrame:
    if not DAILY_LOG_FILE.exists():
        return pd.DataFrame(columns=LOG_COLUMNS)

    rows: list[dict[str, object]] = []
    with open(DAILY_LOG_FILE, "r", encoding="utf-8", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if header is None:
            return pd.DataFrame(columns=LOG_COLUMNS)

        for row in reader:
            if not row:
                continue
            if row == LOG_COLUMNS or row[:6] == LEGACY_LOG_COLUMNS:
                continue

            normalized = row[:]
            if len(normalized) == 6:
                normalized.append("False")
            elif len(normalized) > 7:
                normalized = normalized[:7]
            elif len(normalized) < 6:
                continue

            if len(normalized) < 7:
                normalized.extend([""] * (7 - len(normalized)))

            rows.append(dict(zip(LOG_COLUMNS, normalized)))

    df = pd.DataFrame(rows, columns=LOG_COLUMNS)
    if df.empty:
        return df

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Username"] = df["Username"].astype(str)
    df["Streak"] = pd.to_numeric(df["Streak"], errors="coerce").fillna(0)
    df["TotalXP"] = pd.to_numeric(df["TotalXP"], errors="coerce").fillna(0)
    df["HasPlus"] = df["HasPlus"].apply(_parse_bool)
    df["HasMax"] = df["HasMax"].apply(_parse_bool)

    df = df[df["Date"].notna()]
    df = df[~df["Username"].str.contains("Aggregated", na=False)]
    df = df[df["Cohort"] != "Global"]
    df = df.sort_values(["Date", "Username"]).drop_duplicates(
        subset=["Date", "Username"],
        keep="last",
    )

    return df.reset_index(drop=True)


def _build_daily_metrics(log_df: pd.DataFrame, target_panel_size: int) -> pd.DataFrame:
    if log_df.empty:
        return pd.DataFrame()

    metrics_rows: list[dict[str, object]] = []
    previous_df: pd.DataFrame | None = None

    for date_value in sorted(log_df["Date"].dropna().unique().tolist()):
        current_df = log_df[log_df["Date"] == date_value].copy()
        if current_df.empty:
            continue

        current_df["SubscriptionState"] = current_df.apply(
            lambda row: _subscription_state(bool(row["HasPlus"]), bool(row["HasMax"])),
            axis=1,
        )

        panel_total = len(current_df)
        active_users = int((current_df["Streak"] > 0).sum())
        active_rate = _safe_ratio(active_users, panel_total)
        avg_streak = float(current_df["Streak"].mean()) if panel_total else None
        super_users = int((current_df["SubscriptionState"] == "super").sum())
        max_users = int((current_df["SubscriptionState"] == "max").sum())
        paid_users = int((current_df["SubscriptionState"] != "free").sum())

        high_value_df = current_df[current_df["Cohort"] == "Super-Actifs"]
        high_value_active_rate = _safe_ratio(
            int((high_value_df["Streak"] > 0).sum()),
            len(high_value_df),
        )

        row: dict[str, object] = {
            "Date": date_value,
            "panel_observed": panel_total,
            "target_panel_size": target_panel_size,
            "panel_coverage_ratio": _safe_ratio(panel_total, target_panel_size),
            "active_users": active_users,
            "active_rate": active_rate,
            "avg_streak": avg_streak,
            "super_rate": _safe_ratio(super_users, panel_total),
            "max_rate": _safe_ratio(max_users, panel_total),
            "paid_rate": _safe_ratio(paid_users, panel_total),
            "high_value_active_rate": high_value_active_rate,
            "xp_delta_mean": None,
            "reactivated_users": None,
            "reactivation_rate": None,
            "churned_users": None,
            "churn_rate": None,
            "premium_net_adds_proxy": None,
            "max_net_adds_proxy": None,
            "free_to_super": None,
            "free_to_max": None,
            "super_to_free": None,
            "max_to_free": None,
            "super_to_max": None,
            "max_to_super": None,
            "high_value_retention_rate": None,
            "debutants_abandon_rate": None,
            "standard_abandon_rate": None,
            "super_actifs_abandon_rate": None,
        }

        if previous_df is not None:
            merged = previous_df.merge(
                current_df[
                    [
                        "Username",
                        "Cohort",
                        "Streak",
                        "TotalXP",
                        "SubscriptionState",
                    ]
                ],
                on="Username",
                how="inner",
                suffixes=("_prev", "_curr"),
            )

            if not merged.empty:
                xp_delta = (merged["TotalXP_curr"] - merged["TotalXP_prev"]).clip(lower=0)
                active_prev_mask = merged["Streak_prev"] > 0
                inactive_prev_mask = merged["Streak_prev"] == 0
                churn_mask = active_prev_mask & (merged["Streak_curr"] == 0)
                reactivation_mask = inactive_prev_mask & (merged["Streak_curr"] > 0)

                free_to_super = (
                    (merged["SubscriptionState_prev"] == "free")
                    & (merged["SubscriptionState_curr"] == "super")
                ).sum()
                free_to_max = (
                    (merged["SubscriptionState_prev"] == "free")
                    & (merged["SubscriptionState_curr"] == "max")
                ).sum()
                super_to_free = (
                    (merged["SubscriptionState_prev"] == "super")
                    & (merged["SubscriptionState_curr"] == "free")
                ).sum()
                max_to_free = (
                    (merged["SubscriptionState_prev"] == "max")
                    & (merged["SubscriptionState_curr"] == "free")
                ).sum()
                super_to_max = (
                    (merged["SubscriptionState_prev"] == "super")
                    & (merged["SubscriptionState_curr"] == "max")
                ).sum()
                max_to_super = (
                    (merged["SubscriptionState_prev"] == "max")
                    & (merged["SubscriptionState_curr"] == "super")
                ).sum()

                cohort_for_churn = "Cohort_curr" if "Cohort_curr" in merged.columns else "Cohort_prev"

                row.update(
                    {
                        "xp_delta_mean": float(xp_delta.mean()) if not xp_delta.empty else None,
                        "reactivated_users": int(reactivation_mask.sum()),
                        "reactivation_rate": _safe_ratio(
                            int(reactivation_mask.sum()),
                            int(inactive_prev_mask.sum()),
                        ),
                        "churned_users": int(churn_mask.sum()),
                        "churn_rate": _safe_ratio(
                            int(churn_mask.sum()),
                            int(active_prev_mask.sum()),
                        ),
                        "premium_net_adds_proxy": _safe_ratio(
                            int(free_to_super + free_to_max - super_to_free - max_to_free),
                            len(merged),
                        ),
                        "max_net_adds_proxy": _safe_ratio(
                            int(free_to_max + super_to_max - max_to_super - max_to_free),
                            len(merged),
                        ),
                        "free_to_super": int(free_to_super),
                        "free_to_max": int(free_to_max),
                        "super_to_free": int(super_to_free),
                        "max_to_free": int(max_to_free),
                        "super_to_max": int(super_to_max),
                        "max_to_super": int(max_to_super),
                    }
                )

                high_value_merged = merged[merged[cohort_for_churn] == "Super-Actifs"]
                if not high_value_merged.empty:
                    high_value_prev_active = int((high_value_merged["Streak_prev"] > 0).sum())
                    high_value_churn = int(
                        ((high_value_merged["Streak_prev"] > 0) & (high_value_merged["Streak_curr"] == 0)).sum()
                    )
                    row["high_value_retention_rate"] = (
                        _safe_ratio(high_value_prev_active - high_value_churn, high_value_prev_active)
                        if high_value_prev_active > 0
                        else None
                    )

                cohort_name_to_key = {
                    "Debutants": "debutants_abandon_rate",
                    "Standard": "standard_abandon_rate",
                    "Super-Actifs": "super_actifs_abandon_rate",
                }
                for cohort_name, output_key in cohort_name_to_key.items():
                    cohort_df = merged[merged[cohort_for_churn] == cohort_name]
                    prev_active = int((cohort_df["Streak_prev"] > 0).sum())
                    cohort_churn = int(
                        ((cohort_df["Streak_prev"] > 0) & (cohort_df["Streak_curr"] == 0)).sum()
                    )
                    row[output_key] = _safe_ratio(cohort_churn, prev_active)

        metrics_rows.append(row)
        previous_df = current_df[
            ["Username", "Cohort", "Streak", "TotalXP", "SubscriptionState"]
        ].copy()

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df["Date"] = pd.to_datetime(metrics_df["Date"], errors="coerce")
    return metrics_df.sort_values("Date").reset_index(drop=True)


def _build_engagement_quality_index(latest_row: pd.Series) -> float | None:
    active_rate = latest_row.get("active_rate")
    high_value_active_rate = latest_row.get("high_value_active_rate")
    avg_streak = latest_row.get("avg_streak")
    churn_rate = latest_row.get("churn_rate")

    if active_rate is None or pd.isna(active_rate):
        return None

    streak_strength = min(max((float(avg_streak or 0.0) / 365.0), 0.0), 1.0)
    high_value_component = float(high_value_active_rate) if pd.notna(high_value_active_rate) else float(active_rate)
    churn_component = 1.0 - float(churn_rate) if pd.notna(churn_rate) else 1.0

    score = (
        0.45 * float(active_rate)
        + 0.20 * high_value_component
        + 0.20 * streak_strength
        + 0.15 * max(0.0, churn_component)
    )
    return round(score * 100.0, 2)


def _derive_main_drivers(latest_row: pd.Series, metrics_df: pd.DataFrame) -> tuple[list[str], list[str]]:
    drivers: list[str] = []
    risks: list[str] = []

    premium_momentum_14d = _window_delta(metrics_df["super_rate"], 7)
    max_momentum_14d = _window_delta(metrics_df["max_rate"], 7)
    churn_trend_14d = _window_delta(metrics_df["churn_rate"], 7)
    reactivation_trend_7d = _window_delta(metrics_df["reactivation_rate"], 7)
    high_value_retention_trend = _window_delta(metrics_df["high_value_retention_rate"], 7)
    growth_acceleration_7d = _window_delta(metrics_df["xp_delta_mean"], 7)

    if premium_momentum_14d is not None and premium_momentum_14d > 0.0025:
        drivers.append("Le taux d'abonnement Super accelere sur la fenetre recente.")
    if max_momentum_14d is not None and max_momentum_14d > 0.001:
        drivers.append("La penetration Max montre un momentum positif, meme a bas niveau.")
    if churn_trend_14d is not None and churn_trend_14d < -0.005:
        drivers.append("Le taux d'abandon se detend par rapport a la fenetre precedente.")
    if reactivation_trend_7d is not None and reactivation_trend_7d > 0.005:
        drivers.append("Les reactivations se renforcent et soutiennent la retention nette.")
    if high_value_retention_trend is not None and high_value_retention_trend > 0.005:
        drivers.append("La retention des cohortes a forte valeur s'ameliore.")
    if growth_acceleration_7d is not None and growth_acceleration_7d > 5:
        drivers.append("L'intensite d'apprentissage accelere sur les derniers jours.")

    if premium_momentum_14d is not None and premium_momentum_14d < -0.0025:
        risks.append("Le taux d'abonnement Super ralentit sur la fenetre recente.")
    if max_momentum_14d is not None and max_momentum_14d < -0.001:
        risks.append("Le signal Max se deteriore, ce qui limite la lecture de monetisation premium.")
    if churn_trend_14d is not None and churn_trend_14d > 0.005:
        risks.append("Le taux d'abandon remonte par rapport a la fenetre precedente.")
    if reactivation_trend_7d is not None and reactivation_trend_7d < -0.005:
        risks.append("Les reactivations ralentissent sur la fenetre recente.")
    if high_value_retention_trend is not None and high_value_retention_trend < -0.005:
        risks.append("La retention de la cohorte Super-Actifs se tasse.")
    if growth_acceleration_7d is not None and growth_acceleration_7d < -5:
        risks.append("L'intensite d'apprentissage decelere, ce qui peut peser sur la monetisation.")

    if not drivers:
        drivers.append("Le signal est encore jeune; il faut plus d'historique pour faire ressortir un driver dominant.")
    if not risks:
        risks.append("Le risque principal reste la faible profondeur historique du panel sur certaines series.")

    return drivers[:4], risks[:4]


def _build_confidence_level(latest_row: pd.Series, metrics_df: pd.DataFrame) -> str:
    coverage_ratio = latest_row.get("panel_coverage_ratio")
    observed_days = len(metrics_df)

    if coverage_ratio is not None and pd.notna(coverage_ratio):
        coverage_ratio = float(coverage_ratio)
    else:
        coverage_ratio = 0.0

    if observed_days >= 30 and coverage_ratio >= 0.80:
        return "high"
    if observed_days >= 14 and coverage_ratio >= 0.55:
        return "medium"
    return "low"


def _build_signal_bias(latest_row: pd.Series, monetization_momentum_index: float | None, engagement_quality_index: float | None) -> str:
    churn_rate = latest_row.get("churn_rate")
    churn_rate = float(churn_rate) if churn_rate is not None and pd.notna(churn_rate) else 0.0

    if monetization_momentum_index is None or engagement_quality_index is None:
        return "neutral"

    if monetization_momentum_index >= 60 and engagement_quality_index >= 70 and churn_rate <= 0.04:
        return "favorable"
    if monetization_momentum_index <= 40 or engagement_quality_index <= 55 or churn_rate >= 0.08:
        return "unfavorable"
    return "neutral"


def _flatten_signal_package(signal_package: dict[str, object]) -> dict[str, object]:
    flat: dict[str, object] = {}

    def _flatten(prefix: str, value: object) -> None:
        if isinstance(value, dict):
            for key, nested_value in value.items():
                next_prefix = f"{prefix}_{key}" if prefix else str(key)
                _flatten(next_prefix, nested_value)
            return
        if isinstance(value, list):
            flat[prefix] = " | ".join(str(v) for v in value)
            return
        flat[prefix] = value

    _flatten("", signal_package)
    return flat


def build_financial_signal_sheet_df(signal_package: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    def _add(section: str, signal: str, value: object, definition: str) -> None:
        rows.append(
            {
                "Section": section,
                "Signal": signal,
                "Valeur": value,
                "Definition": definition,
            }
        )

    metadata = signal_package.get("metadata", {})
    panel = signal_package.get("panel", {})
    business = signal_package.get("business_signals", {})
    proxy = signal_package.get("financial_proxy_signals", {})
    model_ready = signal_package.get("model_readiness", {})

    _add("Meta", "as_of_date", metadata.get("as_of_date"), "Date de reference du paquet de signaux.")
    _add("Meta", "phase", metadata.get("phase"), "Phase actuelle du systeme de nowcasting.")
    _add("Panel", "target_panel_size", panel.get("target_panel_size"), "Taille totale du panel cible.")
    _add("Panel", "observed_users_today", panel.get("observed_users_today"), "Utilisateurs observes sur la date de reference.")
    _add("Panel", "coverage_ratio", panel.get("coverage_ratio"), "Part du panel effectivement observee aujourd'hui.")

    for key, definition in {
        "active_rate": "Part des utilisateurs observes avec une streak active.",
        "avg_streak": "Longueur moyenne de streak sur la date de reference.",
        "xp_delta_mean": "Croissance moyenne du XP contre la veille, borne a 0 si negative.",
        "reactivation_rate": "Part des inactifs de la veille redevenus actifs.",
        "churn_rate": "Part des actifs de la veille retombes a zero.",
        "super_rate": "Part du panel observe qui est en abonnement Super (hors Max).",
        "max_rate": "Part du panel observe qui est en abonnement Max.",
    }.items():
        _add("Business", key, business.get(key), definition)

    for key, definition in {
        "engagement_quality_index": "Indice composite explicable de qualite d'engagement.",
        "premium_momentum_14d": "Delta entre les 7 derniers jours et la fenetre precedente du taux Super.",
        "max_momentum_14d": "Delta entre les 7 derniers jours et la fenetre precedente du taux Max.",
        "churn_trend_14d": "Delta entre les 7 derniers jours et la fenetre precedente du churn.",
        "reactivation_trend_7d": "Variation recente du taux de reactivation.",
        "high_value_retention_trend": "Variation recente de la retention des Super-Actifs.",
        "subscription_momentum_proxy": "Proxy explicable de momentum de net adds premium.",
        "monetization_momentum_index": "Indice composite de momentum de monetisation.",
        "growth_acceleration_7d": "Acceleration recente de l'intensite d'apprentissage.",
        "signal_bias": "Lecture globale actuelle du signal.",
        "confidence_level": "Niveau de confiance selon couverture panel et profondeur historique.",
    }.items():
        _add("FinancialProxy", key, proxy.get(key), definition)

    _add(
        "ModelReadiness",
        "supervised_probabilities_ready",
        model_ready.get("supervised_probabilities_ready"),
        "Indique si les probabilites de beat/miss sont basees sur un vrai entrainement supervise.",
    )

    return pd.DataFrame(rows)


def build_financial_signal_package(reference_date: str | None = None) -> dict[str, object] | None:
    target_df = _load_target_panel_df()
    log_df = _load_daily_log_df()
    if log_df.empty:
        return None

    if reference_date is None:
        reference_date = now_toronto().strftime("%Y-%m-%d")

    metrics_df = _build_daily_metrics(log_df, len(target_df))
    if metrics_df.empty:
        return None

    latest_metrics_df = metrics_df[metrics_df["Date"] == pd.to_datetime(reference_date)]
    if latest_metrics_df.empty:
        latest_metrics_df = metrics_df.tail(1)

    latest_row = latest_metrics_df.iloc[-1]

    engagement_quality_index = _build_engagement_quality_index(latest_row)
    engagement_quality_trend = _window_delta(metrics_df["active_rate"], 7)
    premium_momentum_14d = _window_delta(metrics_df["super_rate"], 7)
    max_momentum_14d = _window_delta(metrics_df["max_rate"], 7)
    churn_trend_14d = _window_delta(metrics_df["churn_rate"], 7)
    reactivation_trend_7d = _window_delta(metrics_df["reactivation_rate"], 7)
    high_value_retention_trend = _window_delta(metrics_df["high_value_retention_rate"], 7)
    premium_net_adds_proxy_7d = _window_average(metrics_df["premium_net_adds_proxy"], 7)
    max_net_adds_proxy_7d = _window_average(metrics_df["max_net_adds_proxy"], 7)
    growth_acceleration_7d = _window_delta(metrics_df["xp_delta_mean"], 7)

    subscription_momentum_proxy = _safe_mean(
        [
            premium_net_adds_proxy_7d,
            max_net_adds_proxy_7d,
            premium_momentum_14d,
            max_momentum_14d,
        ]
    )

    monetization_momentum_index = round(
        50.0
        + 50.0
        * (
            0.40 * _scale_delta(premium_momentum_14d, 0.02)
            + 0.20 * _scale_delta(max_momentum_14d, 0.01)
            + 0.25 * _scale_delta(subscription_momentum_proxy, 0.01)
            + 0.15 * _scale_delta(high_value_retention_trend, 0.02)
        ),
        2,
    )

    main_drivers, main_risks = _derive_main_drivers(latest_row, metrics_df)
    confidence_level = _build_confidence_level(latest_row, metrics_df)
    signal_bias = _build_signal_bias(latest_row, monetization_momentum_index, engagement_quality_index)

    package: dict[str, object] = {
        "metadata": {
            "phase": "phase_1_feature_and_proxy_layer",
            "as_of_date": latest_row["Date"].strftime("%Y-%m-%d"),
            "observed_days": int(len(metrics_df)),
            "source": "daily_streaks_log.csv + target_users.csv",
        },
        "panel": {
            "target_panel_size": int(len(target_df)),
            "observed_users_today": int(latest_row.get("panel_observed") or 0),
            "coverage_ratio": _round_or_none(latest_row.get("panel_coverage_ratio")),
        },
        "business_signals": {
            "active_users": int(latest_row.get("active_users") or 0),
            "active_rate": _round_or_none(latest_row.get("active_rate")),
            "avg_streak": _round_or_none(latest_row.get("avg_streak"), 2),
            "xp_delta_mean": _round_or_none(latest_row.get("xp_delta_mean"), 2),
            "reactivated_users": int(latest_row.get("reactivated_users") or 0) if pd.notna(latest_row.get("reactivated_users")) else None,
            "reactivation_rate": _round_or_none(latest_row.get("reactivation_rate")),
            "churned_users": int(latest_row.get("churned_users") or 0) if pd.notna(latest_row.get("churned_users")) else None,
            "churn_rate": _round_or_none(latest_row.get("churn_rate")),
            "super_rate": _round_or_none(latest_row.get("super_rate")),
            "max_rate": _round_or_none(latest_row.get("max_rate")),
            "high_value_active_rate": _round_or_none(latest_row.get("high_value_active_rate")),
            "high_value_retention_rate": _round_or_none(latest_row.get("high_value_retention_rate")),
            "debutants_abandon_rate": _round_or_none(latest_row.get("debutants_abandon_rate")),
            "standard_abandon_rate": _round_or_none(latest_row.get("standard_abandon_rate")),
            "super_actifs_abandon_rate": _round_or_none(latest_row.get("super_actifs_abandon_rate")),
            "free_to_super": int(latest_row.get("free_to_super") or 0) if pd.notna(latest_row.get("free_to_super")) else None,
            "free_to_max": int(latest_row.get("free_to_max") or 0) if pd.notna(latest_row.get("free_to_max")) else None,
            "super_to_max": int(latest_row.get("super_to_max") or 0) if pd.notna(latest_row.get("super_to_max")) else None,
            "super_to_free": int(latest_row.get("super_to_free") or 0) if pd.notna(latest_row.get("super_to_free")) else None,
            "max_to_free": int(latest_row.get("max_to_free") or 0) if pd.notna(latest_row.get("max_to_free")) else None,
        },
        "financial_proxy_signals": {
            "engagement_quality_index": engagement_quality_index,
            "engagement_quality_trend": _round_or_none(engagement_quality_trend),
            "premium_momentum_14d": _round_or_none(premium_momentum_14d),
            "max_momentum_14d": _round_or_none(max_momentum_14d),
            "churn_trend_14d": _round_or_none(churn_trend_14d),
            "reactivation_trend_7d": _round_or_none(reactivation_trend_7d),
            "high_value_retention_trend": _round_or_none(high_value_retention_trend),
            "premium_net_adds_proxy_7d": _round_or_none(premium_net_adds_proxy_7d),
            "max_net_adds_proxy_7d": _round_or_none(max_net_adds_proxy_7d),
            "subscription_momentum_proxy": _round_or_none(subscription_momentum_proxy),
            "monetization_momentum_index": monetization_momentum_index,
            "growth_acceleration_7d": _round_or_none(growth_acceleration_7d, 2),
            "signal_bias": signal_bias,
            "confidence_level": confidence_level,
            "main_drivers": main_drivers,
            "main_risks": main_risks,
            "revenue_beat_probability": None,
            "ebitda_beat_probability": None,
            "guidance_raise_probability": None,
        },
        "model_readiness": {
            "supervised_probabilities_ready": False,
            "next_step": "Add quarterly labels and consensus history before training beat/miss models.",
        },
        "assumptions": [
            "Panel behavior is a sample-based proxy, not Duolingo's full user base.",
            "Super is measured as HasPlus excluding HasMax, while Max depends on profile-level Max detection.",
            "Current outputs are explainable proxy signals, not supervised beat/miss probabilities yet.",
        ],
    }

    return package


def save_financial_signal_package(signal_package: dict[str, object]) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    FINANCIAL_SIGNALS_JSON_FILE.write_text(
        json.dumps(signal_package, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    flat_row = _flatten_signal_package(signal_package)
    history_row = pd.DataFrame([flat_row])

    if FINANCIAL_SIGNALS_HISTORY_FILE.exists():
        try:
            existing = pd.read_csv(FINANCIAL_SIGNALS_HISTORY_FILE)
        except Exception:
            existing = pd.DataFrame()
        if not existing.empty and "metadata_as_of_date" in existing.columns:
            existing = existing[existing["metadata_as_of_date"] != flat_row.get("metadata_as_of_date")]
            history_row = pd.concat([existing, history_row], ignore_index=True)

    history_row.to_csv(FINANCIAL_SIGNALS_HISTORY_FILE, index=False, encoding="utf-8")
    return FINANCIAL_SIGNALS_JSON_FILE, FINANCIAL_SIGNALS_HISTORY_FILE


def generate_financial_signal_package(reference_date: str | None = None) -> dict[str, object] | None:
    signal_package = build_financial_signal_package(reference_date=reference_date)
    if not signal_package:
        return None
    save_financial_signal_package(signal_package)
    return signal_package
