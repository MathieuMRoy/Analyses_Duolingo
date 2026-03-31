"""
Phase 2 quarterly nowcast layer.

Transforms the daily financial signal history into:
- quarter-level pre-earnings snapshots
- explainable implied probabilities
- an investor-oriented quarterly nowcast package
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .config import BASE_DIR, REPORT_DIR, now_toronto
from .financial_signals import FINANCIAL_SIGNALS_HISTORY_FILE
from .valuation_dcf import _extract_latest_balance_and_cashflow_context

QUARTERLY_LABELS_FILE = BASE_DIR / "financial_docs" / "quarterly_labels_template.csv"
QUARTERLY_NOWCAST_JSON_FILE = REPORT_DIR / "quarterly_nowcast_latest.json"
QUARTERLY_SNAPSHOTS_FILE = REPORT_DIR / "quarterly_nowcast_snapshots.csv"


def _safe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: float | None, digits: int = 6) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _clip(value: float | None, lower: float = -1.0, upper: float = 1.0) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return max(lower, min(upper, float(value)))


def _quarter_start_end(quarter: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    try:
        period = pd.Period(quarter, freq="Q")
    except Exception:
        return None, None
    return period.start_time.normalize(), period.end_time.normalize()


def _quarter_from_date(value: object) -> str | None:
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return str(ts.to_period("Q"))


def _sort_quarters(values: list[str]) -> list[str]:
    return sorted(
        [value for value in values if value],
        key=lambda value: pd.Period(value, freq="Q"),
    )


def _previous_quarter(quarter: str) -> str | None:
    try:
        period = pd.Period(quarter, freq="Q")
    except Exception:
        return None
    return str(period - 1)


def _format_pct_text(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    return f"{float(value) * 100:.{digits}f}%".replace(".", ",")


def _format_number_text(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    formatted = f"{float(value):,.{digits}f}"
    return formatted.replace(",", " ").replace(".", ",")


def _stringify_list(value: object) -> str:
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "[]"
    if isinstance(value, str) and value.strip().startswith("["):
        return value
    return json.dumps([str(value)], ensure_ascii=False)


def _parse_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    raw = str(value).strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except Exception:
            pass
    return [item.strip() for item in raw.split(" | ") if item.strip()]


def _load_saved_snapshots_df() -> pd.DataFrame:
    if not QUARTERLY_SNAPSHOTS_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(QUARTERLY_SNAPSHOTS_FILE)
    if df.empty:
        return df

    for column in [
        "snapshot_as_of_date",
        "quarter_start",
        "quarter_end_observed",
        "earnings_release_date",
    ]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    if "snapshot_locked" in df.columns:
        df["snapshot_locked"] = (
            df["snapshot_locked"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "oui"})
        )
    else:
        df["snapshot_locked"] = False

    for column in ["main_drivers", "main_risks"]:
        if column in df.columns:
            df[column] = df[column].apply(_parse_list)

    return df


def _serialize_snapshots_df(df: pd.DataFrame) -> pd.DataFrame:
    serialized = df.copy()
    for column in ["main_drivers", "main_risks"]:
        if column in serialized.columns:
            serialized[column] = serialized[column].apply(_stringify_list)

    for column in [
        "snapshot_as_of_date",
        "quarter_start",
        "quarter_end_observed",
        "earnings_release_date",
    ]:
        if column in serialized.columns:
            serialized[column] = pd.to_datetime(serialized[column], errors="coerce").dt.strftime("%Y-%m-%d")

    if "snapshot_locked" in serialized.columns:
        serialized["snapshot_locked"] = serialized["snapshot_locked"].fillna(False).astype(bool)

    return serialized


def _snapshot_status_label(snapshot_locked: object) -> str:
    return "Figé" if bool(snapshot_locked) else "En cours"


def _build_snapshot_summary_text(snapshot: dict[str, object]) -> str:
    revenue_prob = _safe_float(
        snapshot.get("revenue_beat_probability", snapshot.get("revenue_beat_probability_proxy"))
    )
    guidance_prob = _safe_float(
        snapshot.get("guidance_raise_probability", snapshot.get("guidance_raise_probability_proxy"))
    )
    revenue_reference = _safe_float(snapshot.get("revenue_guidance_reference_musd"))
    drivers = snapshot.get("main_drivers") or []
    risks = snapshot.get("main_risks") or []
    primary_driver = str(drivers[0]).lstrip("- ").rstrip(".") if drivers else "la dynamique récente du panel"
    primary_risk = str(risks[0]).lstrip("- ").rstrip(".") if risks else "les limites de calibration actuelles"
    bias_label = str(snapshot.get("quarter_signal_bias") or "Neutre")

    if revenue_reference:
        revenue_reference_label = f"la guidance revenus ({_format_number_text(revenue_reference, 1)} M$)"
    else:
        revenue_reference_label = "la trajectoire de revenus de référence"

    if bias_label == "Favorable":
        return (
            "Le modèle trimestriel s'appuie sur la monétisation, l'engagement, la rétention, "
            "les réactivations et le churn observés dans le panel. "
            f"La probabilité implicite de battre les revenus du trimestre ressort à {_format_pct_text(revenue_prob)} et reste évaluée par rapport à {revenue_reference_label}. "
            f"La probabilité implicite de relever la guidance ressort à {_format_pct_text(guidance_prob)}, "
            f"soutenue notamment par {primary_driver}. "
        )

    return (
        "Le modèle trimestriel s'appuie sur la monétisation, l'engagement, la rétention, "
        "les réactivations et le churn observés dans le panel. "
        f"La probabilité implicite de battre les revenus du trimestre ressort à {_format_pct_text(revenue_prob)} et reste évaluée par rapport à {revenue_reference_label}. "
        f"La probabilité implicite de relever la guidance ressort à {_format_pct_text(guidance_prob)} et demeure freinée notamment par {primary_risk}."
    )


def _build_confidence_context_text(snapshot: dict[str, object]) -> str:
    confidence_label = str(snapshot.get("confidence_level") or "Faible")
    observed_days = int(_safe_float(snapshot.get("observed_days")) or 0)
    coverage_ratio = _safe_float(snapshot.get("avg_coverage_ratio"))
    guidance_reference = _safe_float(snapshot.get("revenue_guidance_reference_musd"))
    snapshot_locked = bool(snapshot.get("snapshot_locked"))

    coverage_text = _format_pct_text(coverage_ratio)
    if confidence_label == "Elevee":
        first_sentence = (
            f"Confiance elevee : {observed_days} jours de nowcast observes et une couverture moyenne de {coverage_text} donnent une lecture deja dense."
        )
    elif confidence_label == "Moyenne":
        first_sentence = (
            f"Confiance moyenne : {observed_days} jours de nowcast observes et une couverture moyenne de {coverage_text} suffisent pour lire la direction, pas encore pour surinterpreter le signal."
        )
    else:
        first_sentence = (
            f"Confiance faible : le trimestre reste encore jeune ({observed_days} jours de nowcast observes) meme si la couverture moyenne atteint {coverage_text}."
        )

    second_parts: list[str] = []
    if guidance_reference is not None:
        second_parts.append(
            f"Le guidance revenus ({_format_number_text(guidance_reference, 1)} M$) donne deja un point de comparaison exploitable"
        )
    else:
        second_parts.append("Le benchmark guidance revenus reste encore incomplet")

    if snapshot_locked:
        second_parts.append("le snapshot est fige")
    else:
        second_parts.append("le signal peut encore bouger jusqu'a la cloture du trimestre")

    return first_sentence + " " + ", et ".join(second_parts) + "."


def _format_estimation_note(estimate: float | None, benchmark: float | None, *, prefix: str, benchmark_label: str) -> str:
    if estimate is None and benchmark is None:
        return "N/D"
    estimate_text = f"{prefix} {_format_number_text(estimate, 1)} M$" if estimate is not None else f"{prefix} N/D"
    benchmark_text = (
        f"{benchmark_label} {_format_number_text(benchmark, 1)} M$" if benchmark is not None else f"{benchmark_label} N/D"
    )
    return f"{estimate_text} vs {benchmark_text}"


def _mean(series: pd.Series, window: int | None = None) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    if window and len(cleaned) > window:
        cleaned = cleaned.tail(window)
    return float(cleaned.mean())


def _latest(series: pd.Series) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    return float(cleaned.iloc[-1])


def _label_signal(value: float | None) -> str:
    if value is None:
        return "Neutral"
    if value >= 60:
        return "Favorable"
    if value <= 40:
        return "Defavorable"
    return "Neutre"


def _label_confidence(coverage_ratio: float | None, observed_days: int) -> str:
    if coverage_ratio is None:
        return "Faible"
    if coverage_ratio >= 0.75 and observed_days >= 20:
        return "Elevee"
    if coverage_ratio >= 0.45 and observed_days >= 10:
        return "Moyenne"
    return "Faible"


def _prob_from_score(score: float | None, confidence_label: str) -> float | None:
    if score is None:
        return None
    base_prob = 0.10 + 0.80 * (float(score) / 100.0)
    shrink = {
        "Elevee": 1.0,
        "Moyenne": 0.7,
        "Faible": 0.45,
    }.get(confidence_label, 0.45)
    adjusted = 0.50 + (base_prob - 0.50) * shrink
    return round(adjusted, 4)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(pd.Series(values, dtype="float64").median())


def _historical_revenue_guidance_beats(labels_map: dict[str, pd.Series] | None) -> list[float]:
    beats: list[float] = []
    if not labels_map:
        return beats

    for quarter, row in labels_map.items():
        previous_quarter = _previous_quarter(quarter)
        previous_row = labels_map.get(previous_quarter) if previous_quarter else None
        actual_revenue = _safe_float(row.get("actual_revenue_musd"))
        guidance_revenue = _safe_float(previous_row.get("guidance_next_q_revenue_musd")) if previous_row is not None else None
        if actual_revenue and guidance_revenue and guidance_revenue > 0:
            beats.append(actual_revenue / guidance_revenue - 1.0)
    return beats


def _historical_revenue_qoq_growth(labels_map: dict[str, pd.Series] | None) -> list[float]:
    growth_rates: list[float] = []
    if not labels_map:
        return growth_rates

    for quarter, row in labels_map.items():
        previous_quarter = _previous_quarter(quarter)
        previous_row = labels_map.get(previous_quarter) if previous_quarter else None
        actual_revenue = _safe_float(row.get("actual_revenue_musd"))
        previous_revenue = _safe_float(previous_row.get("actual_revenue_musd")) if previous_row is not None else None
        if actual_revenue and previous_revenue and previous_revenue > 0:
            growth_rates.append(actual_revenue / previous_revenue - 1.0)
    return growth_rates


def _historical_ebitda_margins(labels_map: dict[str, pd.Series] | None) -> list[float]:
    margins: list[float] = []
    if not labels_map:
        return margins

    for row in labels_map.values():
        actual_revenue = _safe_float(row.get("actual_revenue_musd"))
        actual_ebitda = _safe_float(row.get("actual_adjusted_ebitda_musd"))
        if actual_revenue and actual_ebitda and actual_revenue > 0:
            margins.append(actual_ebitda / actual_revenue)
    return margins


def _historical_next_q_guidance_ratios(labels_map: dict[str, pd.Series] | None) -> list[float]:
    ratios: list[float] = []
    if not labels_map:
        return ratios

    for row in labels_map.values():
        actual_revenue = _safe_float(row.get("actual_revenue_musd"))
        next_q_guidance = _safe_float(row.get("guidance_next_q_revenue_musd"))
        if actual_revenue and next_q_guidance and actual_revenue > 0:
            ratios.append(next_q_guidance / actual_revenue)
    return ratios


def _historical_ebitda_to_net_income_ratios(
    labels_map: dict[str, pd.Series] | None,
    diluted_shares_m: float | None,
) -> list[float]:
    ratios: list[float] = []
    if not labels_map or not diluted_shares_m or diluted_shares_m <= 0:
        return ratios

    for row in labels_map.values():
        actual_eps = _safe_float(row.get("actual_eps"))
        actual_ebitda = _safe_float(row.get("actual_adjusted_ebitda_musd"))
        actual_revenue = _safe_float(row.get("actual_revenue_musd"))
        if actual_eps is None or actual_ebitda is None or actual_revenue is None:
            continue
        if actual_eps <= 0 or actual_eps >= 3.0 or actual_ebitda <= 0 or actual_revenue <= 0:
            continue

        implied_net_income = actual_eps * diluted_shares_m
        implied_net_margin = implied_net_income / actual_revenue
        implied_ebitda_margin = actual_ebitda / actual_revenue
        if implied_ebitda_margin <= 0:
            continue
        if implied_net_margin <= 0 or implied_net_margin >= 0.35:
            continue

        ratio = implied_net_income / actual_ebitda
        if 0.20 <= ratio <= 0.90:
            ratios.append(ratio)
    return ratios


def _build_eps_context(labels_map: dict[str, pd.Series] | None) -> dict[str, float | None]:
    balance_context = _extract_latest_balance_and_cashflow_context()
    diluted_shares_m = _safe_float(balance_context.get("diluted_shares_m"))
    if diluted_shares_m is None or diluted_shares_m <= 0:
        diluted_shares_m = 49.8

    conversion_ratios = _historical_ebitda_to_net_income_ratios(labels_map, diluted_shares_m)
    ebitda_to_net_income_ratio = _median(conversion_ratios)
    if ebitda_to_net_income_ratio is None:
        ebitda_to_net_income_ratio = 0.55

    return {
        "diluted_shares_m": _round_or_none(diluted_shares_m, 3),
        "ebitda_to_net_income_ratio": _round_or_none(ebitda_to_net_income_ratio, 4),
        "historical_eps_quarters": float(len(conversion_ratios)),
    }


def _load_labels_df() -> pd.DataFrame:
    if not QUARTERLY_LABELS_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(QUARTERLY_LABELS_FILE)
    if df.empty:
        return df

    df["quarter"] = df["quarter"].astype(str)
    if "earnings_release_date" in df.columns:
        df["earnings_release_date"] = pd.to_datetime(df["earnings_release_date"], errors="coerce")

    numeric_columns = [
        "actual_revenue_musd",
        "consensus_revenue_musd",
        "revenue_surprise_pct",
        "actual_eps",
        "consensus_eps",
        "eps_surprise_pct",
        "actual_adjusted_ebitda_musd",
        "consensus_adjusted_ebitda_musd",
        "actual_paid_subscribers_m",
        "consensus_paid_subscribers_m",
        "actual_subscription_revenue_musd",
        "consensus_subscription_revenue_musd",
        "guidance_next_q_revenue_musd",
        "guidance_fy_revenue_musd",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _load_signal_history_df() -> pd.DataFrame:
    if not FINANCIAL_SIGNALS_HISTORY_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(FINANCIAL_SIGNALS_HISTORY_FILE)
    if df.empty:
        return df

    rename_map = {
        "metadata_as_of_date": "as_of_date",
        "metadata_phase": "phase",
        "panel_target_panel_size": "target_panel_size",
        "panel_observed_users_today": "observed_users_today",
        "panel_coverage_ratio": "coverage_ratio",
        "business_signals_active_rate": "active_rate",
        "business_signals_avg_streak": "avg_streak",
        "business_signals_xp_delta_mean": "xp_delta_mean",
        "business_signals_reactivation_rate": "reactivation_rate",
        "business_signals_churn_rate": "churn_rate",
        "business_signals_super_rate": "super_rate",
        "business_signals_max_rate": "max_rate",
        "business_signals_high_value_retention_rate": "high_value_retention_rate",
        "financial_proxy_signals_engagement_quality_index": "engagement_quality_index",
        "financial_proxy_signals_engagement_quality_trend": "engagement_quality_trend",
        "financial_proxy_signals_premium_momentum_14d": "premium_momentum_14d",
        "financial_proxy_signals_max_momentum_14d": "max_momentum_14d",
        "financial_proxy_signals_churn_trend_14d": "churn_trend_14d",
        "financial_proxy_signals_reactivation_trend_7d": "reactivation_trend_7d",
        "financial_proxy_signals_high_value_retention_trend": "high_value_retention_trend",
        "financial_proxy_signals_premium_net_adds_proxy_7d": "premium_net_adds_proxy_7d",
        "financial_proxy_signals_max_net_adds_proxy_7d": "max_net_adds_proxy_7d",
        "financial_proxy_signals_subscription_momentum_proxy": "subscription_momentum_proxy",
        "financial_proxy_signals_monetization_momentum_index": "monetization_momentum_index",
        "financial_proxy_signals_growth_acceleration_7d": "growth_acceleration_7d",
        "financial_proxy_signals_signal_bias": "signal_bias",
        "financial_proxy_signals_confidence_level": "confidence_level",
        "financial_proxy_signals_main_drivers": "main_drivers_text",
        "financial_proxy_signals_main_risks": "main_risks_text",
    }
    df = df.rename(columns=rename_map)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df[df["as_of_date"].notna()].sort_values("as_of_date").reset_index(drop=True)
    df["quarter"] = df["as_of_date"].dt.to_period("Q").astype(str)
    return df


def _build_snapshot_for_quarter(
    quarter: str,
    quarter_df: pd.DataFrame,
    label_row: pd.Series | None = None,
    labels_map: dict[str, pd.Series] | None = None,
    eps_context: dict[str, float | None] | None = None,
) -> dict[str, object]:
    if quarter_df.empty:
        return {}

    quarter_df = quarter_df.sort_values("as_of_date").reset_index(drop=True)
    release_date = None
    if label_row is not None and "earnings_release_date" in label_row.index:
        release_date = label_row.get("earnings_release_date")
        if pd.notna(release_date):
            quarter_df = quarter_df[quarter_df["as_of_date"] < release_date]

    if quarter_df.empty:
        return {}

    observed_days = int(len(quarter_df))
    avg_coverage_ratio = _mean(quarter_df["coverage_ratio"])
    latest_coverage_ratio = _latest(quarter_df["coverage_ratio"])
    avg_monetization_momentum = _mean(quarter_df["monetization_momentum_index"], 30)
    avg_engagement_quality = _mean(quarter_df["engagement_quality_index"], 30)
    avg_premium_momentum_14d = _mean(quarter_df["premium_momentum_14d"], 14)
    avg_max_momentum_14d = _mean(quarter_df["max_momentum_14d"], 14)
    avg_churn_trend_14d = _mean(quarter_df["churn_trend_14d"], 14)
    avg_reactivation_trend_7d = _mean(quarter_df["reactivation_trend_7d"], 7)
    avg_high_value_retention_trend = _mean(quarter_df["high_value_retention_trend"], 14)
    avg_active_rate = _mean(quarter_df["active_rate"], 14)
    avg_super_rate = _mean(quarter_df["super_rate"], 14)
    avg_max_rate = _mean(quarter_df["max_rate"], 14)
    avg_xp_delta = _mean(quarter_df["xp_delta_mean"], 14)
    avg_subscription_proxy = _mean(quarter_df["subscription_momentum_proxy"], 14)
    previous_quarter = _previous_quarter(quarter)
    previous_label_row = labels_map.get(previous_quarter) if labels_map and previous_quarter else None
    revenue_guidance_reference = None
    if previous_label_row is not None and "guidance_next_q_revenue_musd" in previous_label_row.index:
        revenue_guidance_reference = _safe_float(previous_label_row.get("guidance_next_q_revenue_musd"))

    monetization_norm = _clip(
        (avg_monetization_momentum - 50.0) / 30.0 if avg_monetization_momentum is not None else None
    )
    engagement_norm = _clip(
        (avg_engagement_quality - 90.0) / 10.0 if avg_engagement_quality is not None else None
    )
    premium_norm = _clip(
        (avg_premium_momentum_14d / 0.03) if avg_premium_momentum_14d is not None else None
    )
    max_norm = _clip((avg_max_momentum_14d / 0.01) if avg_max_momentum_14d is not None else None)
    churn_norm = _clip(
        (-avg_churn_trend_14d / 0.02) if avg_churn_trend_14d is not None else None
    )
    reactivation_norm = _clip(
        (avg_reactivation_trend_7d / 0.02) if avg_reactivation_trend_7d is not None else None
    )
    high_value_norm = _clip(
        (avg_high_value_retention_trend / 0.02) if avg_high_value_retention_trend is not None else None
    )
    subscription_norm = _clip(
        (avg_subscription_proxy / 0.01) if avg_subscription_proxy is not None else None
    )

    breadth_factor = min(1.0, (avg_coverage_ratio or 0.0) / 0.70) * min(1.0, observed_days / 21.0)

    quarter_score = round(
        (
            50.0
            + 50.0
            * (
                0.28 * monetization_norm
                + 0.18 * engagement_norm
                + 0.16 * premium_norm
                + 0.08 * max_norm
                + 0.15 * churn_norm
                + 0.08 * reactivation_norm
                + 0.07 * high_value_norm
            )
        )
        * (0.70 + 0.30 * breadth_factor),
        2,
    )

    revenue_score = round(
        (
            50.0
            + 50.0
            * (
                0.36 * monetization_norm
                + 0.18 * subscription_norm
                + 0.14 * premium_norm
                + 0.12 * engagement_norm
                + 0.10 * reactivation_norm
                + 0.10 * high_value_norm
            )
        )
        * (0.70 + 0.30 * breadth_factor),
        2,
    )
    ebitda_score = round(
        (
            50.0
            + 50.0
            * (
                0.30 * monetization_norm
                + 0.20 * engagement_norm
                + 0.22 * churn_norm
                + 0.10 * reactivation_norm
                + 0.10 * high_value_norm
                + 0.08 * max_norm
            )
        )
        * (0.70 + 0.30 * breadth_factor),
        2,
    )
    guidance_score = round(
        (
            50.0
            + 50.0
            * (
                0.34 * monetization_norm
                + 0.18 * premium_norm
                + 0.14 * engagement_norm
                + 0.14 * churn_norm
                + 0.10 * reactivation_norm
                + 0.10 * high_value_norm
            )
        )
        * (0.70 + 0.30 * breadth_factor),
        2,
    )

    confidence_label = _label_confidence(avg_coverage_ratio, observed_days)
    signal_bias = _label_signal(quarter_score)
    revenue_prob = _prob_from_score(revenue_score, confidence_label)
    ebitda_prob = _prob_from_score(ebitda_score, confidence_label)
    guidance_prob = _prob_from_score(guidance_score, confidence_label)

    revenue_guidance_beats = _historical_revenue_guidance_beats(labels_map)
    revenue_qoq_growth = _historical_revenue_qoq_growth(labels_map)
    ebitda_margins = _historical_ebitda_margins(labels_map)
    next_q_guidance_ratios = _historical_next_q_guidance_ratios(labels_map)

    median_guidance_beat = _median(revenue_guidance_beats)
    median_qoq_growth = _median(revenue_qoq_growth)
    median_ebitda_margin = _median(ebitda_margins)
    median_next_q_guidance_ratio = _median(next_q_guidance_ratios)
    diluted_shares_m = _safe_float((eps_context or {}).get("diluted_shares_m")) or 49.8
    base_ebitda_to_net_income_ratio = _safe_float((eps_context or {}).get("ebitda_to_net_income_ratio")) or 0.55

    previous_actual_revenue = _safe_float(previous_label_row.get("actual_revenue_musd")) if previous_label_row is not None else None

    estimated_revenue = None
    if revenue_guidance_reference and revenue_guidance_reference > 0:
        base_beat = median_guidance_beat if median_guidance_beat is not None else 0.025
        beat_adjustment = ((revenue_prob or 0.50) - 0.50) * 0.08
        implied_beat = max(-0.03, min(0.08, base_beat + beat_adjustment))
        estimated_revenue = revenue_guidance_reference * (1.0 + implied_beat)
    elif previous_actual_revenue and previous_actual_revenue > 0:
        base_growth = median_qoq_growth if median_qoq_growth is not None else 0.06
        growth_adjustment = ((revenue_prob or 0.50) - 0.50) * 0.10
        implied_growth = max(-0.05, min(0.15, base_growth + growth_adjustment))
        estimated_revenue = previous_actual_revenue * (1.0 + implied_growth)

    estimated_ebitda = None
    if estimated_revenue and estimated_revenue > 0:
        base_margin = median_ebitda_margin if median_ebitda_margin is not None else 0.29
        margin_adjustment = ((ebitda_prob or 0.50) - 0.50) * 0.04
        implied_margin = max(0.18, min(0.40, base_margin + margin_adjustment))
        estimated_ebitda = estimated_revenue * implied_margin

    estimated_net_income = None
    estimated_eps = None
    if estimated_ebitda and estimated_ebitda > 0 and diluted_shares_m > 0:
        net_income_adjustment = ((ebitda_prob or 0.50) - 0.50) * 0.08
        implied_net_income_ratio = max(
            0.35,
            min(0.75, base_ebitda_to_net_income_ratio + net_income_adjustment),
        )
        estimated_net_income = estimated_ebitda * implied_net_income_ratio
        estimated_eps = estimated_net_income / diluted_shares_m

    estimated_next_q_guidance = None
    if estimated_revenue and estimated_revenue > 0:
        base_ratio = median_next_q_guidance_ratio if median_next_q_guidance_ratio is not None else 1.04
        guidance_adjustment = ((guidance_prob or 0.50) - 0.50) * 0.04
        implied_ratio = max(0.98, min(1.12, base_ratio + guidance_adjustment))
        estimated_next_q_guidance = estimated_revenue * implied_ratio

    drivers: list[str] = []
    risks: list[str] = []
    if avg_monetization_momentum is not None and avg_monetization_momentum >= 55:
        drivers.append("Le momentum de monétisation se maintient au-dessus de la zone neutre.")
    if avg_premium_momentum_14d is not None and avg_premium_momentum_14d > 0.002:
        drivers.append("La dynamique Super sur 14 jours demeure favorable.")
    if avg_churn_trend_14d is not None and avg_churn_trend_14d < -0.002:
        drivers.append("Le taux d'abandon se détend sur la période récente.")
    if avg_reactivation_trend_7d is not None and avg_reactivation_trend_7d > 0.002:
        drivers.append("Les réactivations soutiennent la rétention nette.")
    if avg_high_value_retention_trend is not None and avg_high_value_retention_trend > 0.002:
        drivers.append("La rétention des cohortes à forte valeur reste solide.")
    if avg_coverage_ratio is not None and avg_coverage_ratio >= 0.70:
        drivers.append("La couverture du panel offre une profondeur de lecture satisfaisante.")

    if avg_monetization_momentum is not None and avg_monetization_momentum < 45:
        risks.append("Le momentum de monétisation demeure sous la zone neutre.")
    if avg_premium_momentum_14d is not None and avg_premium_momentum_14d < -0.002:
        risks.append("La dynamique Super sur 14 jours ralentit.")
    if avg_churn_trend_14d is not None and avg_churn_trend_14d > 0.002:
        risks.append("Le taux d'abandon repart à la hausse sur la période récente.")
    if avg_reactivation_trend_7d is not None and avg_reactivation_trend_7d < -0.002:
        risks.append("Les réactivations montrent des signes d'essoufflement.")
    if avg_high_value_retention_trend is not None and avg_high_value_retention_trend < -0.002:
        risks.append("La rétention des cohortes à forte valeur se dégrade.")
    if avg_coverage_ratio is not None and avg_coverage_ratio < 0.45:
        risks.append("La couverture du panel limite encore la fiabilité du signal.")

    if not drivers:
        drivers.append("Le trimestre reste encore trop jeune pour faire émerger un moteur dominant.")
    if not risks:
        risks.append("Le principal risque reste le manque d'historique de guidance pour calibrer finement le modèle.")

    snapshot = {
        "quarter": quarter,
        "quarter_start": quarter_df["as_of_date"].min().strftime("%Y-%m-%d"),
        "quarter_end_observed": quarter_df["as_of_date"].max().strftime("%Y-%m-%d"),
        "earnings_release_date": release_date.strftime("%Y-%m-%d") if pd.notna(release_date) else None,
        "observed_days": observed_days,
        "avg_coverage_ratio": _round_or_none(avg_coverage_ratio),
        "latest_coverage_ratio": _round_or_none(latest_coverage_ratio),
        "avg_monetization_momentum_index": _round_or_none(avg_monetization_momentum, 2),
        "avg_engagement_quality_index": _round_or_none(avg_engagement_quality, 2),
        "avg_premium_momentum_14d": _round_or_none(avg_premium_momentum_14d),
        "avg_max_momentum_14d": _round_or_none(avg_max_momentum_14d),
        "avg_churn_trend_14d": _round_or_none(avg_churn_trend_14d),
        "avg_reactivation_trend_7d": _round_or_none(avg_reactivation_trend_7d),
        "avg_high_value_retention_trend": _round_or_none(avg_high_value_retention_trend),
        "avg_active_rate": _round_or_none(avg_active_rate),
        "avg_super_rate": _round_or_none(avg_super_rate),
        "avg_max_rate": _round_or_none(avg_max_rate),
        "avg_xp_delta_mean": _round_or_none(avg_xp_delta, 2),
        "quarter_signal_score": _round_or_none(quarter_score, 2),
        "revenue_score_proxy": _round_or_none(revenue_score, 2),
        "ebitda_score_proxy": _round_or_none(ebitda_score, 2),
        "guidance_score_proxy": _round_or_none(guidance_score, 2),
        "quarter_signal_bias": signal_bias,
        "confidence_level": confidence_label,
        "revenue_beat_probability_proxy": revenue_prob,
        "ebitda_beat_probability_proxy": ebitda_prob,
        "guidance_raise_probability_proxy": guidance_prob,
        "revenue_guidance_reference_musd": _round_or_none(revenue_guidance_reference, 2),
        "revenue_guidance_reference_quarter": previous_quarter,
        "estimated_revenue_musd": _round_or_none(estimated_revenue, 2),
        "estimated_ebitda_musd": _round_or_none(estimated_ebitda, 2),
        "estimated_net_income_musd": _round_or_none(estimated_net_income, 2),
        "estimated_eps": _round_or_none(estimated_eps, 3),
        "diluted_shares_reference_m": _round_or_none(diluted_shares_m, 3),
        "ebitda_to_net_income_ratio": _round_or_none(base_ebitda_to_net_income_ratio, 4),
        "estimated_next_q_guidance_musd": _round_or_none(estimated_next_q_guidance, 2),
        "main_drivers": drivers[:4],
        "main_risks": risks[:4],
        "model_summary_text": "",
        "confidence_context_text": "",
        "snapshot_as_of_date": None,
        "snapshot_locked": False,
        "snapshot_status_label": "En cours",
    }

    if label_row is not None:
        for field in [
            "actual_revenue_musd",
            "consensus_revenue_musd",
            "revenue_surprise_pct",
            "actual_eps",
            "consensus_eps",
            "eps_surprise_pct",
            "actual_adjusted_ebitda_musd",
            "consensus_adjusted_ebitda_musd",
            "actual_paid_subscribers_m",
            "consensus_paid_subscribers_m",
            "actual_subscription_revenue_musd",
            "consensus_subscription_revenue_musd",
            "guidance_next_q_revenue_musd",
            "guidance_fy_revenue_musd",
            "guidance_signal",
            "source_actuals",
            "source_consensus",
            "notes",
        ]:
            if field in label_row.index:
                value = label_row.get(field)
                if pd.isna(value):
                    value = None
                snapshot[field] = value

    return snapshot


def _flatten_quarterly_package(package: dict[str, object]) -> dict[str, object]:
    rows: dict[str, object] = {}

    def _walk(prefix: str, value: object) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                next_prefix = f"{prefix}_{key}" if prefix else key
                _walk(next_prefix, nested)
        elif isinstance(value, list):
            if value and all(isinstance(item, dict) for item in value):
                rows[prefix] = json.dumps(value, ensure_ascii=False)
            else:
                rows[prefix] = " | ".join(str(item) for item in value)
        else:
            rows[prefix] = value

    _walk("", package)
    return rows


def build_quarterly_nowcast_raw_df(package: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    readiness = package.get("labels_readiness", {})

    for snapshot in package.get("historical_snapshots", []):
        drivers = snapshot.get("main_drivers") or []
        risks = snapshot.get("main_risks") or []
        revenue_reference = _safe_float(snapshot.get("revenue_guidance_reference_musd"))
        estimated_revenue = _safe_float(snapshot.get("estimated_revenue_musd"))
        estimated_ebitda = _safe_float(snapshot.get("estimated_ebitda_musd"))
        estimated_eps = _safe_float(snapshot.get("estimated_eps"))
        estimated_next_q_guidance = _safe_float(snapshot.get("estimated_next_q_guidance_musd"))
        row = {
            "quarter": snapshot.get("quarter"),
            "snapshot_as_of_date": snapshot.get("snapshot_as_of_date"),
            "snapshot_locked": bool(snapshot.get("snapshot_locked")),
            "snapshot_status_label": snapshot.get("snapshot_status_label") or _snapshot_status_label(snapshot.get("snapshot_locked")),
            "quarter_start": snapshot.get("quarter_start"),
            "quarter_end_observed": snapshot.get("quarter_end_observed"),
            "earnings_release_date": snapshot.get("earnings_release_date"),
            "observed_days": snapshot.get("observed_days"),
            "avg_coverage_ratio": snapshot.get("avg_coverage_ratio"),
            "latest_coverage_ratio": snapshot.get("latest_coverage_ratio"),
            "avg_active_rate": snapshot.get("avg_active_rate"),
            "avg_super_rate": snapshot.get("avg_super_rate"),
            "avg_max_rate": snapshot.get("avg_max_rate"),
            "avg_xp_delta_mean": snapshot.get("avg_xp_delta_mean"),
            "avg_monetization_momentum_index": snapshot.get("avg_monetization_momentum_index"),
            "avg_engagement_quality_index": snapshot.get("avg_engagement_quality_index"),
            "avg_premium_momentum_14d": snapshot.get("avg_premium_momentum_14d"),
            "avg_max_momentum_14d": snapshot.get("avg_max_momentum_14d"),
            "avg_churn_trend_14d": snapshot.get("avg_churn_trend_14d"),
            "avg_reactivation_trend_7d": snapshot.get("avg_reactivation_trend_7d"),
            "avg_high_value_retention_trend": snapshot.get("avg_high_value_retention_trend"),
            "quarter_signal_score": snapshot.get("quarter_signal_score"),
            "quarter_signal_bias": snapshot.get("quarter_signal_bias"),
            "confidence_level": snapshot.get("confidence_level"),
            "revenue_beat_probability": snapshot.get(
                "revenue_beat_probability",
                snapshot.get("revenue_beat_probability_proxy"),
            ),
            "revenue_beat_probability_proxy": snapshot.get("revenue_beat_probability_proxy"),
            "ebitda_beat_probability": snapshot.get(
                "ebitda_beat_probability",
                snapshot.get("ebitda_beat_probability_proxy"),
            ),
            "ebitda_beat_probability_proxy": snapshot.get("ebitda_beat_probability_proxy"),
            "guidance_raise_probability": snapshot.get(
                "guidance_raise_probability",
                snapshot.get("guidance_raise_probability_proxy"),
            ),
            "guidance_raise_probability_proxy": snapshot.get("guidance_raise_probability_proxy"),
            "revenue_guidance_reference_musd": snapshot.get("revenue_guidance_reference_musd"),
            "revenue_guidance_reference_quarter": snapshot.get("revenue_guidance_reference_quarter"),
            "estimated_revenue_musd": snapshot.get("estimated_revenue_musd"),
            "estimated_ebitda_musd": snapshot.get("estimated_ebitda_musd"),
            "estimated_net_income_musd": snapshot.get("estimated_net_income_musd"),
            "estimated_eps": snapshot.get("estimated_eps"),
            "diluted_shares_reference_m": snapshot.get("diluted_shares_reference_m"),
            "ebitda_to_net_income_ratio": snapshot.get("ebitda_to_net_income_ratio"),
            "estimated_next_q_guidance_musd": snapshot.get("estimated_next_q_guidance_musd"),
            "actual_revenue_musd": snapshot.get("actual_revenue_musd"),
            "actual_eps": snapshot.get("actual_eps"),
            "actual_adjusted_ebitda_musd": snapshot.get("actual_adjusted_ebitda_musd"),
            "actual_subscription_revenue_musd": snapshot.get("actual_subscription_revenue_musd"),
            "actual_paid_subscribers_m": snapshot.get("actual_paid_subscribers_m"),
            "guidance_next_q_revenue_musd": snapshot.get("guidance_next_q_revenue_musd"),
            "guidance_fy_revenue_musd": snapshot.get("guidance_fy_revenue_musd"),
            "guidance_signal": snapshot.get("guidance_signal"),
            "main_drivers_text": "\n".join(f"- {item}" for item in drivers),
            "main_risks_text": "\n".join(f"- {item}" for item in risks),
            "model_summary_text": snapshot.get("model_summary_text") or _build_snapshot_summary_text(snapshot),
            "confidence_context_text": snapshot.get("confidence_context_text") or _build_confidence_context_text(snapshot),
            "revenue_note_text": _format_estimation_note(
                estimated_revenue,
                revenue_reference,
                prefix="Est.",
                benchmark_label="guidance",
            ),
            "ebitda_note_text": (
                f"Est. {_format_number_text(estimated_ebitda, 1)} M$"
                if estimated_ebitda is not None
                else "Est. N/D"
            ),
            "eps_note_text": (
                f"Pont EBITDA -> resultat net / {_format_number_text(_safe_float(snapshot.get('diluted_shares_reference_m')), 3)} M actions"
                if estimated_eps is not None
                else "Pont EBITDA -> resultat net / actions diluees"
            ),
            "next_guidance_note_text": _format_estimation_note(
                estimated_next_q_guidance,
                revenue_reference,
                prefix="Guide N+1 estimé",
                benchmark_label="base",
            ),
            "actual_labels_ready": readiness.get("actual_labels_ready"),
            "guidance_benchmarks_ready": readiness.get("guidance_benchmarks_ready"),
            "supervised_ready": readiness.get("supervised_ready"),
            "next_step": readiness.get("next_step"),
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.sort_values("quarter").reset_index(drop=True)
    return df


def _build_historical_snapshot_df(history_df: pd.DataFrame, labels_df: pd.DataFrame) -> pd.DataFrame:
    quarter_rows: list[dict[str, object]] = []
    all_quarters = _sort_quarters(
        list(set(history_df["quarter"].dropna().tolist()) | set(labels_df.get("quarter", pd.Series(dtype=str)).dropna().tolist()))
    )

    labels_map = {
        str(row["quarter"]): row
        for _, row in labels_df.iterrows()
        if row.get("quarter")
    }
    eps_context = _build_eps_context(labels_map)

    for quarter in all_quarters:
        quarter_df = history_df[history_df["quarter"] == quarter]
        label_row = labels_map.get(quarter)
        snapshot = _build_snapshot_for_quarter(quarter, quarter_df, label_row, labels_map, eps_context=eps_context)
        if snapshot:
            quarter_rows.append(snapshot)

    if not quarter_rows:
        return pd.DataFrame()

    df = pd.DataFrame(quarter_rows)
    df = df.sort_values("quarter").reset_index(drop=True)
    return df


POST_RELEASE_FIELDS = {
    "earnings_release_date",
    "actual_revenue_musd",
    "consensus_revenue_musd",
    "revenue_surprise_pct",
    "actual_eps",
    "consensus_eps",
    "eps_surprise_pct",
    "actual_adjusted_ebitda_musd",
    "consensus_adjusted_ebitda_musd",
    "actual_paid_subscribers_m",
    "consensus_paid_subscribers_m",
    "actual_subscription_revenue_musd",
    "consensus_subscription_revenue_musd",
    "guidance_next_q_revenue_musd",
    "guidance_fy_revenue_musd",
    "guidance_signal",
    "source_actuals",
    "source_consensus",
    "notes",
}


def _merge_saved_and_live_snapshots(live_df: pd.DataFrame, reference_ts: pd.Timestamp) -> pd.DataFrame:
    saved_df = _load_saved_snapshots_df()
    if live_df.empty and saved_df.empty:
        return pd.DataFrame()

    live_rows = {
        str(row["quarter"]): row.to_dict()
        for _, row in live_df.iterrows()
        if row.get("quarter")
    }
    saved_rows = {
        str(row["quarter"]): row.to_dict()
        for _, row in saved_df.iterrows()
        if row.get("quarter")
    }

    merged_rows: list[dict[str, object]] = []
    all_quarters = _sort_quarters(list(set(live_rows.keys()) | set(saved_rows.keys())))
    reference_day = pd.Timestamp(reference_ts).normalize()

    for quarter in all_quarters:
        quarter_end = _quarter_start_end(quarter)[1]
        quarter_closed = quarter_end is not None and reference_day > quarter_end
        live_row = live_rows.get(quarter)
        saved_row = saved_rows.get(quarter)

        if saved_row and bool(saved_row.get("snapshot_locked")):
            selected = dict(saved_row)
            if live_row:
                for field in POST_RELEASE_FIELDS:
                    value = live_row.get(field)
                    if value is None:
                        continue
                    if isinstance(value, float) and pd.isna(value):
                        continue
                    selected[field] = value
        elif live_row:
            selected = dict(live_row)
            snapshot_as_of_date = selected.get("quarter_end_observed") or selected.get("snapshot_as_of_date")
            if snapshot_as_of_date is None:
                snapshot_as_of_date = reference_day.strftime("%Y-%m-%d")
            selected["snapshot_as_of_date"] = snapshot_as_of_date
            selected["snapshot_locked"] = bool(quarter_closed)
        elif saved_row:
            selected = dict(saved_row)
        else:
            continue

        selected["quarter"] = quarter
        selected["snapshot_status_label"] = _snapshot_status_label(selected.get("snapshot_locked"))
        selected["main_drivers"] = _parse_list(selected.get("main_drivers"))
        selected["main_risks"] = _parse_list(selected.get("main_risks"))
        selected["model_summary_text"] = _build_snapshot_summary_text(selected)
        selected["confidence_context_text"] = _build_confidence_context_text(selected)
        merged_rows.append(selected)

    if not merged_rows:
        return pd.DataFrame()

    df = pd.DataFrame(merged_rows)
    df = df.sort_values("quarter").reset_index(drop=True)
    return df


def build_quarterly_nowcast_package(reference_date: str | None = None) -> dict[str, object] | None:
    history_df = _load_signal_history_df()
    if history_df.empty:
        return None

    labels_df = _load_labels_df()
    if reference_date is None:
        reference_date = now_toronto().strftime("%Y-%m-%d")

    reference_ts = pd.to_datetime(reference_date, errors="coerce")
    if pd.isna(reference_ts):
        reference_ts = history_df["as_of_date"].max()

    eligible_history = history_df[history_df["as_of_date"] <= reference_ts].copy()
    if eligible_history.empty:
        eligible_history = history_df.copy()

    current_quarter = _quarter_from_date(reference_ts) or str(eligible_history["quarter"].iloc[-1])
    historical_snapshot_df = _build_historical_snapshot_df(eligible_history, labels_df)
    if historical_snapshot_df.empty:
        return None

    current_snapshot_df = historical_snapshot_df[historical_snapshot_df["quarter"] == current_quarter]
    if current_snapshot_df.empty:
        current_snapshot = historical_snapshot_df.iloc[-1].to_dict()
        current_quarter = str(current_snapshot.get("quarter"))
    else:
        current_snapshot = current_snapshot_df.iloc[-1].to_dict()

    actual_labels_ready = 0
    guidance_benchmarks_ready = 0
    if not labels_df.empty:
        actual_labels_ready = int(labels_df["actual_revenue_musd"].notna().sum()) if "actual_revenue_musd" in labels_df.columns else 0
        guidance_benchmarks_ready = int(labels_df["guidance_next_q_revenue_musd"].notna().sum()) if "guidance_next_q_revenue_musd" in labels_df.columns else 0

    model_output = {
        "quarter_signal_bias": current_snapshot.get("quarter_signal_bias"),
        "confidence_level": current_snapshot.get("confidence_level"),
        "quarter_signal_score": current_snapshot.get("quarter_signal_score"),
        "revenue_guidance_reference_musd": current_snapshot.get("revenue_guidance_reference_musd"),
        "revenue_guidance_reference_quarter": current_snapshot.get("revenue_guidance_reference_quarter"),
        "revenue_beat_probability": current_snapshot.get("revenue_beat_probability_proxy"),
        "revenue_beat_probability_proxy": current_snapshot.get("revenue_beat_probability_proxy"),
        "revenue_beat_guidance_probability": current_snapshot.get("revenue_beat_probability_proxy"),
        "estimated_revenue_musd": current_snapshot.get("estimated_revenue_musd"),
        "ebitda_beat_probability_proxy": current_snapshot.get("ebitda_beat_probability_proxy"),
        "ebitda_beat_probability": current_snapshot.get("ebitda_beat_probability_proxy"),
        "estimated_ebitda_musd": current_snapshot.get("estimated_ebitda_musd"),
        "estimated_net_income_musd": current_snapshot.get("estimated_net_income_musd"),
        "estimated_eps": current_snapshot.get("estimated_eps"),
        "diluted_shares_reference_m": current_snapshot.get("diluted_shares_reference_m"),
        "guidance_raise_probability_proxy": current_snapshot.get("guidance_raise_probability_proxy"),
        "guidance_raise_probability": current_snapshot.get("guidance_raise_probability_proxy"),
        "estimated_next_q_guidance_musd": current_snapshot.get("estimated_next_q_guidance_musd"),
        "main_drivers": current_snapshot.get("main_drivers", []),
        "main_risks": current_snapshot.get("main_risks", []),
        "confidence_context_text": current_snapshot.get("confidence_context_text") or _build_confidence_context_text(current_snapshot),
    }

    package: dict[str, object] = {
        "metadata": {
            "phase": "phase_2_quarterly_nowcast_v1",
            "as_of_date": pd.Timestamp(reference_ts).strftime("%Y-%m-%d"),
            "current_quarter": current_quarter,
            "default_selected_quarter": current_quarter,
            "available_quarters": historical_snapshot_df["quarter"].dropna().astype(str).tolist(),
            "history_days_available": int(len(eligible_history)),
            "history_quarters_available": int(historical_snapshot_df["quarter"].nunique()),
            "source": "financial_signals_history.csv + quarterly_labels_template.csv",
        },
        "current_quarter": current_snapshot,
        "model_output": model_output,
        "labels_readiness": {
            "actual_labels_ready": actual_labels_ready,
            "guidance_benchmarks_ready": guidance_benchmarks_ready,
            "supervised_ready": bool(actual_labels_ready >= 6 and guidance_benchmarks_ready >= 4),
            "next_step": (
                "Completer d'abord l'historique de guidance management, puis ajouter ensuite le consensus analystes comme couche de comparaison marche."
            ),
        },
        "historical_snapshots": historical_snapshot_df.to_dict("records"),
        "assumptions": [
            "Les probabilites trimestrielles actuelles sont des probabilites implicites explicables, benchmarkees sur le guidance management quand il est disponible.",
            "Le score combine momentum de monetisation, qualite d'engagement, retention et reactivations.",
            "La confiance depend de la couverture moyenne du panel et du nombre de jours observes dans le trimestre.",
        ],
    }
    return package


def save_quarterly_nowcast_package(package: dict[str, object]) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    QUARTERLY_NOWCAST_JSON_FILE.write_text(
        json.dumps(package, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    snapshots_df = pd.DataFrame(package.get("historical_snapshots", []))
    if not snapshots_df.empty:
        snapshots_df.to_csv(QUARTERLY_SNAPSHOTS_FILE, index=False, encoding="utf-8")
    else:
        pd.DataFrame().to_csv(QUARTERLY_SNAPSHOTS_FILE, index=False, encoding="utf-8")

    return QUARTERLY_NOWCAST_JSON_FILE, QUARTERLY_SNAPSHOTS_FILE


def generate_quarterly_nowcast_package(reference_date: str | None = None) -> dict[str, object] | None:
    package = build_quarterly_nowcast_package(reference_date=reference_date)
    if not package:
        return None
    save_quarterly_nowcast_package(package)
    return package
