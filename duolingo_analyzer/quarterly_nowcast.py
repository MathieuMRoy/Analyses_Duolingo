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


def _previous_quarter(quarter: str) -> str | None:
    try:
        period = pd.Period(quarter, freq="Q")
    except Exception:
        return None
    return str(period - 1)


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

    drivers: list[str] = []
    risks: list[str] = []
    if avg_monetization_momentum is not None and avg_monetization_momentum >= 55:
        drivers.append("Le momentum de monétisation reste au-dessus de la zone neutre.")
    if avg_premium_momentum_14d is not None and avg_premium_momentum_14d > 0.002:
        drivers.append("Le momentum Super 14j reste positif.")
    if avg_churn_trend_14d is not None and avg_churn_trend_14d < -0.002:
        drivers.append("La tendance d'abandon se détend sur la fenêtre récente.")
    if avg_reactivation_trend_7d is not None and avg_reactivation_trend_7d > 0.002:
        drivers.append("Les réactivations soutiennent la rétention nette.")
    if avg_high_value_retention_trend is not None and avg_high_value_retention_trend > 0.002:
        drivers.append("La cohorte à forte valeur conserve un signal de rétention solide.")
    if avg_coverage_ratio is not None and avg_coverage_ratio >= 0.70:
        drivers.append("La couverture du panel donne une bonne profondeur de lecture.")

    if avg_monetization_momentum is not None and avg_monetization_momentum < 45:
        risks.append("Le momentum de monétisation reste sous la zone neutre.")
    if avg_premium_momentum_14d is not None and avg_premium_momentum_14d < -0.002:
        risks.append("Le momentum Super ralentit sur la fenêtre 14j.")
    if avg_churn_trend_14d is not None and avg_churn_trend_14d > 0.002:
        risks.append("La tendance d'abandon remonte sur la fenêtre récente.")
    if avg_reactivation_trend_7d is not None and avg_reactivation_trend_7d < -0.002:
        risks.append("Les réactivations se tassent.")
    if avg_high_value_retention_trend is not None and avg_high_value_retention_trend < -0.002:
        risks.append("La rétention des cohortes à forte valeur se dégrade.")
    if avg_coverage_ratio is not None and avg_coverage_ratio < 0.45:
        risks.append("La couverture du panel limite encore la confiance du signal.")

    if not drivers:
        drivers.append("Le trimestre manque encore de profondeur pour faire émerger un driver dominant.")
    if not risks:
        risks.append("Le risque principal reste le manque d'historique guidance pour calibrer finement le modele.")

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
        "revenue_beat_probability_proxy": _prob_from_score(revenue_score, confidence_label),
        "ebitda_beat_probability_proxy": _prob_from_score(ebitda_score, confidence_label),
        "guidance_raise_probability_proxy": _prob_from_score(guidance_score, confidence_label),
        "revenue_guidance_reference_musd": _round_or_none(revenue_guidance_reference, 2),
        "revenue_guidance_reference_quarter": previous_quarter,
        "main_drivers": drivers[:4],
        "main_risks": risks[:4],
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

    def _add(section: str, signal: str, value: object, definition: str) -> None:
        rows.append(
            {
                "Section": section,
                "Signal": signal,
                "Valeur": value,
                "Definition": definition,
            }
        )

    metadata = package.get("metadata", {})
    current = package.get("current_quarter", {})
    model = package.get("model_output", {})
    readiness = package.get("labels_readiness", {})

    _add("Meta", "as_of_date", metadata.get("as_of_date"), "Date de reference du nowcast trimestriel.")
    _add("Meta", "current_quarter", metadata.get("current_quarter"), "Trimestre actuellement suivi.")
    _add("Meta", "phase", metadata.get("phase"), "Phase du systeme de nowcasting trimestriel.")

    for key, definition in {
        "observed_days": "Nombre de jours de signaux observes dans le trimestre courant.",
        "avg_coverage_ratio": "Couverture moyenne du panel sur le trimestre courant.",
        "avg_monetization_momentum_index": "Moyenne trimestrielle recente du momentum de monetisation.",
        "avg_engagement_quality_index": "Moyenne trimestrielle recente de la qualite d'engagement.",
        "avg_premium_momentum_14d": "Variation recente du taux Super sur la fenetre 14 jours.",
        "avg_churn_trend_14d": "Variation recente du taux d'abandon sur la fenetre 14 jours.",
        "avg_reactivation_trend_7d": "Variation recente des reactivations sur la fenetre 7 jours.",
        "avg_high_value_retention_trend": "Variation recente de la retention des cohortes a forte valeur.",
    }.items():
        _add("Current Quarter", key, current.get(key), definition)

    _add("Model", "quarter_signal_bias", model.get("quarter_signal_bias"), "Lecture globale du trimestre courant.")
    _add("Model", "confidence_level", model.get("confidence_level"), "Confiance du score trimestriel en fonction de la couverture et de la profondeur.")
    _add("Model", "quarter_signal_score", model.get("quarter_signal_score"), "Score composite explicable du trimestre.")
    _add(
        "Model",
        "revenue_guidance_reference_musd",
        model.get("revenue_guidance_reference_musd"),
        "Guidance management revenus utilise comme benchmark quand il est disponible.",
    )
    _add(
        "Model",
        "revenue_beat_probability_proxy",
        model.get("revenue_beat_probability_proxy"),
        "Probabilite implicite de battre le guidance revenus management, non supervisee a ce stade.",
    )
    _add(
        "Model",
        "ebitda_beat_probability_proxy",
        model.get("ebitda_beat_probability_proxy"),
        "Probabilite implicite de beat EBITDA, non supervisee a ce stade.",
    )
    _add(
        "Model",
        "guidance_raise_probability_proxy",
        model.get("guidance_raise_probability_proxy"),
        "Probabilite implicite de relever la guidance, non supervisee a ce stade.",
    )
    _add("Model", "main_drivers", " | ".join(model.get("main_drivers", [])), "Drivers haussiers principaux.")
    _add("Model", "main_risks", " | ".join(model.get("main_risks", [])), "Risques principaux.")

    _add(
        "Readiness",
        "actual_labels_ready",
        readiness.get("actual_labels_ready"),
        "Nombre de trimestres avec actuals financiers renseignes.",
    )
    _add(
        "Readiness",
        "guidance_benchmarks_ready",
        readiness.get("guidance_benchmarks_ready"),
        "Nombre de trimestres avec guidance management revenus disponible comme benchmark.",
    )
    _add(
        "Readiness",
        "supervised_ready",
        readiness.get("supervised_ready"),
        "Etat de preparation du backtest supervise.",
    )

    return pd.DataFrame(rows)


def _build_historical_snapshot_df(history_df: pd.DataFrame, labels_df: pd.DataFrame) -> pd.DataFrame:
    quarter_rows: list[dict[str, object]] = []
    all_quarters = sorted(set(history_df["quarter"].dropna().tolist()) | set(labels_df.get("quarter", pd.Series(dtype=str)).dropna().tolist()))

    labels_map = {
        str(row["quarter"]): row
        for _, row in labels_df.iterrows()
        if row.get("quarter")
    }

    for quarter in all_quarters:
        quarter_df = history_df[history_df["quarter"] == quarter]
        label_row = labels_map.get(quarter)
        snapshot = _build_snapshot_for_quarter(quarter, quarter_df, label_row, labels_map)
        if snapshot:
            quarter_rows.append(snapshot)

    if not quarter_rows:
        return pd.DataFrame()

    df = pd.DataFrame(quarter_rows)
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
        "revenue_beat_probability_proxy": current_snapshot.get("revenue_beat_probability_proxy"),
        "revenue_beat_guidance_probability": current_snapshot.get("revenue_beat_probability_proxy"),
        "ebitda_beat_probability_proxy": current_snapshot.get("ebitda_beat_probability_proxy"),
        "ebitda_beat_probability": current_snapshot.get("ebitda_beat_probability_proxy"),
        "guidance_raise_probability_proxy": current_snapshot.get("guidance_raise_probability_proxy"),
        "guidance_raise_probability": current_snapshot.get("guidance_raise_probability_proxy"),
        "main_drivers": current_snapshot.get("main_drivers", []),
        "main_risks": current_snapshot.get("main_risks", []),
    }

    package: dict[str, object] = {
        "metadata": {
            "phase": "phase_2_quarterly_nowcast_v1",
            "as_of_date": pd.Timestamp(reference_ts).strftime("%Y-%m-%d"),
            "current_quarter": current_quarter,
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
