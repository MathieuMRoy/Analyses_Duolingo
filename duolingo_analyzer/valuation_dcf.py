"""
DCF valuation layer fed by quarterly nowcast and financial filings.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from .config import BASE_DIR, REPORT_DIR

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - fallback if dependency is missing
    PdfReader = None


QUARTERLY_LABELS_FILE = BASE_DIR / "financial_docs" / "quarterly_labels_template.csv"
DCF_VALUATION_JSON_FILE = REPORT_DIR / "dcf_valuation_latest.json"

ANNUAL_REPORT_CANDIDATES = [
    "0001628280-26-012494.pdf",  # FY25 10-K
    "0001628280-25-049743.pdf",  # Q3 2025 10-Q
    "0001562088-25-000168.pdf",  # Q2 2025 10-Q
]

DEFAULT_LATEST_FINANCIAL_CONTEXT = {
    "source_file": "fallback_fy25_context",
    "cash_musd": 1036.4,
    "short_term_investments_musd": 104.1,
    "operating_cash_flow_musd": 387.8,
    "capitalized_software_musd": 9.3,
    "capex_musd": 18.1,
    "free_cash_flow_musd": 360.4,
    "net_income_musd": 414.1,
    "adjusted_ebitda_musd": None,
    "diluted_shares_m": 48.315,
    "stock_based_compensation_musd": 194.8,
    "total_debt_musd": 0.0,
}


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


def _round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _load_labels_df() -> pd.DataFrame:
    if not QUARTERLY_LABELS_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(QUARTERLY_LABELS_FILE)
    if df.empty:
        return df

    df["quarter"] = df["quarter"].astype(str)
    numeric_columns = [
        "actual_revenue_musd",
        "actual_adjusted_ebitda_musd",
        "actual_subscription_revenue_musd",
        "actual_paid_subscribers_m",
        "guidance_next_q_revenue_musd",
        "guidance_fy_revenue_musd",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _quarter_period(quarter: str) -> pd.Period:
    return pd.Period(quarter, freq="Q")


def _previous_quarter(quarter: str) -> str:
    return str(_quarter_period(quarter) - 1)


def _trailing_quarters(quarter: str, count: int = 4) -> list[str]:
    end_period = _quarter_period(quarter)
    return [str(end_period - offset) for offset in reversed(range(count))]


def _sum_actuals_by_year(labels_df: pd.DataFrame) -> dict[int, dict[str, float | None]]:
    yearly: dict[int, dict[str, float | None]] = {}
    if labels_df.empty:
        return yearly

    for year, group in labels_df.groupby(labels_df["quarter"].str[:4].astype(int)):
        revenues = pd.to_numeric(group.get("actual_revenue_musd"), errors="coerce")
        ebitda = pd.to_numeric(group.get("actual_adjusted_ebitda_musd"), errors="coerce")
        if revenues.notna().sum() >= 4:
            yearly.setdefault(year, {})["actual_revenue_musd"] = float(revenues.sum())
        if ebitda.notna().sum() >= 4:
            yearly.setdefault(year, {})["actual_adjusted_ebitda_musd"] = float(ebitda.sum())
    return yearly


def _extract_pdf_text(path: Path) -> str:
    if PdfReader is None or not path.exists():
        return ""
    reader = PdfReader(str(path))
    return "\n".join((page.extract_text() or "") for page in reader.pages).replace("\u00a0", " ")


def _extract_first_match(text: str, pattern: str) -> float | None:
    match = re.search(pattern, text, re.I | re.S)
    if not match:
        return None
    raw = match.group(1).replace(",", "").replace("$", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _extract_two_matches(text: str, pattern: str) -> tuple[float | None, float | None]:
    match = re.search(pattern, text, re.I | re.S)
    if not match:
        return None, None
    values: list[float | None] = []
    for idx in (1, 2):
        raw = match.group(idx).replace(",", "").replace("$", "").strip()
        try:
            values.append(float(raw))
        except ValueError:
            values.append(None)
    return values[0], values[1]


def _extract_latest_balance_and_cashflow_context() -> dict[str, object]:
    docs_dir = BASE_DIR / "financial_docs"
    for candidate in ANNUAL_REPORT_CANDIDATES:
        path = docs_dir / candidate
        text = _extract_pdf_text(path)
        if not text:
            continue

        cash_musd, short_term_investments_musd = _extract_two_matches(
            text,
            r"As of December 31, \d{4}, we had \$?(\d+(?:,\d{3})*(?:\.\d+)?) million in cash and cash equivalents and \$?(\d+(?:,\d{3})*(?:\.\d+)?) million of short-term investments",
        )

        if cash_musd is None:
            cash_kusd, _ = _extract_two_matches(
                text,
                r"Cash and cash equivalents\s*\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+(\d+(?:,\d{3})*(?:\.\d+)?)",
            )
            if cash_kusd is not None:
                cash_musd = cash_kusd / 1000.0

        if short_term_investments_musd is None:
            short_kusd, _ = _extract_two_matches(
                text,
                r"Short-term investments\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+(\d+(?:,\d{3})*(?:\.\d+)?)",
            )
            if short_kusd is not None:
                short_term_investments_musd = short_kusd / 1000.0

        operating_cash_flow_kusd, _ = _extract_two_matches(
            text,
            r"Net cash provided by operating activities(?: \(GAAP\))?\s*\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)",
        )
        free_cash_flow_kusd, _ = _extract_two_matches(
            text,
            r"Free cash flow.*?\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)",
        )
        cap_software_kusd, _ = _extract_two_matches(
            text,
            r"Capitalized software development costs and purchases of intangible assets\s*\((\d+(?:,\d{3})*(?:\.\d+)?)\)\s+\((\d+(?:,\d{3})*(?:\.\d+)?)\)",
        )
        capex_kusd, _ = _extract_two_matches(
            text,
            r"Purchases of property and equipment\s*\((\d+(?:,\d{3})*(?:\.\d+)?)\)\s+\((\d+(?:,\d{3})*(?:\.\d+)?)\)",
        )
        net_income_kusd, _ = _extract_two_matches(
            text,
            r"Net income \(GAAP\).*?\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)",
        )
        stock_based_compensation_kusd, _ = _extract_two_matches(
            text,
            r"(?:Stock|Share)-based compensation expense\s*\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)",
        )
        diluted_eps = _extract_first_match(
            text,
            r"Net income per share attributable.*?diluted\s*\$?\s*(\d+(?:\.\d+)?)",
        )
        adjusted_ebitda_kusd, _ = _extract_two_matches(
            text,
            r"Adjusted EBITDA\s*\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s+\$?\s*(\d+(?:,\d{3})*(?:\.\d+)?)",
        )

        diluted_shares_m = None
        if net_income_kusd and diluted_eps and diluted_eps > 0:
            diluted_shares_m = net_income_kusd / diluted_eps / 1000.0

        return {
            "source_file": candidate,
            "cash_musd": _round_or_none(cash_musd, 1),
            "short_term_investments_musd": _round_or_none(short_term_investments_musd, 1),
            "operating_cash_flow_musd": _round_or_none((operating_cash_flow_kusd or 0) / 1000.0, 1)
            if operating_cash_flow_kusd is not None
            else None,
            "capitalized_software_musd": _round_or_none((cap_software_kusd or 0) / 1000.0, 1)
            if cap_software_kusd is not None
            else None,
            "capex_musd": _round_or_none((capex_kusd or 0) / 1000.0, 1) if capex_kusd is not None else None,
            "free_cash_flow_musd": _round_or_none((free_cash_flow_kusd or 0) / 1000.0, 1)
            if free_cash_flow_kusd is not None
            else None,
            "net_income_musd": _round_or_none((net_income_kusd or 0) / 1000.0, 1)
            if net_income_kusd is not None
            else None,
            "adjusted_ebitda_musd": _round_or_none((adjusted_ebitda_kusd or 0) / 1000.0, 1)
            if adjusted_ebitda_kusd is not None
            else None,
            "stock_based_compensation_musd": _round_or_none((stock_based_compensation_kusd or 0) / 1000.0, 1)
            if stock_based_compensation_kusd is not None
            else None,
            "diluted_shares_m": _round_or_none(diluted_shares_m, 3),
            "total_debt_musd": 0.0,
        }

    return DEFAULT_LATEST_FINANCIAL_CONTEXT.copy()


def _build_snapshot_map(quarterly_nowcast: dict[str, object] | None) -> dict[str, dict[str, object]]:
    snapshots = (quarterly_nowcast or {}).get("historical_snapshots") or []
    return {
        str(snapshot.get("quarter")): snapshot
        for snapshot in snapshots
        if snapshot.get("quarter")
    }


def _estimate_quarter_metric(
    quarter: str,
    snapshot_map: dict[str, dict[str, object]],
    labels_map: dict[str, dict[str, object]],
    *,
    actual_field: str,
    estimate_field: str,
) -> float | None:
    label_row = labels_map.get(quarter) or {}
    actual_value = _safe_float(label_row.get(actual_field))
    if actual_value is not None:
        return actual_value
    snapshot = snapshot_map.get(quarter) or {}
    return _safe_float(snapshot.get(estimate_field))


def build_dcf_valuation_package(
    quarterly_nowcast: dict[str, object] | None,
    reference_date: str | None = None,
) -> dict[str, object] | None:
    if not quarterly_nowcast:
        return None

    labels_df = _load_labels_df()
    if labels_df.empty:
        return None

    current_snapshot = (quarterly_nowcast.get("current_quarter") or {}).copy()
    current_quarter = str(
        current_snapshot.get("quarter")
        or quarterly_nowcast.get("metadata", {}).get("current_quarter")
        or ""
    ).strip()
    if not current_quarter:
        return None

    snapshot_map = _build_snapshot_map(quarterly_nowcast)
    labels_map = {
        str(row["quarter"]): row.to_dict()
        for _, row in labels_df.iterrows()
        if row.get("quarter")
    }

    trailing_quarters = _trailing_quarters(current_quarter, 4)
    revenue_ttm_values = [
        _estimate_quarter_metric(
            quarter,
            snapshot_map,
            labels_map,
            actual_field="actual_revenue_musd",
            estimate_field="estimated_revenue_musd",
        )
        for quarter in trailing_quarters
    ]
    ebitda_ttm_values = [
        _estimate_quarter_metric(
            quarter,
            snapshot_map,
            labels_map,
            actual_field="actual_adjusted_ebitda_musd",
            estimate_field="estimated_ebitda_musd",
        )
        for quarter in trailing_quarters
    ]

    revenue_ttm_estimated_musd = None
    if all(value is not None for value in revenue_ttm_values):
        revenue_ttm_estimated_musd = float(sum(value for value in revenue_ttm_values if value is not None))

    ebitda_ttm_estimated_musd = None
    if all(value is not None for value in ebitda_ttm_values):
        ebitda_ttm_estimated_musd = float(sum(value for value in ebitda_ttm_values if value is not None))

    yearly_actuals = _sum_actuals_by_year(labels_df)
    current_year = int(current_quarter[:4])
    previous_year = current_year - 1
    previous_year_revenue = _safe_float((yearly_actuals.get(previous_year) or {}).get("actual_revenue_musd"))
    previous_year_ebitda = _safe_float((yearly_actuals.get(previous_year) or {}).get("actual_adjusted_ebitda_musd"))

    previous_quarter = _previous_quarter(current_quarter)
    previous_label = labels_map.get(previous_quarter) or {}
    fy_guidance_revenue_musd = _safe_float(previous_label.get("guidance_fy_revenue_musd"))
    next_q_guidance_revenue_musd = _safe_float(previous_label.get("guidance_next_q_revenue_musd"))

    balance_context = _extract_latest_balance_and_cashflow_context()

    operating_cash_flow_musd = _safe_float(balance_context.get("operating_cash_flow_musd"))
    capitalized_software_musd = _safe_float(balance_context.get("capitalized_software_musd")) or 0.0
    capex_musd = _safe_float(balance_context.get("capex_musd")) or 0.0
    historical_fcf_musd = _safe_float(balance_context.get("free_cash_flow_musd"))
    diluted_shares_m = _safe_float(balance_context.get("diluted_shares_m"))
    cash_musd = _safe_float(balance_context.get("cash_musd")) or 0.0
    short_term_investments_musd = _safe_float(balance_context.get("short_term_investments_musd")) or 0.0
    stock_based_compensation_musd = _safe_float(balance_context.get("stock_based_compensation_musd")) or 0.0
    total_debt_musd = _safe_float(balance_context.get("total_debt_musd")) or 0.0

    net_cash_musd = cash_musd + short_term_investments_musd - total_debt_musd

    fcf_margin_base = None
    if historical_fcf_musd is not None and previous_year_revenue and previous_year_revenue > 0:
        fcf_margin_base = historical_fcf_musd / previous_year_revenue
    elif operating_cash_flow_musd is not None and previous_year_revenue and previous_year_revenue > 0:
        fcf_margin_base = (operating_cash_flow_musd - capitalized_software_musd - capex_musd) / previous_year_revenue
    elif previous_year_ebitda and previous_year_revenue and previous_year_revenue > 0:
        fcf_margin_base = min(0.40, max(0.18, (previous_year_ebitda / previous_year_revenue) * 0.85))

    dcf_revenue_base_musd = revenue_ttm_estimated_musd or previous_year_revenue
    dcf_ebitda_base_musd = ebitda_ttm_estimated_musd or previous_year_ebitda

    free_cash_flow_base_musd = None
    if dcf_revenue_base_musd and fcf_margin_base is not None:
        free_cash_flow_base_musd = dcf_revenue_base_musd * fcf_margin_base
    elif historical_fcf_musd is not None:
        free_cash_flow_base_musd = historical_fcf_musd

    free_cash_flow_base_less_sbc_musd = None
    if free_cash_flow_base_musd is not None:
        free_cash_flow_base_less_sbc_musd = max(0.0, free_cash_flow_base_musd - stock_based_compensation_musd)

    fy_growth_assumption = None
    if fy_guidance_revenue_musd and previous_year_revenue and previous_year_revenue > 0:
        fy_growth_assumption = fy_guidance_revenue_musd / previous_year_revenue - 1.0
    elif dcf_revenue_base_musd and previous_year_revenue and previous_year_revenue > 0:
        fy_growth_assumption = dcf_revenue_base_musd / previous_year_revenue - 1.0

    growth_rate = min(0.25, max(0.08, fy_growth_assumption if fy_growth_assumption is not None else 0.15))
    wacc = 0.1058
    terminal_growth = 0.03

    estimated_revenue_musd = _safe_float(current_snapshot.get("estimated_revenue_musd"))
    estimated_ebitda_musd = _safe_float(current_snapshot.get("estimated_ebitda_musd"))

    package: dict[str, object] = {
        "metadata": {
            "as_of_date": reference_date or quarterly_nowcast.get("metadata", {}).get("as_of_date"),
            "quarter": current_quarter,
            "source": "quarterly_nowcast + quarterly_labels_template + latest annual filing",
            "source_file": balance_context.get("source_file"),
        },
        "assumptions": {
            "growth_rate": _round_or_none(growth_rate, 4),
            "wacc": _round_or_none(wacc, 4),
            "terminal_growth": _round_or_none(terminal_growth, 4),
        },
        "anchors": {
            "estimated_revenue_current_quarter_musd": _round_or_none(estimated_revenue_musd, 1),
            "estimated_ebitda_current_quarter_musd": _round_or_none(estimated_ebitda_musd, 1),
            "next_q_guidance_reference_musd": _round_or_none(next_q_guidance_revenue_musd, 1),
            "fy_guidance_revenue_musd": _round_or_none(fy_guidance_revenue_musd, 1),
            "revenue_ttm_estimated_musd": _round_or_none(dcf_revenue_base_musd, 1),
            "ebitda_ttm_estimated_musd": _round_or_none(dcf_ebitda_base_musd, 1),
            "operating_cash_flow_musd": _round_or_none(operating_cash_flow_musd, 1),
            "capitalized_software_musd": _round_or_none(capitalized_software_musd, 1),
            "capex_musd": _round_or_none(capex_musd, 1),
            "free_cash_flow_historical_musd": _round_or_none(historical_fcf_musd, 1),
            "stock_based_compensation_musd": _round_or_none(stock_based_compensation_musd, 1),
            "free_cash_flow_base_musd": _round_or_none(free_cash_flow_base_musd, 1),
            "free_cash_flow_base_less_sbc_musd": _round_or_none(free_cash_flow_base_less_sbc_musd, 1),
            "fcf_margin_base": _round_or_none(fcf_margin_base, 4),
            "cash_musd": _round_or_none(cash_musd, 1),
            "short_term_investments_musd": _round_or_none(short_term_investments_musd, 1),
            "cash_and_investments_musd": _round_or_none(cash_musd + short_term_investments_musd, 1),
            "total_debt_musd": _round_or_none(total_debt_musd, 1),
            "net_cash_musd": _round_or_none(net_cash_musd, 1),
            "diluted_shares_m": _round_or_none(diluted_shares_m, 3),
            "previous_year_revenue_musd": _round_or_none(previous_year_revenue, 1),
            "previous_year_adjusted_ebitda_musd": _round_or_none(previous_year_ebitda, 1),
            "implied_fy_growth_rate": _round_or_none(fy_growth_assumption, 4),
        },
        "sensitivity": {
            "wacc_values": [round(value, 4) for value in [wacc - 0.01, wacc, wacc + 0.01]],
            "terminal_values": [round(value, 4) for value in [0.02, terminal_growth, 0.04]],
        },
        "assumption_notes": [
            "La base de revenus s'appuie sur les trois derniers trimestres publies et le trimestre en cours estime par le nowcast.",
            "Le FCF de base applique la marge de free cash flow observee sur le dernier exercice annualise disponible.",
            "La croissance annuelle part de la guidance FY du management quand elle est disponible, avec des bornes de prudence.",
            "La valorisation reste une lecture sous hypotheses, a completer par la sensibilite WACC / croissance terminale.",
        ],
    }
    return package


def save_dcf_valuation_package(package: dict[str, object]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    DCF_VALUATION_JSON_FILE.write_text(
        json.dumps(package, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return DCF_VALUATION_JSON_FILE


def generate_dcf_valuation_package(
    quarterly_nowcast: dict[str, object] | None,
    reference_date: str | None = None,
) -> dict[str, object] | None:
    package = build_dcf_valuation_package(quarterly_nowcast, reference_date=reference_date)
    if not package:
        return None
    save_dcf_valuation_package(package)
    return package
