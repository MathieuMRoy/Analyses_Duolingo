from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from pypdf import PdfReader


BASE_DIR = Path(__file__).resolve().parents[1]
DOC_DIR = BASE_DIR / "financial_docs"
LABELS_FILE = DOC_DIR / "quarterly_labels_template.csv"


QUARTER_SPECS = [
    {
        "quarter": "2024Q1",
        "calendar_quarter_end": "2024-03-31",
        "earnings_release_date": "2024-05-08",
        "shareholder_letter": "Q1FY24 Shareholder Letter.pdf",
    },
    {
        "quarter": "2024Q2",
        "calendar_quarter_end": "2024-06-30",
        "earnings_release_date": "2024-08-07",
        "shareholder_letter": "Q2FY24 Duolingo 06-30-24 Shareholder Letter.pdf",
    },
    {
        "quarter": "2024Q3",
        "calendar_quarter_end": "2024-09-30",
        "earnings_release_date": "2024-11-06",
        "shareholder_letter": "Q3FY24 Duolingo 09-30-24 Shareholder Letter.pdf",
    },
    {
        "quarter": "2024Q4",
        "calendar_quarter_end": "2024-12-31",
        "earnings_release_date": "2025-02-27",
        "shareholder_letter": "Q4FY24 Duolingo 12-31-24 Shareholder Letter.pdf",
    },
    {
        "quarter": "2025Q1",
        "calendar_quarter_end": "2025-03-31",
        "earnings_release_date": "2025-05-01",
        "shareholder_letter": "Q1FY25 Duolingo 3-31-25 Shareholder Letter - Final 05.01.2025.pdf",
    },
    {
        "quarter": "2025Q2",
        "calendar_quarter_end": "2025-06-30",
        "earnings_release_date": "2025-08-06",
        "shareholder_letter": "Q2FY25 Duolingo 6-30-25 Shareholder Letter_Final.pdf",
    },
    {
        "quarter": "2025Q3",
        "calendar_quarter_end": "2025-09-30",
        "earnings_release_date": "2025-11-05",
        "shareholder_letter": "Q3FY25 Duolingo 9-30-25 Shareholder Letter Final.pdf",
    },
    {
        "quarter": "2025Q4",
        "calendar_quarter_end": "2025-12-31",
        "earnings_release_date": "2026-02-26",
        "shareholder_letter": "Q4FY25 Duolingo 12-31-25 Shareholder Letter (1).pdf",
    },
]


def _read_pdf_text(path: Path) -> str:
    reader = PdfReader(str(path))
    text = "\n".join((page.extract_text() or "") for page in reader.pages)
    return text.replace("\u00a0", " ")


def _normalized_lines(text: str) -> list[str]:
    return [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]


def _head_lines(text: str, max_lines: int = 260) -> list[str]:
    return _normalized_lines(text)[:max_lines]


def _clean_number(value: str) -> float:
    return float(value.replace(",", "").replace("$", ""))


def _extract_second_number_from_line(lines: list[str], keywords: tuple[str, ...]) -> float | None:
    lower_keywords = tuple(keyword.lower() for keyword in keywords)
    for line in lines:
        lowered = line.lower()
        if not all(keyword in lowered for keyword in lower_keywords):
            continue
        # Avoid guidance ranges such as "$238.5 - $241.5".
        if " - $" in line or "$" in line and " - " in line:
            continue
        numbers = re.findall(r"\$?(\d+(?:,\d{3})*(?:\.\d+)?)", line)
        if len(numbers) >= 2:
            return _clean_number(numbers[1])
    return None


def _extract_quarter_value_from_multi_period_line(
    lines: list[str],
    keywords: tuple[str, ...],
) -> float | None:
    lower_keywords = tuple(keyword.lower() for keyword in keywords)
    for line in lines:
        lowered = line.lower()
        if not all(keyword in lowered for keyword in lower_keywords):
            continue
        numbers = re.findall(r"\$?(\d+(?:,\d{3})*(?:\.\d+)?)", line)
        if len(numbers) >= 2:
            return _clean_number(numbers[1])
    return None


def _extract_first_number_from_line(lines: list[str], keywords: tuple[str, ...]) -> float | None:
    lower_keywords = tuple(keyword.lower() for keyword in keywords)
    for line in lines:
        lowered = line.lower()
        if not all(keyword in lowered for keyword in lower_keywords):
            continue
        numbers = re.findall(r"\$?(\d+(?:,\d{3})*(?:\.\d+)?)", line)
        if numbers:
            return _clean_number(numbers[0])
    return None


def _extract_single_money_after_phrase(text: str, phrases: tuple[str, ...]) -> float | None:
    for phrase in phrases:
        pattern = rf"{phrase}\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)"
        match = re.search(pattern, text, re.I)
        if match:
            return _clean_number(match.group(1))
    return None


def _extract_single_number_after_phrase(text: str, phrases: tuple[str, ...]) -> float | None:
    for phrase in phrases:
        pattern = rf"{phrase}\s+(\d+(?:,\d{{3}})*(?:\.\d+)?)\s+million"
        match = re.search(pattern, text, re.I)
        if match:
            return _clean_number(match.group(1))
    return None


def _extract_actual_eps(text: str) -> float | None:
    patterns = [
        r"diluted\s+\$?(\d+(?:\.\d+)?)\s+\$?(\d+(?:\.\d+)?)",
        r"Net income per share attributable to Class A and Class B common stockholders, diluted\s+\$?(\d+(?:\.\d+)?)\s+\$?(\d+(?:\.\d+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return _clean_number(match.group(2))
    return None


def _extract_guidance_midpoints(text: str, metric_label: str) -> tuple[float | None, float | None]:
    # Guidance tables can show:
    # - quarter range + FY range: "Revenues $238.5 - $241.5 $962 - $968"
    # - quarter point + FY range: "Revenues $288.5 $1,197 - $1,221"
    # - quarter point + FY point:  "Revenues $288.5 $1,209.0"
    guidance_text = _extract_guidance_section(text)
    escaped_label = re.escape(metric_label)

    range_range = rf"{escaped_label}\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s*-\s*\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s*-\s*\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)"
    match = re.search(range_range, guidance_text, re.I)
    if match:
        q_low, q_high, fy_low, fy_high = (_clean_number(value) for value in match.groups())
        return round((q_low + q_high) / 2.0, 3), round((fy_low + fy_high) / 2.0, 3)

    point_range = rf"{escaped_label}\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s*-\s*\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)"
    match = re.search(point_range, guidance_text, re.I)
    if match:
        quarter_value, fy_low, fy_high = (_clean_number(value) for value in match.groups())
        return round(quarter_value, 3), round((fy_low + fy_high) / 2.0, 3)

    point_point = rf"{escaped_label}\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)\s+\$?(\d+(?:,\d{{3}})*(?:\.\d+)?)"
    match = re.search(point_point, guidance_text, re.I)
    if match:
        quarter_value, fy_value = (_clean_number(value) for value in match.groups())
        return round(quarter_value, 3), round(fy_value, 3)

    return None, None


def _extract_guidance_section(text: str) -> str:
    match = re.search(r"(Q\d(?:\s+and\s+FY)?\s+\d{4}\s+Guidance.*)", text, re.I | re.S)
    if match:
        return match.group(1)[:5000]
    match = re.search(r"(Guidance Commentary.*)", text, re.I | re.S)
    if match:
        return match.group(1)[:4000]
    return text[-5000:]


def _extract_guidance_signal(text: str) -> str | None:
    lowered = _extract_guidance_section(text).lower()
    if "raising" in lowered or "raised" in lowered:
        return "Raised"
    if "lowering" in lowered or "reduced" in lowered or "reduce" in lowered:
        return "Lowered"
    if "reaffirm" in lowered or "maintain" in lowered or "unchanged" in lowered:
        return "Maintained"
    if "continue to expect" in lowered:
        return "Maintained"
    return None


def _extract_metrics_for_letter(path: Path) -> dict[str, object]:
    text = _read_pdf_text(path)
    lines = _head_lines(text)
    is_q4_fy_letter = "Q4 / FY" in text[:1000]

    actual_revenue = _extract_single_money_after_phrase(
        text,
        (
            "Total revenues were",
            "In Q1, revenues were",
            "In Q2, revenues were",
            "In Q3, revenues were",
            "In Q4, revenues were",
        ),
    )
    if actual_revenue is None:
        if is_q4_fy_letter:
            actual_revenue = _extract_first_number_from_line(lines, ("revenue",))
        else:
            actual_revenue = _extract_second_number_from_line(lines, ("revenue",))

    actual_paid_subscribers = _extract_single_number_after_phrase(
        text,
        ("Paid subscribers totaled",),
    )
    if actual_paid_subscribers is None:
        actual_paid_subscribers = _extract_second_number_from_line(lines, ("paid subscribers",))

    actual_adjusted_ebitda = _extract_single_money_after_phrase(
        text,
        ("Adjusted EBITDA was",),
    )
    if actual_adjusted_ebitda is None:
        if is_q4_fy_letter:
            actual_adjusted_ebitda = _extract_first_number_from_line(lines, ("adjusted ebitda",))
        else:
            actual_adjusted_ebitda = _extract_second_number_from_line(lines, ("adjusted ebitda",))

    actual_subscription_revenue = _extract_quarter_value_from_multi_period_line(lines, ("subscription $",))
    if actual_subscription_revenue is None:
        actual_subscription_revenue = _extract_quarter_value_from_multi_period_line(
            lines,
            ("subscription revenues $",),
        )
    if actual_subscription_revenue is None:
        actual_subscription_revenue = _extract_second_number_from_line(lines, ("subscription revenues",))

    actual_eps = _extract_actual_eps(text)
    guidance_next_q_revenue, guidance_fy_revenue = _extract_guidance_midpoints(text, "Revenues")
    _, _ = _extract_guidance_midpoints(text, "Bookings")
    guidance_signal = _extract_guidance_signal(text)

    missing = [
        name
        for name, value in {
            "actual_revenue_musd": actual_revenue,
            "actual_adjusted_ebitda_musd": actual_adjusted_ebitda,
            "actual_paid_subscribers_m": actual_paid_subscribers,
            "actual_subscription_revenue_musd": actual_subscription_revenue,
            "actual_eps": actual_eps,
        }.items()
        if value is None
    ]

    note_parts = ["Auto-extracted from shareholder letter"]
    if guidance_next_q_revenue is not None or guidance_fy_revenue is not None:
        note_parts.append("guidance stored as midpoint of published range")
    if missing:
        note_parts.append("missing fields: " + ", ".join(missing))

    return {
        "actual_revenue_musd": actual_revenue,
        "actual_eps": actual_eps,
        "actual_adjusted_ebitda_musd": actual_adjusted_ebitda,
        "actual_paid_subscribers_m": actual_paid_subscribers,
        "actual_subscription_revenue_musd": actual_subscription_revenue,
        "guidance_next_q_revenue_musd": guidance_next_q_revenue,
        "guidance_fy_revenue_musd": guidance_fy_revenue,
        "guidance_signal": guidance_signal,
        "source_actuals": path.name,
        "notes": " | ".join(note_parts),
    }


def build_quarterly_actuals() -> pd.DataFrame:
    if LABELS_FILE.exists():
        df = pd.read_csv(LABELS_FILE)
    else:
        df = pd.DataFrame({"quarter": [spec["quarter"] for spec in QUARTER_SPECS]})

    # The template starts mostly empty, so pandas often infers float dtypes.
    # Switch to object before mixed string/number assignments.
    df = df.astype(object)

    for spec in QUARTER_SPECS:
        quarter = spec["quarter"]
        path = DOC_DIR / spec["shareholder_letter"]
        if not path.exists():
            continue

        extracted = _extract_metrics_for_letter(path)

        if "quarter" not in df.columns:
            df["quarter"] = ""
        if quarter not in set(df["quarter"].astype(str)):
            df.loc[len(df)] = {"quarter": quarter}

        mask = df["quarter"].astype(str) == quarter
        df.loc[mask, "calendar_quarter_end"] = spec["calendar_quarter_end"]
        df.loc[mask, "earnings_release_date"] = spec["earnings_release_date"]
        for key, value in extracted.items():
            df.loc[mask, key] = value

    ordered_columns = [
        "quarter",
        "calendar_quarter_end",
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
    ]
    for column in ordered_columns:
        if column not in df.columns:
            df[column] = ""

    df = df[ordered_columns].sort_values("quarter").reset_index(drop=True)
    df.to_csv(LABELS_FILE, index=False)
    return df


if __name__ == "__main__":
    df = build_quarterly_actuals()
    print(df.to_string(index=False))
