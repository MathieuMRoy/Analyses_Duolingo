"""
Fonctions utilitaires partagées : parsing, formatage, texte.
Ce module centralise les helpers utilisés par stats.py, excel_dashboard.py
et les différents générateurs de rapports.
"""

import numbers
import re
import pandas as pd


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, numbers.Number):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.upper() in {"N/A", "NA"}:
            return None
        cleaned = raw.replace(",", ".")
        cleaned = cleaned.replace("XP", "").replace("xp", "")
        cleaned = cleaned.replace("%", "")
        cleaned = cleaned.replace("−", "-")
        # excel_dashboard version is stricter: cleans all non-math chars
        cleaned = re.sub(r"[^0-9\.\-\+]", "", cleaned)
        if cleaned in {"", "+", "-", "."}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def parse_bool_fraction(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, numbers.Number):
        return 1.0 if float(value) != 0 else 0.0
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"true", "vrai", "1", "yes", "oui"}:
            return 1.0
        if raw in {"false", "faux", "0", "no", "non", ""}:
            return 0.0
    return None


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.replace("**", "")
    text = str(value).replace("\r", "\n")
    text = text.replace("•", "-").replace("â€¢", "-")
    text = text.replace("…", "...").replace("â€¦", "...")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def truncate_text(text: str, max_chars: int) -> str:
    clean = normalize_text(text)
    if len(clean) <= max_chars:
        return clean
    shortened = clean[: max_chars - 3].rstrip(" ,;:-")
    return f"{shortened}..."


def compact_summary_text(
    text: object,
    max_sentences: int = 2,
    max_chars: int = 180,
    separator: str = " ",
) -> str:
    clean = normalize_text(text)
    if not clean:
        return "-"
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
    if not sentences:
        return clean

    selected: list[str] = []
    current_length = 0
    for sentence in sentences:
        candidate_length = current_length + (len(separator) if selected else 0) + len(sentence)
        if selected and candidate_length > max_chars:
            break
        selected.append(sentence)
        current_length = candidate_length
        if len(selected) >= max_sentences:
            break

    return separator.join(selected) if selected else sentences[0]


def compact_bullet_text(text: object, max_items: int = 2, max_chars: int = 95) -> str:
    clean = normalize_text(text)
    if not clean:
        return "-"

    items: list[str] = []
    for raw_line in clean.splitlines():
        candidate = raw_line.lstrip("-* ").strip()
        if candidate:
            items.append(candidate)

    if not items:
        items = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]

    compact_items: list[str] = []
    current_length = 0
    for item in items:
        candidate_length = current_length + (1 if compact_items else 0) + len(item)
        if compact_items and candidate_length > max_chars:
            break
        if item and item not in compact_items:
            compact_items.append(item)
            current_length = candidate_length
        if len(compact_items) >= max_items:
            break

    if not compact_items:
        return "-"
    return "\n".join(f"- {item}" for item in compact_items)


def pretty_fr_number(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        numeric = float(value)
    except Exception:
        return str(value)

    if digits == 0:
        return f"{int(round(numeric)):,}".replace(",", " ")
    return f"{numeric:,.{digits}f}".replace(",", " ").replace(".", ",")


def pretty_ratio_pct(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value) * 100:.{digits}f}%".replace(".", ",")
    except Exception:
        return "N/D"


def pretty_delta_pts(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value) * 100:+.{digits}f} pts".replace(".", ",")
    except Exception:
        return "N/D"


def pretty_score(value: object, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    try:
        return f"{float(value):.{digits}f} / 100".replace(".", ",")
    except Exception:
        return "N/D"


def format_estimation_vs_guidance_note(
    estimated_value: object,
    guidance_value: object,
    *,
    prefix: str = "Est.",
    guidance_label: str = "Guidance",
) -> str:
    estimated_text = (
        f"{pretty_fr_number(estimated_value, 1)} M$"
        if isinstance(estimated_value, numbers.Number)
        else "N/D"
    )
    guidance_text = (
        f"{pretty_fr_number(guidance_value, 1)} M$"
        if isinstance(guidance_value, numbers.Number)
        else "N/D"
    )
    return f"{prefix} {estimated_text} vs {guidance_label} {guidance_text}"


def build_quarterly_model_explainer(
    *,
    revenue_prob: object,
    guidance_prob: object,
    revenue_reference: object,
    drivers: list[object],
    risks: list[object],
) -> str:
    primary_driver = str(drivers[0]).lstrip("- ").rstrip(".") if drivers else "la dynamique récente du panel"
    primary_risk = str(risks[0]).lstrip("- ").rstrip(".") if risks else "les limites actuelles de calibration"

    base_sentence = (
        "Notre modèle trimestriel combine la monétisation, l'engagement, la rétention, "
        "le churn, les réactivations et la couverture du panel."
    )

    if isinstance(revenue_reference, numbers.Number):
        reference_text = f"{pretty_fr_number(revenue_reference, 1)} M$"
        revenue_sentence = (
            f"La probabilité implicite de battre les revenus du trimestre ressort à {pretty_ratio_pct(revenue_prob, 1)} ; "
            f"elle compare notre estimation à la référence interne du trimestre, actuellement ancrée sur la guidance management de {reference_text}."
        )
    else:
        revenue_sentence = (
            f"La probabilité implicite de battre les revenus du trimestre ressort à {pretty_ratio_pct(revenue_prob, 1)} ; "
            "elle s'appuie encore sur une référence interne de transition, faute de guidance exploitable dans l'historique."
        )

    g_prob_val = parse_float(guidance_prob)
    if g_prob_val is not None and g_prob_val >= 0.5:
        guidance_sentence = f"La probabilité implicite d'un relèvement de guidance ressort à {pretty_ratio_pct(guidance_prob, 1)} et reste surtout portée par {primary_driver}."
    else:
        guidance_sentence = f"La probabilité implicite d'un relèvement de guidance ressort à {pretty_ratio_pct(guidance_prob, 1)} et reste freinée par {primary_risk}."

    return base_sentence + " " + revenue_sentence + " " + guidance_sentence


def label_signal_bias(value: object) -> str:
    mapping = {
        "favorable": "Favorable",
        "neutral": "Neutre",
        "unfavorable": "Defavorable",
    }
    return mapping.get(str(value or "").strip().lower(), "N/D")


def label_confidence(value: object) -> str:
    mapping = {
        "high": "Elevee",
        "medium": "Moyenne",
        "low": "Faible",
    }
    return mapping.get(str(value or "").strip().lower(), "N/D")


def format_points(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:+.1f} pts"


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.1f}%"


def format_integer(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{int(round(float(value))):,}".replace(",", " ")


def format_streak(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.1f} j"


def format_xp(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{int(round(float(value))):,} XP".replace(",", " ")


def format_xp_delta(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    rounded = int(round(float(value)))
    sign = "+" if rounded > 0 else ""
    return f"{sign}{rounded:,} XP".replace(",", " ")


def delta_font_color(value: float | None, positive_is_good: bool = True) -> str:
    if value is None or pd.isna(value):
        return "666666"
    numeric = float(value)
    if numeric == 0:
        return "666666"
    if positive_is_good:
        return "008000" if numeric > 0 else "C00000"
    return "008000" if numeric < 0 else "C00000"


def weekly_insight_text(latest_week: pd.Series | None) -> str:
    if latest_week is None:
        return "La lecture Week over Week apparaitra ici des qu'une semaine de resume sera disponible."

    delta_xp = latest_week.get("Delta XP vs S-1")
    delta_conv = latest_week.get("Delta Conv. vs S-1")
    delta_abandon = latest_week.get("Delta Abandon vs S-1")
    delta_score = latest_week.get("Delta Score vs S-1")

    if all(pd.isna(value) for value in [delta_xp, delta_conv, delta_abandon, delta_score]):
        return (
            "La semaine en cours est bien calculee, mais il faut encore une semaine precedente "
            "pour afficher une comparaison Week over Week."
        )

    parts: list[str] = []
    if not pd.isna(delta_xp):
        parts.append(f"XP moyen {format_xp_delta(delta_xp)} vs semaine precedente")
    if not pd.isna(delta_conv):
        parts.append(f"conversion Super {format_points(delta_conv)}")
    if not pd.isna(delta_abandon):
        parts.append(f"taux d'abandon {format_points(delta_abandon)}")
    if not pd.isna(delta_score):
        parts.append(f"score d'engagement {format_points(delta_score)}")

    return "Cette semaine : " + " | ".join(parts) + "."
