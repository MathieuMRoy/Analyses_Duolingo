"""Helpers partagés pour le rendu textuel des feuilles Excel."""

from __future__ import annotations

from ..utils import (
    compact_bullet_text,
    compact_summary_text,
    label_confidence,
    label_signal_bias,
    pretty_delta_pts,
    pretty_fr_number,
    pretty_ratio_pct,
    pretty_score,
)


def build_render_helpers() -> dict[str, object]:
    """Retourne les helpers textuels passés aux renderers de feuilles."""
    return {
        "label_signal_bias": label_signal_bias,
        "label_confidence": label_confidence,
        "pretty_fr_number": pretty_fr_number,
        "pretty_ratio_pct": pretty_ratio_pct,
        "pretty_score": pretty_score,
        "pretty_delta_pts": pretty_delta_pts,
        "compact_summary_text": compact_summary_text,
        "compact_bullet_text": compact_bullet_text,
    }
