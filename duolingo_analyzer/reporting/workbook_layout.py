"""Règles partagées de structure du classeur Excel."""

from __future__ import annotations

from openpyxl import Workbook

from ..columns import (
    AI_SHEET,
    ALT_DATA_RAW_SHEET,
    ALT_DATA_SHEET,
    BAD_SHEET_NAMES,
    CHART_DATA_SHEET,
    DCF_SHEET,
    GLOSSAIRE_RAW_SHEET,
    GLOSSAIRE_SHEET,
    LEGACY_SHEET_NAMES,
    QUARTERLY_RAW_SHEET,
    QUARTERLY_SHEET,
    SIGNALS_RAW_SHEET,
    SIGNALS_SHEET,
    SUMMARY_SHEET,
    TRENDS_SHEET,
)
from .workbook_postprocess import hide_sheets, remove_sheets, reorder_sheets


def removable_sheet_names() -> list[str]:
    """Feuilles obsolètes ou mal encodées à retirer systématiquement."""
    return [*BAD_SHEET_NAMES, *LEGACY_SHEET_NAMES, AI_SHEET]


def hidden_sheet_names(include_chart_data: bool = True) -> list[str]:
    """Feuilles techniques à masquer dans le workbook final."""
    names = [
        SIGNALS_RAW_SHEET,
        QUARTERLY_RAW_SHEET,
        ALT_DATA_RAW_SHEET,
        GLOSSAIRE_RAW_SHEET,
    ]
    if include_chart_data:
        names.append(CHART_DATA_SHEET)
    return names


def preferred_sheet_order(include_dcf: bool) -> list[str]:
    """Ordre visible des onglets, suivi des feuilles techniques."""
    ordered = [
        SUMMARY_SHEET,
        SIGNALS_SHEET,
        QUARTERLY_SHEET,
        ALT_DATA_SHEET,
        TRENDS_SHEET,
    ]
    if include_dcf:
        ordered.append(DCF_SHEET)
    ordered.extend(
        [
            GLOSSAIRE_SHEET,
            SIGNALS_RAW_SHEET,
            QUARTERLY_RAW_SHEET,
            ALT_DATA_RAW_SHEET,
            GLOSSAIRE_RAW_SHEET,
            CHART_DATA_SHEET,
        ]
    )
    return ordered


def apply_standard_workbook_layout(wb: Workbook, *, include_dcf: bool, hide_chart_data: bool) -> None:
    """Applique le ménage structurel standard du workbook."""
    remove_sheets(wb, removable_sheet_names())
    hide_sheets(wb, hidden_sheet_names(include_chart_data=hide_chart_data))
    reorder_sheets(wb, preferred_sheet_order(include_dcf=include_dcf))
