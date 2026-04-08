"""ADK agent definitions for the Duolingo investor workflow."""

from __future__ import annotations

from google.adk import Agent

from ..config import GEMINI_MODEL
from .tools import (
    get_alternative_data_context,
    get_daily_signals_context,
    get_dcf_context,
    get_full_report_context,
    get_quarterly_nowcast_context,
    get_workbook_overview,
    run_report_quality_checks,
)


def build_daily_signals_agent() -> Agent:
    return Agent(
        name="daily_signals_agent",
        model=GEMINI_MODEL,
        description="Spécialiste du panel quotidien, des signaux financiers, et des lectures 7 jours / 30 jours.",
        instruction=(
            "Tu es l'analyste des signaux quotidiens Duolingo. "
            "Avant de répondre, appelle toujours get_daily_signals_context. "
            "Ta mission est de lire le panel du jour, d'expliquer ce qui change sur 7 jours et 30 jours, "
            "et de distinguer clairement le bruit quotidien d'une vraie inflexion. "
            "Réponds en français, avec un ton d'analyste buy-side, précis, compact et sans jargon inutile."
        ),
        tools=[get_daily_signals_context],
    )


def build_quarterly_nowcast_agent() -> Agent:
    return Agent(
        name="quarterly_nowcast_agent",
        model=GEMINI_MODEL,
        description="Spécialiste du nowcast trimestriel, des probabilités implicites, et de la qualité du snapshot.",
        instruction=(
            "Tu es l'analyste du nowcast trimestriel Duolingo. "
            "Avant de répondre, appelle toujours get_quarterly_nowcast_context. "
            "Tu dois expliquer les probabilités de beat revenus, beat EBITDA, guidance raise, "
            "ainsi que la qualité du snapshot, le niveau de confiance et la logique d'estimation. "
            "Si le trimestre n'est pas figé ou si le benchmark guidance manque, dis-le franchement."
        ),
        tools=[get_quarterly_nowcast_context],
    )


def build_alternative_data_agent() -> Agent:
    return Agent(
        name="alternative_data_agent",
        model=GEMINI_MODEL,
        description="Spécialiste des signaux externes gratuits : emploi, recherche, social, et momentum WoW.",
        instruction=(
            "Tu es l'analyste de l'onglet Alternative Data. "
            "Avant de répondre, appelle toujours get_alternative_data_context. "
            "Lis les signaux comme des indicateurs externes de momentum, pas comme des vérités absolues. "
            "Sois clair sur les sources stables, les signaux manquants, et la lecture week over week."
        ),
        tools=[get_alternative_data_context],
    )


def build_valuation_agent() -> Agent:
    return Agent(
        name="valuation_agent",
        model=GEMINI_MODEL,
        description="Spécialiste de la DCF, du prix cible, et de l'upside / downside implicite.",
        instruction=(
            "Tu es l'analyste valorisation Duolingo. "
            "Avant de répondre, appelle toujours get_dcf_context. "
            "Tu dois expliquer le prix cible, le cours actuel, l'upside/downside implicite, "
            "et les hypothèses centrales de croissance, WACC et terminale. "
            "Présente la DCF comme une lecture sous hypothèses, jamais comme une certitude."
        ),
        tools=[get_dcf_context],
    )


def build_report_quality_agent() -> Agent:
    return Agent(
        name="report_quality_agent",
        model=GEMINI_MODEL,
        description="Agent QA qui vérifie la structure du workbook et repère les dérives de données ou de snapshots.",
        instruction=(
            "Tu es le contrôleur qualité du rapport Duolingo. "
            "Avant de répondre, appelle run_report_quality_checks et, si utile, get_workbook_overview. "
            "Ta priorité est de détecter les problèmes concrets : onglets manquants, snapshots trimestriels fragiles, "
            "Alternative Data trop maigre, cours DCF absent, ou mémoire trimestrielle mal préservée. "
            "Réponds par constats clairs, ordonnés par gravité."
        ),
        tools=[run_report_quality_checks, get_workbook_overview],
    )


def build_root_agent() -> Agent:
    return Agent(
        name="duolingo_investor_supervisor",
        model=GEMINI_MODEL,
        description="Superviseur ADK du rapport Duolingo. Il délègue aux spécialistes quotidien, trimestriel, externe, valorisation et QA.",
        instruction=(
            "Tu es le superviseur analyste du rapport Duolingo. "
            "Tu aides à lire le workbook, à répondre aux questions investisseur, à détecter les angles morts, "
            "et à orienter vers le bon spécialiste. "
            "Quand la question est large, commence par appeler get_full_report_context. "
            "Quand la question est ciblée, délègue au sous-agent le plus pertinent. "
            "Ne jamais inventer une donnée absente. Si un signal est fragile ou incomplet, dis-le explicitement. "
            "Réponds en français, avec une structure propre et un ton d'analyste buy-side compact."
        ),
        tools=[get_full_report_context],
        sub_agents=[
            build_daily_signals_agent(),
            build_quarterly_nowcast_agent(),
            build_alternative_data_agent(),
            build_valuation_agent(),
            build_report_quality_agent(),
        ],
    )


root_agent = build_root_agent()


__all__ = [
    "build_daily_signals_agent",
    "build_quarterly_nowcast_agent",
    "build_alternative_data_agent",
    "build_valuation_agent",
    "build_report_quality_agent",
    "build_root_agent",
    "root_agent",
]
