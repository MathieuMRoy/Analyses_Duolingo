"""Experimental ADK workflow agents for deterministic orchestration."""

from __future__ import annotations

from google.adk.agents import ParallelAgent, SequentialAgent

from .agents import (
    build_alternative_data_agent,
    build_daily_signals_agent,
    build_quarterly_nowcast_agent,
    build_report_quality_agent,
    build_root_agent,
    build_valuation_agent,
)


def build_parallel_context_agent() -> ParallelAgent:
    """Run the independent specialist readers in parallel."""
    return ParallelAgent(
        name="parallel_context_agent",
        description=(
            "Workflow expérimental qui lance en parallèle les spécialistes quotidien, "
            "trimestriel, alternative data et valorisation."
        ),
        sub_agents=[
            build_daily_signals_agent(),
            build_quarterly_nowcast_agent(),
            build_alternative_data_agent(),
            build_valuation_agent(),
        ],
    )


def build_report_review_workflow() -> SequentialAgent:
    """Run a deterministic review flow: gather context, run QA, then hand off to the supervisor."""
    return SequentialAgent(
        name="report_review_workflow",
        description=(
            "Workflow expérimental qui exécute d'abord la collecte de lectures spécialisées, "
            "puis le QA, puis la synthèse par le superviseur."
        ),
        sub_agents=[
            build_parallel_context_agent(),
            build_report_quality_agent(),
            build_root_agent(),
        ],
    )


parallel_context_agent = build_parallel_context_agent()
report_review_workflow = build_report_review_workflow()


__all__ = [
    "build_parallel_context_agent",
    "build_report_review_workflow",
    "parallel_context_agent",
    "report_review_workflow",
]
