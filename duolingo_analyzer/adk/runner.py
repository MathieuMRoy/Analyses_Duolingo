"""Local runner helpers for the Duolingo ADK companion."""

from __future__ import annotations

from datetime import datetime

from google.adk import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from ..config import GOOGLE_API_KEY
from .agents import build_root_agent


APP_NAME = "duolingo_investor_companion"


def _default_session_id() -> str:
    return f"session-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def build_local_runner(
    *,
    user_id: str = "local_user",
    session_id: str | None = None,
) -> tuple[Runner, str]:
    """Create a local ADK runner backed by an in-memory session service."""
    if not GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY manquante. Impossible de lancer l'agent ADK.")

    session_id = session_id or _default_session_id()
    session_service = InMemorySessionService()
    session_service.create_session(app_name=APP_NAME, user_id=user_id, session_id=session_id)
    runner = Runner(agent=build_root_agent(), app_name=APP_NAME, session_service=session_service)
    return runner, session_id


def run_adk_prompt(
    prompt: str,
    *,
    user_id: str = "local_user",
    session_id: str | None = None,
) -> str:
    """Run the Duolingo ADK supervisor on a single prompt and return the final answer."""
    runner, resolved_session_id = build_local_runner(user_id=user_id, session_id=session_id)
    content = types.Content(role="user", parts=[types.Part(text=prompt)])
    final_answer = ""

    for event in runner.run(user_id=user_id, session_id=resolved_session_id, new_message=content):
        if event.is_final_response() and event.content and event.content.parts:
            final_answer = (event.content.parts[0].text or "").strip()

    return final_answer


__all__ = ["APP_NAME", "build_local_runner", "run_adk_prompt"]
