"""Local CLI to query the Duolingo ADK supervisor."""

from __future__ import annotations

import argparse
import sys

from duolingo_analyzer.adk.runner import run_adk_prompt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Interroger localement l'agent ADK Duolingo sur le rapport investisseur."
    )
    parser.add_argument(
        "prompt",
        nargs="*",
        help="Question à poser à l'agent. Si vide, un prompt par défaut sera utilisé.",
    )
    parser.add_argument("--user-id", default="local_user", help="Identifiant logique de session utilisateur.")
    parser.add_argument("--session-id", default=None, help="Session ID optionnel pour garder le même thread.")
    args = parser.parse_args()

    prompt = " ".join(args.prompt).strip() or (
        "Donne-moi un briefing investisseur compact sur l'état actuel du rapport Duolingo."
    )

    try:
        answer = run_adk_prompt(prompt, user_id=args.user_id, session_id=args.session_id)
    except Exception as exc:
        print(f"[ERREUR ADK] {exc}")
        return 1

    print(answer or "[AUCUNE REPONSE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
