"""
Main entrypoint for the Duolingo engagement and nowcasting pipeline.
"""
from __future__ import annotations

import pandas as pd

from duolingo_analyzer.agent import generer_rapport_ia
from duolingo_analyzer.config import TARGET_USERS_FILE
from duolingo_analyzer.discovery import initialiser_cibles
from duolingo_analyzer.financial_signals import generate_financial_signal_package
from duolingo_analyzer.quarterly_nowcast import generate_quarterly_nowcast_package
from duolingo_analyzer.scraper import collecter_streaks_quotidiens, purger_anciennes_donnees
from duolingo_analyzer.report_builder import sauvegarder_rapport_excel
from duolingo_analyzer.stats import calculer_statistiques
from duolingo_analyzer.valuation_dcf import generate_dcf_valuation_package


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  DUOLINGO ENGAGEMENT + NOWCAST PIPELINE")
    print("=" * 60 + "\n")

    if not TARGET_USERS_FILE.exists():
        print("[INIT] target_users.csv introuvable, initialisation du panel...\n")
        initialiser_cibles()

    print(f"[LOAD] Chargement de {TARGET_USERS_FILE}...")
    df_users = pd.read_csv(TARGET_USERS_FILE)
    utilisateurs_cibles = df_users.to_dict("records")
    print(f"   OK - {len(utilisateurs_cibles)} utilisateurs charges.\n")

    if not utilisateurs_cibles:
        print("[ERREUR] Aucun utilisateur a analyser. Arret du script.")
        raise SystemExit(1)

    collecter_streaks_quotidiens(utilisateurs_cibles)
    purger_anciennes_donnees()

    statistiques = calculer_statistiques()
    if not statistiques:
        print("[ERREUR] Impossible de calculer les statistiques. Arret.")
        raise SystemExit(1)

    signaux_financiers = generate_financial_signal_package(statistiques.get("date_jour"))
    nowcast_trimestriel = generate_quarterly_nowcast_package(statistiques.get("date_jour"))
    valorisation_dcf = generate_dcf_valuation_package(nowcast_trimestriel, statistiques.get("date_jour"))
    rapport_ia = generer_rapport_ia(statistiques, signaux_financiers, nowcast_trimestriel)
    sauvegarder_rapport_excel(
        statistiques,
        rapport_ia,
        signaux_financiers,
        nowcast_trimestriel,
        valorisation_dcf,
    )

    print("\n" + "=" * 60)
    print("  OK - ANALYSE TERMINEE AVEC SUCCES")
    print("=" * 60 + "\n")
