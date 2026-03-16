"""
==============================================================================
  Duolingo Super-User Engagement Analyzer (Modularized)
  -----------------------------------------------------
  Outil d'analyse de l'engagement des super-utilisateurs de Duolingo.

  Partie 0 : discovery.py (Initialisation et candidats)
  Partie 1 : scraper.py   (Collecte des streaks)
  Partie 2 : stats.py     (Calculs Pandas)
  Partie 3 : agent.py     (Rapport IA ADK)

  Usage :
      python main.py
==============================================================================
"""

import pandas as pd

from duolingo_analyzer.config import TARGET_USERS_FILE
from duolingo_analyzer.discovery import initialiser_cibles
from duolingo_analyzer.scraper import collecter_streaks_quotidiens
from duolingo_analyzer.stats import calculer_statistiques, sauvegarder_rapport_excel
from duolingo_analyzer.agent import generer_rapport_ia

if __name__ == "__main__":
    print("\n" + "🦉" * 30)
    print("  DUOLINGO SUPER-USER ENGAGEMENT ANALYZER")
    print("🦉" * 30 + "\n")

    # ── PARTIE 0 : Initialisation ─────────────────────────────────────────
    if not TARGET_USERS_FILE.exists():
        print("📋 Fichier target_users.csv non trouvé → Initialisation...\n")
        # Doit renvoyer le CSV complet qu'il vient de créer
        initialiser_cibles()
        
    print(f"📋 Chargement de {TARGET_USERS_FILE}...")
    df_users = pd.read_csv(TARGET_USERS_FILE)
    
    # Transformation en liste de dictionnaires pour le scraper
    utilisateurs_cibles = df_users.to_dict('records')
    print(f"   ✓ {len(utilisateurs_cibles)} utilisateurs chargés (avec cohortes).\n")

    if not utilisateurs_cibles:
        print("\n❌ Aucun utilisateur à analyser. Arrêt du script.")
        exit(1)

    # ── PARTIE 1 : Collecte des streaks ───────────────────────────────────
    collecter_streaks_quotidiens(utilisateurs_cibles)

    # ── PARTIE 2 : Calcul des statistiques ────────────────────────────────
    statistiques = calculer_statistiques()

    if not statistiques:
        print("\n❌ Impossible de calculer les statistiques. Arrêt.")
        exit(1)

    # ── PARTIE 3 : Génération du rapport IA ───────────────────────────────
    rapport_ia = generer_rapport_ia(statistiques)

    # ── Sauvegarde Excel Premium ──────────────────────────────────────────
    sauvegarder_rapport_excel(statistiques, rapport_ia)

    print("\n" + "🦉" * 30)
    print("  ✅ ANALYSE TERMINÉE AVEC SUCCÈS")
    print("🦉" * 30 + "\n")
