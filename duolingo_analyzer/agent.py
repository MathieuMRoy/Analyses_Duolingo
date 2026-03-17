"""
Partie 3 : Agent IA Analyste
Génère le rapport narratif via Google GenAI.
"""
from datetime import datetime
from google import genai

from .config import (
    GOOGLE_API_KEY,
    GEMINI_MODEL,
)


def _safe_number(value, default=0.0):
    """Convertit une valeur potentiellement None en nombre pour le formatage."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _executer_agent_adk(system_prompt: str, user_prompt: str) -> str:
    """
    Configure et exécute l'agent via le SDK google.genai officiel.
    """
    print(f"\n  🤖 Initialisation de l'agent GenAI (modèle: {GEMINI_MODEL})...")
    
    try:
        client = genai.Client(api_key=GOOGLE_API_KEY)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.4,
            ),
        )
        return response.text
    except Exception as e:
        print(f"\n  [ERREUR GENAI] {e}")
        print("     Génération d'un rapport placeholder à la place.\n")
        return None


def generer_rapport_ia(stats: dict) -> str:
    """
    PARTIE 3 : Génère un rapport d'engagement formaté via l'Agent.
    Sauvegarde le résultat dans rapport_quotidien.txt.
    """
    print("============================================================")
    print("  PARTIE 3 — AGENT IA ANALYSTE (Google GenAI)")
    print("============================================================")

    system_prompt = (
        "Tu es un Analyste Financier Senior (Buy-Side) couvrant l'action Duolingo (NASDAQ: DUOL). "
        "Ton objectif est d'analyser les indicateurs d'engagement quotidiens (Retention, taux d'abandon, DAU) "
        "pour anticiper les résultats financiers du trimestre en cours (Q1 2026) et guider les décisions d'investissement. "
        "Produis un rapport structuré avec des sections claires pour une intégration dans un tableau de bord Excel. "
        "UTILISE EXCLUSIVEMENT les balises suivantes pour délimiter tes sections :\n"
        "[TITRE] (Un titre financier percutant (ex: 'DUOL - Mise à jour Q1 2026'))\n"
        "[RESUME] (Résumé de la thèse d'investissement basée sur l'engagement en 2-3 phrases)\n"
        "[TENDANCES] (Impact des metriques de retention et de taux d'abandon sur le modele de revenus futurs)\n"
        "[ATTENTION] (Risques d'exécution, essoufflement de l'engagement ou baisse d'utilisateurs monétisables)\n"
        "[CONSEILS] (Recommandations pour les investisseurs : anticiper une hausse/baisse, points à surveiller)\n"
        "Sois pro, direct, utilise un vocabulaire financier (guidance, top-line, MAU/DAU, conversion) et n'ajoute aucun texte en dehors des balises."
    )

    date = stats.get("date_jour", datetime.now().strftime("%Y-%m-%d"))
    moy = _safe_number(stats.get("moyenne_streak_jour"), 0)
    actifs = int(_safe_number(stats.get("utilisateurs_actifs"), 0))
    tombes = int(_safe_number(stats.get("streaks_tombes_zero"), 0))
    total = int(_safe_number(stats.get("nb_profils_jour"), 0))
    
    retention = _safe_number(stats.get("taux_retention"), 0)
    churn = _safe_number(stats.get("taux_churn"), 0)
    sante = _safe_number(stats.get("score_sante_jour"), 0)
    conversion = _safe_number(stats.get("taux_conversion_plus"), 0)
    delta_xp = _safe_number(stats.get("delta_xp_moyen"), 0)
    cohortes = stats.get("cohortes", {})
    
    hier_info = ""
    if stats.get("moyenne_streak_hier") is not None:
        evolution = _safe_number(moy, 0) - _safe_number(stats["moyenne_streak_hier"], 0)
        hier_info = f"\n• Évolution moyenne vs hier : {evolution:+.1f} jours"

    user_prompt = (
        f"Analyse ces métriques d'engagement d'un panel d'utilisateurs Duolingo pour le {date} "
        f"dans l'optique des futurs résultats Q1 2026 :\n\n"
        f"• Taille du panel analysé : {total} utilisateurs (répartis en 3 cohortes XP)\n"
        f"• Score de Santé (Utilisateurs Actifs / Panel) : {sante:.1f}%\n"
        f"• Taux de Rétention (vs hier) : {retention:.1f}%\n"
        f"• Taux d'Abandon Global : {churn:.2f}%\n"
        f"  - Abandon Débutants : {_safe_number(cohortes.get('Debutants', {}).get('churn'), 0):.1f}%\n"
        f"  - Abandon Standard : {_safe_number(cohortes.get('Standard', {}).get('churn'), 0):.1f}%\n"
        f"  - Abandon Super-Actifs : {_safe_number(cohortes.get('Super-Actifs', {}).get('churn'), 0):.1f}%\n"
        f"• Taux de Conversion Estimé (Super Duolingo) : {conversion:.1f}%\n"
        f"• Intensité d'Engagement (Delta XP Moyen) : +{delta_xp:.0f} XP/jour\n"
        f"• Moyenne des streaks : {moy:.1f} jours{hier_info}\n\n"
        "Génère le rapport structuré avec les balises demandées en adoptant un ton d'analyste financier."
    )

    print(f"\n  📝 System Prompt :\n     {system_prompt[:85]}...")
    print(f"\n  📝 User Prompt :\n     {user_prompt[:80]}...\n")

    rapport = None

    if not GOOGLE_API_KEY:
        print("  ⚠️ Clé GOOGLE_API_KEY manquante dans l'environnement (.env).")
        print("  Veuillez la configurer pour utiliser le véritable Agent.")
    else:
        rapport = _executer_agent_adk(system_prompt, user_prompt)

    if not rapport:
        rapport = (
            f"📈 RAPPORT D'ENGAGEMENT (GÉNÉRÉ LOCALEMENT - PLACEHOLDER)\n\n"
            f"Analyse du {date}:\n"
            f"Sur notre panel de {total} utilisateurs, la longueur moyenne de\n"
            f"série est de {moy:.1f} jours. {actifs} utilisateurs restent engagés.\n"
            f"Aujourd'hui, {tombes} utilisateurs ont perdu leur rythme d'apprentissage."
        )

    print("\n  ✅ Analyse IA générée avec succès (prête pour Excel).")

    return rapport