"""
Part 3: Financial interpretation agent.
Generates an investor-style narrative via Google GenAI.
"""
from __future__ import annotations

from datetime import datetime

from google import genai

from .config import GEMINI_MODEL, GOOGLE_API_KEY


def _safe_number(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_ratio_pct(value) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "N/D"


def _format_money_musd(value) -> str:
    if value is None:
        return "N/D"
    try:
        return f"{float(value):.1f} M$"
    except (TypeError, ValueError):
        return "N/D"


def _executer_agent_adk(system_prompt: str, user_prompt: str) -> str | None:
    print(f"\n  [IA] Initialisation de l'agent GenAI (modele: {GEMINI_MODEL})...")

    try:
        client = genai.Client(api_key=GOOGLE_API_KEY)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=genai.types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.7,
            ),
        )
        return response.text
    except Exception as exc:
        print(f"\n  [ERREUR GENAI] {exc}")
        print("     Generation d'un rapport placeholder a la place.\n")
        return None


def _build_financial_signal_prompt(
    signal_package: dict,
    quarterly_nowcast: dict | None = None,
) -> tuple[str, str]:
    metadata = signal_package.get("metadata", {})
    panel = signal_package.get("panel", {})
    business = signal_package.get("business_signals", {})
    proxy = signal_package.get("financial_proxy_signals", {})
    quarterly_meta = (quarterly_nowcast or {}).get("metadata", {})
    quarterly_model = (quarterly_nowcast or {}).get("model_output", {})
    quarterly_current = (quarterly_nowcast or {}).get("current_quarter", {})
    quarterly_readiness = (quarterly_nowcast or {}).get("labels_readiness", {})

    quarterly_mode = quarterly_nowcast is not None
    sections_instruction = "[RESUME] [TENDANCES] [ATTENTION] [CONSEILS]"
    if quarterly_mode:
        sections_instruction += " [MODELE] [MODELE_TENDANCES] [MODELE_RISQUES]"

    system_prompt = (
        "Tu es un analyste buy-side specialise en signaux avances de monetisation, "
        "d'engagement et de retention. Tu recois un paquet de signaux structures derives "
        "d'un panel quotidien Duolingo et tu dois l'interpreter comme un nowcast business "
        "et financier. Tu ne dois pas inventer de probabilites supervisees si elles sont absentes. "
        "Tu restes prudent, tu relies les comportements utilisateurs aux implications business "
        f"et tu produis uniquement les sections suivantes, dans cet ordre exact : {sections_instruction}. "
        "Le rendu final sera insere dans des cases Excel de taille fixe : chaque section doit tenir "
        "en 3 a 4 lignes maximum. Utilise un francais fluide, professionnel et naturel. "
        "Evite de repeter les chiffres bruts si tu peux les interpreter. "
        "[RESUME] : 1 a 2 phrases maximum, lecture du jour. "
        "[TENDANCES] : 2 puces maximum, tres courtes, uniquement ce qui s'ameliore. "
        "[ATTENTION] : 2 puces maximum, tres courtes, uniquement les risques ou faiblesses. "
        "[CONSEILS] : 1 a 2 phrases maximum, conclusion actionnable. "
    )
    if quarterly_mode:
        system_prompt += (
            "[MODELE] : 2 phrases maximum. Explique clairement sur quels signaux repose le nowcast "
            "trimestriel et ce que signifient les probabilites de beat revenus, de beat EBITDA "
            "et de guidance raise. "
            "[MODELE_TENDANCES] : 2 puces maximum, tres courtes, focalisees sur les drivers trimestriels. "
            "[MODELE_RISQUES] : 2 puces maximum, tres courtes, focalisees sur les risques trimestriels. "
            "N'utilise jamais le mot 'proxy'. Pas de markdown decoratif, pas de gras, pas de jargon inutile. "
            "Le ton doit ressembler a une note d'analyste concise, pas a un resume scolaire."
        )

    user_prompt = (
        f"Date de reference: {metadata.get('as_of_date', 'N/D')}\n"
        f"Phase systeme: {metadata.get('phase', 'N/D')}\n"
        f"Jours observes dans l'historique: {metadata.get('observed_days', 'N/D')}\n"
        f"Panel cible: {panel.get('target_panel_size', 'N/D')}\n"
        f"Utilisateurs observes aujourd'hui: {panel.get('observed_users_today', 'N/D')}\n"
        f"Couverture panel: {_format_ratio_pct(panel.get('coverage_ratio'))}\n\n"
        f"Active rate: {_format_ratio_pct(business.get('active_rate'))}\n"
        f"Average streak: {business.get('avg_streak', 'N/D')}\n"
        f"XP delta mean: {business.get('xp_delta_mean', 'N/D')}\n"
        f"Churn rate: {_format_ratio_pct(business.get('churn_rate'))}\n"
        f"Reactivation rate: {_format_ratio_pct(business.get('reactivation_rate'))}\n"
        f"Super rate: {_format_ratio_pct(business.get('super_rate'))}\n"
        f"Max rate: {_format_ratio_pct(business.get('max_rate'))}\n"
        f"High-value retention rate: {_format_ratio_pct(business.get('high_value_retention_rate'))}\n\n"
        f"Engagement quality index: {proxy.get('engagement_quality_index', 'N/D')}\n"
        f"Engagement quality trend: {_format_ratio_pct(proxy.get('engagement_quality_trend'))}\n"
        f"Premium momentum 14d: {_format_ratio_pct(proxy.get('premium_momentum_14d'))}\n"
        f"Max momentum 14d: {_format_ratio_pct(proxy.get('max_momentum_14d'))}\n"
        f"Churn trend 14d: {_format_ratio_pct(proxy.get('churn_trend_14d'))}\n"
        f"Reactivation trend 7d: {_format_ratio_pct(proxy.get('reactivation_trend_7d'))}\n"
        f"High-value retention trend: {_format_ratio_pct(proxy.get('high_value_retention_trend'))}\n"
        f"Subscription momentum proxy: {proxy.get('subscription_momentum_proxy', 'N/D')}\n"
        f"Monetization momentum index: {proxy.get('monetization_momentum_index', 'N/D')}\n"
        f"Growth acceleration 7d: {proxy.get('growth_acceleration_7d', 'N/D')}\n"
        f"Signal bias: {proxy.get('signal_bias', 'neutral')}\n"
        f"Confidence level: {proxy.get('confidence_level', 'low')}\n"
        f"Revenue beat probability: {proxy.get('revenue_beat_probability', 'N/D')}\n"
        f"EBITDA beat probability: {proxy.get('ebitda_beat_probability', 'N/D')}\n"
        f"Guidance raise probability: {proxy.get('guidance_raise_probability', 'N/D')}\n"
        f"Main drivers: {', '.join(proxy.get('main_drivers', []))}\n"
        f"Main risks: {', '.join(proxy.get('main_risks', []))}\n\n"
        f"Current quarter tracked: {quarterly_meta.get('current_quarter', 'N/D')}\n"
        f"Quarter signal bias: {quarterly_model.get('quarter_signal_bias', 'N/D')}\n"
        f"Quarter confidence: {quarterly_model.get('confidence_level', 'N/D')}\n"
        f"Quarter signal score: {quarterly_model.get('quarter_signal_score', 'N/D')}\n"
        f"Revenue beat probability: {quarterly_model.get('revenue_beat_probability', quarterly_model.get('revenue_beat_guidance_probability', quarterly_model.get('revenue_beat_probability_proxy', 'N/D')))}\n"
        f"Revenue guidance reference: {quarterly_model.get('revenue_guidance_reference_musd', 'N/D')}\n"
        f"Revenue guidance reference quarter: {quarterly_model.get('revenue_guidance_reference_quarter', 'N/D')}\n"
        f"Estimated revenue: {_format_money_musd(quarterly_model.get('estimated_revenue_musd'))}\n"
        f"EBITDA beat probability: {quarterly_model.get('ebitda_beat_probability', quarterly_model.get('ebitda_beat_probability_proxy', 'N/D'))}\n"
        f"Estimated EBITDA: {_format_money_musd(quarterly_model.get('estimated_ebitda_musd'))}\n"
        f"Guidance raise probability: {quarterly_model.get('guidance_raise_probability', quarterly_model.get('guidance_raise_probability_proxy', 'N/D'))}\n"
        f"Estimated next-quarter guidance: {_format_money_musd(quarterly_model.get('estimated_next_q_guidance_musd'))}\n"
        f"Quarter observed days: {quarterly_current.get('observed_days', 'N/D')}\n"
        f"Quarter avg coverage ratio: {_format_ratio_pct(quarterly_current.get('avg_coverage_ratio'))}\n"
        f"Quarter premium momentum 14d: {_format_ratio_pct(quarterly_current.get('avg_premium_momentum_14d'))}\n"
        f"Quarter churn trend 14d: {_format_ratio_pct(quarterly_current.get('avg_churn_trend_14d'))}\n"
        f"Quarter reactivation trend 7d: {_format_ratio_pct(quarterly_current.get('avg_reactivation_trend_7d'))}\n"
        f"Quarter main drivers: {', '.join(quarterly_model.get('main_drivers', []))}\n"
        f"Quarter main risks: {', '.join(quarterly_model.get('main_risks', []))}\n"
        f"Labels readiness: actuals={quarterly_readiness.get('actual_labels_ready', 'N/D')}, "
        f"guidance_benchmarks={quarterly_readiness.get('guidance_benchmarks_ready', 'N/D')}, "
        f"supervised_ready={quarterly_readiness.get('supervised_ready', 'N/D')}\n\n"
        "Genere une lecture reflechie, orientee investisseur, mais tres compacte. "
        "Chaque bloc doit tenir dans une case Excel : maximum 3 a 4 lignes par section, "
        "et 2 puces courtes maximum pour les sections a puces. "
        "Privilegie l'analyse des dynamiques plutot que la repetition brute des chiffres."
    )

    return system_prompt, user_prompt


def _build_legacy_prompt(stats: dict) -> tuple[str, str]:
    date = stats.get("date_jour", datetime.now().strftime("%Y-%m-%d"))
    moyenne_streak = _safe_number(stats.get("moyenne_streak_jour"), 0)
    panel_total = int(_safe_number(stats.get("nb_profils_jour"), 0))
    retention = _safe_number(stats.get("taux_retention"), 0)
    churn = _safe_number(stats.get("taux_churn"), 0)
    engagement = _safe_number(stats.get("score_sante_jour"), 0)
    super_rate = _safe_number(stats.get("taux_conversion_plus"), 0)
    delta_xp = _safe_number(stats.get("delta_xp_moyen"), 0)
    cohortes = stats.get("cohortes", {})

    system_prompt = (
        "Tu es un analyste financier buy-side. "
        "Tu dois interpreter des statistiques d'engagement Duolingo et produire uniquement "
        "les sections [RESUME] [TENDANCES] [ATTENTION] [CONSEILS]."
    )

    user_prompt = (
        f"Date: {date}\n"
        f"Panel: {panel_total}\n"
        f"Engagement score: {engagement:.1f}%\n"
        f"Retention: {retention:.1f}%\n"
        f"Churn: {churn:.1f}%\n"
        f"Super rate: {super_rate:.1f}%\n"
        f"Delta XP mean: {delta_xp:.0f}\n"
        f"Average streak: {moyenne_streak:.1f}\n"
        f"Debutants churn: {_safe_number(cohortes.get('Debutants', {}).get('churn'), 0):.1f}%\n"
        f"Standard churn: {_safe_number(cohortes.get('Standard', {}).get('churn'), 0):.1f}%\n"
        f"Super-Actifs churn: {_safe_number(cohortes.get('Super-Actifs', {}).get('churn'), 0):.1f}%\n"
    )

    return system_prompt, user_prompt


def generer_rapport_ia(
    stats: dict,
    signal_package: dict | None = None,
    quarterly_nowcast: dict | None = None,
) -> str:
    print("============================================================")
    print("  PARTIE 3 - AGENT IA ANALYSTE")
    print("============================================================")

    if signal_package:
        system_prompt, user_prompt = _build_financial_signal_prompt(signal_package, quarterly_nowcast)
    else:
        system_prompt, user_prompt = _build_legacy_prompt(stats)

    print(f"\n  [PROMPT] System Prompt:\n     {system_prompt[:100]}...")
    print(f"\n  [PROMPT] User Prompt:\n     {user_prompt[:120]}...\n")

    rapport = None
    if not GOOGLE_API_KEY:
        print("  [WARNING] GOOGLE_API_KEY manquante dans l'environnement (.env).")
    else:
        rapport = _executer_agent_adk(system_prompt, user_prompt)

    if not rapport:
        if signal_package:
            proxy = signal_package.get("financial_proxy_signals", {})
            quarterly_model = (quarterly_nowcast or {}).get("model_output", {})
            quarterly_label = quarterly_model.get("quarter_signal_bias", "N/D")
            quarterly_revenue = quarterly_model.get(
                "revenue_beat_probability",
                quarterly_model.get(
                    "revenue_beat_guidance_probability",
                    quarterly_model.get("revenue_beat_probability_proxy", "N/D"),
                ),
            )
            quarterly_ebitda = quarterly_model.get(
                "ebitda_beat_probability",
                quarterly_model.get("ebitda_beat_probability_proxy", "N/D"),
            )
            quarterly_guidance = quarterly_model.get(
                "guidance_raise_probability",
                quarterly_model.get("guidance_raise_probability_proxy", "N/D"),
            )
            quarter_drivers = quarterly_model.get("main_drivers") or proxy.get("main_drivers") or []
            quarter_risks = quarterly_model.get("main_risks") or proxy.get("main_risks") or []
            quarter_drivers_text = "\n".join(f"- {item}" for item in quarter_drivers[:2]) or "- Les réactivations soutiennent encore la lecture."
            quarter_risks_text = "\n".join(f"- {item}" for item in quarter_risks[:2]) or "- La calibration du modèle reste encore limitée."
            rapport = (
                "[RESUME]\n"
                "Le signal quotidien reste exploitable, mais le modèle trimestriel demeure une lecture implicite et prudente.\n\n"
                "[TENDANCES]\n"
                f"- Le biais du jour reste {proxy.get('signal_bias', 'neutral')}.\n"
                f"- Le nowcast trimestriel ressort {str(quarterly_label).lower()}.\n\n"
                "[ATTENTION]\n"
                "- Les probabilités restent implicites tant que l'historique guidance n'est pas complet.\n\n"
                "[CONSEILS]\n"
                "Utilisez ce rapport comme une lecture de direction et de préparation aux résultats, pas comme un verdict absolu.\n\n"
                "[MODELE]\n"
                "Le modèle trimestriel agrège la monétisation, l'engagement, la rétention, le churn, les réactivations et la couverture du panel. "
                f"Il suggère aujourd'hui une probabilité implicite de {quarterly_revenue} pour les revenus, de {quarterly_ebitda} pour l'EBITDA et de {quarterly_guidance} pour un relèvement de guidance.\n\n"
                "[MODELE_TENDANCES]\n"
                f"{quarter_drivers_text}\n\n"
                "[MODELE_RISQUES]\n"
                f"{quarter_risks_text}"
            )
        else:
            date = stats.get("date_jour", datetime.now().strftime("%Y-%m-%d"))
            rapport = (
                "[RESUME]\n"
                f"Lecture quotidienne pour le {date}.\n\n"
                "[TENDANCES]\n"
                "Le pipeline legacy reste fonctionnel.\n\n"
                "[ATTENTION]\n"
                "Le systeme de signaux financiers n'est pas encore branche sur cette execution.\n\n"
                "[CONSEILS]\n"
                "Passer au paquet de signaux structures pour une lecture plus investisseur."
            )

    print("\n  [OK] Analyse IA generee avec succes.")
    return rapport
