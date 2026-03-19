"""Construction de la feuille dictionnaire des KPIs."""

import pandas as pd


def build_kpi_dictionary_df() -> pd.DataFrame:
    base_rows = [
        {
            "KPI": "Moyenne Streak (Jours)",
            "Definition": "Longueur moyenne de la serie de jours consecutifs d'utilisation. Mesure la fidelite du panel.",
            "Methode / calcul": "Moyenne simple du streak observe sur le panel du jour.",
        },
        {
            "KPI": "Apprentissage (XP/j)",
            "Definition": "Gain moyen de points d'experience depuis la veille. Mesure l'effort d'apprentissage quotidien.",
            "Methode / calcul": "Delta de TotalXP entre hier et aujourd'hui, borne a 0 minimum, puis moyenne sur le panel.",
        },
        {
            "KPI": "Taux Abonn. Super",
            "Definition": "Part observable du panel premium via le signal hasPlus. Tant que MAX n'est pas detecte de facon fiable, une partie de MAX peut rester incluse ici.",
            "Methode / calcul": "Profils premium observables / profils observes dans le panel.",
        },
        {
            "KPI": "Taux d'Abandon Global",
            "Definition": "Part des utilisateurs actifs hier qui ne le sont plus aujourd'hui.",
            "Methode / calcul": "Utilisateurs avec streak > 0 hier et streak = 0 aujourd'hui / utilisateurs actifs hier.",
        },
        {
            "KPI": "Reactivations vs Veille",
            "Definition": "Nombre d'utilisateurs inactifs hier redevenus actifs aujourd'hui.",
            "Methode / calcul": "Comptage des profils avec streak = 0 hier puis streak > 0 aujourd'hui.",
        },
        {
            "KPI": "Progression Debutants vers Standard",
            "Definition": "Part des Debutants actifs hier qui ont progresse vers Standard aujourd'hui sans abandonner.",
            "Methode / calcul": "Debutants actifs hier devenus Standard aujourd'hui / Debutants actifs hier.",
        },
        {
            "KPI": "Abandon Debutants",
            "Definition": "Vrai abandon chez les Debutants, mesure sur la cohorte d'hier et non sur la cohorte du jour.",
            "Methode / calcul": "Debutants actifs hier devenus inactifs aujourd'hui / Debutants actifs hier.",
        },
        {
            "KPI": "Abandon Standard",
            "Definition": "Vrai abandon chez les Standard, mesure sur la cohorte d'hier.",
            "Methode / calcul": "Standard actifs hier devenus inactifs aujourd'hui / Standard actifs hier.",
        },
        {
            "KPI": "Abandon Super-Actifs",
            "Definition": "Vrai abandon chez les Super-Actifs, mesure sur la cohorte d'hier.",
            "Methode / calcul": "Super-Actifs actifs hier devenus inactifs aujourd'hui / Super-Actifs actifs hier.",
        },
        {
            "KPI": "Score d'Engagement",
            "Definition": "Part des profils observes qui restent actifs aujourd'hui.",
            "Methode / calcul": "Profils avec streak > 0 / panel observe du jour.",
        },
    ]

    quarterly_rows = [
        {
            "KPI": "Confiance (trimestriel)",
            "Definition": "Niveau de fiabilite du nowcast trimestriel.",
            "Methode / calcul": "Elevee si couverture moyenne >= 75% et jours observes >= 20 ; Moyenne si couverture >= 45% et jours observes >= 10 ; sinon Faible.",
        },
        {
            "KPI": "Score trimestre",
            "Definition": "Score synthetique 0-100 du trimestre, utilise pour lire le biais global du nowcast.",
            "Methode / calcul": "Score compose a partir de monetisation, engagement, momentum Super, churn, reactivations et retention high-value, puis ajuste par un breadth factor qui penalise les trimestres encore jeunes.",
        },
        {
            "KPI": "Prob. beat revenus",
            "Definition": "Probabilite implicite de battre les revenus du trimestre. En facade, c'est la lecture principale du nowcast revenus.",
            "Methode / calcul": "On part d'un score revenus, converti en probabilite implicite. Cette probabilite reste guidance-first : elle se lit face au benchmark management du trimestre tant qu'un consensus complet n'est pas disponible.",
        },
        {
            "KPI": "Prob. beat EBITDA",
            "Definition": "Probabilite implicite de battre l'EBITDA du trimestre.",
            "Methode / calcul": "Transformation score -> probabilite appliquee a un score EBITDA fonde sur monetisation, engagement, churn, reactivations et retention high-value.",
        },
        {
            "KPI": "Prob. guidance raise",
            "Definition": "Probabilite implicite d'un relevement de guidance pour le trimestre suivant.",
            "Methode / calcul": "Transformation score -> probabilite appliquee a un score guidance surtout pilote par monetisation, momentum Super, engagement, churn et reactivations.",
        },
        {
            "KPI": "Estimation revenus trimestrielle",
            "Definition": "Estimation interne des revenus du trimestre, affichee en grand dans le nowcast.",
            "Methode / calcul": "Si une guidance revenus de reference existe, estimation = guidance x (1 + beat implicite). Le beat implicite part du beat historique median et est ajuste par la probabilite revenus, avec bornes de prudence. Sinon, fallback sur le revenu reel du trimestre precedent avec une croissance QoQ implicite bornee.",
        },
        {
            "KPI": "Est. X M$ vs guidance Y M$",
            "Definition": "Lecture directe de l'estimation revenus par rapport au benchmark management du trimestre.",
            "Methode / calcul": "Le chiffre de gauche est l'estimation du modele. Le chiffre de droite est la guidance revenus du management pour le trimestre suivi, lorsqu'elle est disponible dans l'historique trimestriel.",
        },
        {
            "KPI": "EBITDA estime",
            "Definition": "Estimation interne de l'EBITDA ajuste du trimestre.",
            "Methode / calcul": "EBITDA estime = revenus estimes x marge EBITDA implicite. La marge implicite part de la marge historique mediane et est ajustee par la probabilite EBITDA, avec bornes conservatrices.",
        },
        {
            "KPI": "Guide N+1 estime",
            "Definition": "Estimation interne de la guidance revenus du trimestre suivant.",
            "Methode / calcul": "Guide N+1 estime = revenus estimes x ratio implicite. Le ratio part du ratio historique median guidance suivante / revenus et est ajuste par la probabilite guidance raise.",
        },
        {
            "KPI": "Historique trimestriel fige",
            "Definition": "Memoire du nowcast tel qu'il etait a la date du snapshot, sans etre ecrase une fois le trimestre termine.",
            "Methode / calcul": "Chaque trimestre garde son snapshot final. Quand un nouveau trimestre commence, le precedent reste fige pour permettre le backtest et la comparaison estimate vs reel.",
        },
        {
            "KPI": "Cadre du modele",
            "Definition": "Bloc qui explique la reference utilisee et le niveau de maturite du modele trimestriel.",
            "Methode / calcul": "Resume du statut du snapshot, de la reference guidance revenus, du niveau de supervision disponible et de la prochaine etape de calibration.",
        },
    ]

    return pd.concat(
        [pd.DataFrame(base_rows), pd.DataFrame(quarterly_rows)],
        ignore_index=True,
    )
