"""Construction de la feuille dictionnaire des KPIs."""

import pandas as pd


def build_kpi_dictionary_df() -> pd.DataFrame:
    base_df = pd.DataFrame(
        [
            {
                "KPI": "Moyenne Streak (J)",
                "Définition": "Longueur moyenne de la série de jours consécutifs d'utilisation. Mesure la fidélité à long terme.",
                "Méthode / calcul": "Moyenne simple du streak observé sur le panel du jour.",
            },
            {
                "KPI": "Apprentissage (XP/j)",
                "Définition": "Gain moyen de points d'expérience (XP) depuis hier. Mesure l'effort d'apprentissage quotidien.",
                "Méthode / calcul": "Pour chaque profil, delta de TotalXP vs veille avec plancher à 0, puis moyenne sur le panel.",
            },
            {
                "KPI": "Taux Abonn. Super",
                "Définition": "Part observable du panel premium via hasPlus. Tant que Max n'est pas détecté de façon fiable, ce taux peut encore inclure une partie des comptes Max.",
                "Méthode / calcul": "Nombre de profils premium observables / nombre total de profils observés dans le panel.",
            },
            {
                "KPI": "Taux d'Abandon Global",
                "Définition": "Pourcentage d'utilisateurs actifs hier qui ne le sont plus aujourd'hui (streak retombe à 0).",
                "Méthode / calcul": "Utilisateurs actifs hier devenus inactifs aujourd'hui / utilisateurs actifs hier.",
            },
            {
                "KPI": "Reactivations vs Veille",
                "Définition": "Nombre d'utilisateurs inactifs hier (streak à 0) redevenus actifs aujourd'hui.",
                "Méthode / calcul": "Comptage simple des profils passés de streak 0 à streak > 0 entre hier et aujourd'hui.",
            },
            {
                "KPI": "Progression Débutants vers Standard",
                "Définition": "Part des Débutants actifs hier qui sont encore actifs aujourd'hui et ont progressé vers la cohorte Standard.",
                "Méthode / calcul": "Nombre de Débutants actifs hier observés aujourd'hui en Standard / Débutants actifs hier.",
            },
            {
                "KPI": "Abandon Débutants",
                "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Débutants et actifs hier, puis sont tombés à une streak de 0 aujourd'hui.",
                "Méthode / calcul": "Débutants actifs hier devenus inactifs aujourd'hui / Débutants actifs hier.",
            },
            {
                "KPI": "Abandon Standard",
                "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Standard et actifs hier, puis sont tombés à une streak de 0 aujourd'hui.",
                "Méthode / calcul": "Standard actifs hier devenus inactifs aujourd'hui / Standard actifs hier.",
            },
            {
                "KPI": "Abandon Super-Actifs",
                "Définition": "Taux d'abandon parmi les utilisateurs qui étaient Super-Actifs et actifs hier, puis sont tombés à une streak de 0 aujourd'hui.",
                "Méthode / calcul": "Super-Actifs actifs hier devenus inactifs aujourd'hui / Super-Actifs actifs hier.",
            },
            {
                "KPI": "Score Santé Global",
                "Définition": "Pourcentage des utilisateurs suivis qui ont un streak > 0 aujourd'hui.",
                "Méthode / calcul": "Utilisateurs avec streak > 0 / panel observé du jour.",
            },
        ]
    )

    quarterly_df = pd.DataFrame(
        [
            {
                "KPI": "Confiance (trimestriel)",
                "Définition": "Niveau de fiabilité du nowcast trimestriel.",
                "Méthode / calcul": "Élevée si couverture moyenne >= 75% et jours observés >= 20 ; Moyenne si couverture >= 45% et jours >= 10 ; sinon Faible.",
            },
            {
                "KPI": "Score trimestre",
                "Définition": "Score synthétique 0-100 du trimestre, utilisé pour lire le biais global du nowcast.",
                "Méthode / calcul": "50 + 50 x (0,28 monétisation + 0,18 engagement + 0,16 momentum Super + 0,08 momentum Max + 0,15 churn + 0,08 réactivations + 0,07 rétention high-value), puis ajustement par breadth factor.",
            },
            {
                "KPI": "Breadth factor",
                "Définition": "Facteur qui pénalise un trimestre encore peu couvert ou trop jeune.",
                "Méthode / calcul": "min(couverture moyenne / 70%, 1) x min(jours observés / 21, 1). Il réduit l'amplitude des scores si le trimestre est encore peu observé.",
            },
            {
                "KPI": "Prob. beat revenus",
                "Définition": "Probabilité implicite, non supervisée, de battre les revenus du trimestre.",
                "Méthode / calcul": "On calcule d'abord un score revenus, puis base_prob = 10% + 80% x score/100. Cette base est ensuite ramenée vers 50% selon la confiance : Élevée 1,0 ; Moyenne 0,7 ; Faible 0,45.",
            },
            {
                "KPI": "Prob. beat EBITDA",
                "Définition": "Probabilité implicite, non supervisée, de battre l'EBITDA trimestriel.",
                "Méthode / calcul": "Même transformation que pour les revenus, mais à partir d'un score EBITDA construit à partir de monétisation, engagement, churn, réactivations, rétention high-value et momentum Max.",
            },
            {
                "KPI": "Prob. guidance raise",
                "Définition": "Probabilité implicite, non supervisée, d'un relèvement de guidance sur le trimestre suivant.",
                "Méthode / calcul": "Même transformation score -> probabilité, appliquée à un score guidance reposant surtout sur monétisation, momentum Super, engagement, churn et réactivations.",
            },
            {
                "KPI": "Revenus estimés",
                "Définition": "Estimation interne des revenus trimestriels cohérente avec le signal du panel et les références historiques.",
                "Méthode / calcul": "Si une guidance revenus de référence existe, estimation = guidance x (1 + beat implicite). Le beat implicite part du beat historique médian et est ajusté par la prob. revenus, avec bornes [-3%, +8%]. Sinon fallback sur le CA réel du trimestre précédent avec une croissance QoQ implicite bornée [-5%, +15%].",
            },
            {
                "KPI": "EBITDA estimé",
                "Définition": "Estimation interne de l'EBITDA ajusté du trimestre.",
                "Méthode / calcul": "EBITDA estimé = revenus estimés x marge implicite. La marge implicite part de la marge EBITDA historique médiane et est ajustée par la prob. EBITDA, avec bornes [18%, 40%].",
            },
            {
                "KPI": "Guide N+1 estimé",
                "Définition": "Estimation interne de la guidance revenus du trimestre suivant.",
                "Méthode / calcul": "Guide N+1 estimé = revenus estimés x ratio implicite. Le ratio part du ratio historique médian guidance N+1 / revenus et est ajusté par la prob. guidance raise, avec bornes [0,98x, 1,12x].",
            },
        ]
    )

    return pd.concat([base_df, quarterly_df], ignore_index=True)
