# Couche ADK Duolingo

Cette couche ADK ajoute un **compagnon analyste** au-dessus du pipeline existant.

Le principe :
- les calculs métier restent déterministes dans `financial_signals.py`, `quarterly_nowcast.py`, `alternative_data.py` et `valuation_dcf.py`
- l'ADK sert à **orchestrer**, **questionner** et **contrôler** ces sorties

## Agents disponibles

- `duolingo_investor_supervisor`
  - superviseur principal
  - peut répondre à des questions larges sur l'état du rapport
- `daily_signals_agent`
  - lecture du panel quotidien et des tendances `7 jours / 30 jours`
- `quarterly_nowcast_agent`
  - lecture du nowcast trimestriel, confiance et probabilités implicites
- `alternative_data_agent`
  - lecture des signaux externes et du momentum WoW
- `valuation_agent`
  - lecture DCF, prix cible, cours actuel, upside / downside
- `report_quality_agent`
  - contrôles qualité du workbook et de la persistance

## Workflows ADK

- `parallel_context_agent`
  - lance en parallèle les spécialistes quotidien, trimestriel, alternative data et valorisation
- `report_review_workflow`
  - enchaîne lecture spécialisée -> QA -> synthèse

Ces workflows sont fournis comme base d'orchestration pour la suite. L'entrée la plus pratique aujourd'hui reste le superviseur interactif.

## Outils ADK

Les outils exposés par la couche ADK sont **read-only** :
- `get_full_report_context`
- `get_daily_signals_context`
- `get_quarterly_nowcast_context`
- `get_alternative_data_context`
- `get_dcf_context`
- `run_report_quality_checks`
- `get_workbook_overview`

Ils lisent prioritairement les fichiers déjà persistés (`latest.json`, workbook, CSV d'historique) pour éviter de relancer tout le pipeline à chaque question.

## Utilisation locale

Commande simple :

```powershell
$env:PYTHONPATH='S:\Analyses_Duolingo'
python S:\Analyses_Duolingo\scripts_utilitaires\run_adk_analyst.py "Donne-moi les 3 risques principaux du rapport du jour."
```

Exemples de questions utiles :
- `Pourquoi la confiance trimestrielle est faible ?`
- `Quels signaux s'améliorent sur 30 jours ?`
- `La DCF donne quel upside implicite ?`
- `Y a-t-il un problème de structure dans le workbook ?`

## Ce que cette couche ne fait pas

- elle ne remplace pas le nowcast
- elle ne remplace pas la DCF
- elle ne scrappe pas le panel utilisateur
- elle ne doit pas devenir la source de vérité des calculs

Le cœur du modèle doit rester explicite et déterministe.
