# Architecture du projet

Ce repo produit un classeur Duolingo orienté investisseur à partir de données quotidiennes, de signaux trimestriels et de quelques sources externes.

## Pipeline quotidien

1. `main.py`
   - orchestre le scraping, les signaux financiers, le nowcast trimestriel, l'alternative data et la DCF
2. `duolingo_analyzer/report_builder.py`
   - reconstruit le workbook principal `rapport_historique.xlsx`
3. `scripts_utilitaires/build_historique.py`
   - reconstruit la version historique consolidée
4. `scripts_utilitaires/google_drive_sync.py`
   - récupère/pousse les fichiers persistés dans Google Drive

## Où vit la logique métier

- `duolingo_analyzer/financial_signals.py`
  - signaux quotidiens du panel
- `duolingo_analyzer/quarterly_nowcast.py`
  - agrégation trimestrielle, probabilités, estimations
- `duolingo_analyzer/alternative_data.py`
  - signaux externes quotidiens et historique WoW
- `duolingo_analyzer/valuation_dcf.py`
  - hypothèses et calculs de valorisation DCF

## Où vit le rendu Excel

- `duolingo_analyzer/reporting/sheets/summary_sheet.py`
  - onglet `Suivi Quotidien`
- `duolingo_analyzer/reporting/sheets/financial_nowcast_sheet.py`
  - onglet `Signaux Financiers`
- `duolingo_analyzer/reporting/sheets/quarterly_nowcast_sheet.py`
  - onglet `Nowcast Trimestriel`
- `duolingo_analyzer/reporting/sheets/alternative_data_sheet.py`
  - onglet `Alternative Data`
- `duolingo_analyzer/reporting/sheets/monthly_trends_sheet.py`
  - onglet `Tendances Mensuelles`
- `duolingo_analyzer/reporting/sheets/dcf_valuation_sheet.py`
  - onglet `Valorisation DCF`
- `duolingo_analyzer/reporting/sheets/kpi_dictionary_sheet.py`
  - onglet `Dictionnaire des KPIs`

## Helpers partagés

- `duolingo_analyzer/columns.py`
  - noms des feuilles, aliases de colonnes, constantes partagées
- `duolingo_analyzer/reporting/styles.py`
  - palette, polices, helpers de style
- `duolingo_analyzer/reporting/render_helpers.py`
  - helpers textuels passés aux renderers
- `duolingo_analyzer/reporting/workbook_layout.py`
  - ordre des onglets, feuilles masquées, retrait des onglets legacy
- `duolingo_analyzer/reporting/workbook_postprocess.py`
  - opérations workbook génériques

## Fichiers de données persistés

- `daily_streaks_log.csv`
  - historique quotidien principal du panel
- `target_users.csv`
  - panel cible à suivre
- `alternative_data_inputs.csv`
  - entrées manuelles complémentaires pour l'onglet `Alternative Data`
- `rapports_donnees/alternative_data_history.csv`
  - historique quotidien des signaux externes
- `financial_docs/quarterly_labels_template.csv`
  - labels trimestriels, guidance, réels

## Règle simple pour modifier vite

- besoin de changer un calcul : toucher le module métier correspondant
- besoin de changer l'apparence d'un onglet : toucher `reporting/sheets/...`
- besoin de renommer des feuilles ou harmoniser la structure : toucher `columns.py` et `reporting/workbook_layout.py`
- besoin de modifier le wording partagé : privilégier `reporting/render_helpers.py` ou le renderer concerné
