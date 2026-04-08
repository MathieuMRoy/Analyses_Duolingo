# Architecture du projet

Ce repo produit un classeur Duolingo oriente investisseur a partir de donnees quotidiennes, de signaux trimestriels et de quelques sources externes.

## Pipeline quotidien

1. `main.py`
   - orchestre le scraping, les signaux financiers, le nowcast trimestriel, l'alternative data et la DCF
2. `duolingo_analyzer/report_builder.py`
   - reconstruit le workbook principal `rapport_historique.xlsx`
3. `scripts_utilitaires/build_historique.py`
   - reconstruit la version historique consolidee
4. `scripts_utilitaires/google_drive_sync.py`
   - recupere et pousse les fichiers persistes dans Google Drive

## Ou vit la logique metier

- `duolingo_analyzer/financial_signals.py`
  - signaux quotidiens du panel
- `duolingo_analyzer/quarterly_nowcast.py`
  - agregation trimestrielle, probabilites, estimations
- `duolingo_analyzer/alternative_data.py`
  - signaux externes quotidiens et historique WoW
- `duolingo_analyzer/valuation_dcf.py`
  - hypotheses et calculs de valorisation DCF

## Ou vit le rendu Excel

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

## Helpers partages

- `duolingo_analyzer/columns.py`
  - noms des feuilles, aliases de colonnes, constantes partagees
- `duolingo_analyzer/reporting/styles.py`
  - palette, polices, helpers de style
- `duolingo_analyzer/reporting/render_helpers.py`
  - helpers textuels passes aux renderers
- `duolingo_analyzer/reporting/workbook_layout.py`
  - ordre des onglets, feuilles masquees, retrait des onglets legacy
- `duolingo_analyzer/reporting/workbook_postprocess.py`
  - operations workbook generiques

## Fichiers de donnees persistes

- `daily_streaks_log.csv`
  - historique quotidien principal du panel
- `target_users.csv`
  - panel cible a suivre
- `alternative_data_inputs.csv`
  - entrees manuelles complementaires pour l'onglet `Alternative Data`
- `rapports_donnees/alternative_data_history.csv`
  - historique quotidien des signaux externes
- `financial_docs/quarterly_labels_template.csv`
  - labels trimestriels, guidance, reels

## Regle simple pour modifier vite

- besoin de changer un calcul : toucher le module metier correspondant
- besoin de changer l'apparence d'un onglet : toucher `reporting/sheets/...`
- besoin de renommer des feuilles ou harmoniser la structure : toucher `columns.py` et `reporting/workbook_layout.py`
- besoin de modifier le wording partage : privilegier `reporting/render_helpers.py` ou le renderer concerne
