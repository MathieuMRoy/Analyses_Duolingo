# GitHub Actions - Rapport Duolingo Quotidien

Cette configuration permet de lancer l'analyse chaque jour a 11h, heure de Toronto, meme si ton ordinateur est eteint.

## Ce que fait le workflow

1. Recupere `daily_streaks_log.csv` et `rapport_historique.xlsx` depuis Google Drive.
2. Lance `python main.py`.
3. Renvoie les fichiers mis a jour dans le meme dossier Google Drive.
4. Sauvegarde aussi les artefacts du run dans GitHub Actions.

Le workflow tourne toutes les heures, mais il ne lance vraiment l'analyse que lorsqu'il est 11:00 a Toronto. Cette approche gere automatiquement les changements d'heure.

## Etapes de mise en place

1. Cree un depot GitHub prive pour ce projet, puis pousse tout le contenu de `S:\Analyses_Duolingo`.
2. Cree un projet Google Cloud.
3. Active l'API Google Drive.
4. Cree un compte de service.
5. Genere une cle JSON pour ce compte de service.
6. Partage ton dossier Google Drive `Rapport Duolingo` avec l'adresse e-mail du compte de service, en editeur.
7. Dans GitHub, ajoute ces secrets dans `Settings > Secrets and variables > Actions` :
   - `DUOLINGO_JWT`
   - `GOOGLE_API_KEY`
   - `GEMINI_MODEL` (optionnel)
   - `GDRIVE_SERVICE_ACCOUNT_JSON`
   - `GDRIVE_FOLDER_ID`

## Valeurs des secrets

- `GDRIVE_SERVICE_ACCOUNT_JSON` :
  colle le contenu complet du fichier JSON du compte de service.
- `GDRIVE_FOLDER_ID` :
  prends l'identifiant du dossier dans l'URL Google Drive, c'est la partie apres `/folders/`.

## Premier test

1. Va dans l'onglet `Actions` de GitHub.
2. Ouvre le workflow `Daily Duolingo Report`.
3. Lance `Run workflow` une premiere fois.
4. Verifie que `rapport_historique.xlsx` et `daily_streaks_log.csv` apparaissent ou se mettent a jour dans Google Drive.

## Points importants

- Le depot doit etre prive.
- `target_users.csv` doit etre present dans le depot.
- Si `GOOGLE_API_KEY` est absent, le workflow peut quand meme tourner, mais le rapport IA retombera sur le mode local/placeholder.
