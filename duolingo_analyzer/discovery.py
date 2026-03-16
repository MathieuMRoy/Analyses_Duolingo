"""
Partie 0 : Découverte
Module en charge de générer des candidats de noms d'utilisateurs via 
la méthode de la "Boule de Neige" (Snowball Sampling) authentifiée avec JWT 
pour récupérer les profils actifs.
"""
import csv
import random
import time
import requests
import json
import base64
from datetime import datetime, timedelta
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .config import (
    DUOLINGO_PROFILE_API,
    SESSION,
    TARGET_USERS_FILE,
    OBJECTIF_PAR_COHORTE,
    OBJECTIF_UTILISATEURS,
    DUOLINGO_JWT,
    REPORT_DIR,
    BASE_DIR,
)

NODES_EXPLORES_FILE = BASE_DIR / "explored_nodes.json"

# Noms d'utilisateurs réels ayant de vastes réseaux d'abonnés
SEED_USERNAMES = [
    "luis", "Jason Adams", "Matthew", "Ricardo Méndez Fragoso", "Nasseef", 
    "Cristene", "sylvia", "Sabrina", "Louis", "Kristiyan", "Jakub M.", 
    "Gillian", "Natalie Glance", "John", "Maria", "David", "Anna", 
    "Carlos", "Pedro", "Sofia", "Emma", "duolingo", "Duo", "zari", "lily"
]

# ─── Constantes de performance ──────────────────────────────────────────────
SAVE_INTERVAL = 10        # Sauvegarde plus fréquente pour voir le progrès
PARALLEL_WORKERS = 8      # Turbo Mode for Brute Force
ACTIVITY_DELAY = (0.5, 1.2)  # Délai réduit (vrai mode Turbo)


def _extraire_viewer_id(jwt_token: str) -> str:
    """
    Extrait dynamiquement le viewer ID (sub) depuis le JWT Duolingo
    au lieu de le hardcoder.
    """
    try:
        # Le JWT a 3 parties séparées par des points, le payload est la 2e
        payload_b64 = jwt_token.split(".")[1]
        # Ajouter le padding manquant pour base64
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return str(payload.get("sub", ""))
    except Exception:
        return ""


def _verifier_utilisateur(username: str) -> dict | None:
    """
    Vérifie si un nom d'utilisateur existe sur Duolingo via l'API publique.
    Retourne les données du profil si l'utilisateur existe, None sinon.
    """
    username_clean = username.replace(" ", "")
    if not username_clean:
        return None

    import time
    import random
    
    time.sleep(random.uniform(0.1, 0.3)) # Délai minimum pour la vitesse
    try:
        response = SESSION.get(
            DUOLINGO_PROFILE_API,
            params={"username": username_clean},
            timeout=15,
        )
        if response.status_code == 200:
            data = response.json()
            users = data.get("users", [])
            if users and len(users) > 0:
                return users[0]
        elif response.status_code == 403:
            print(f"  🛑 Accès limité (403). Pause de sécurité (30-60s)...")
            time.sleep(random.uniform(30, 60))
        return None
    except requests.exceptions.RequestException:
        return None
    except (json.JSONDecodeError, ValueError):
        return None


def _trouver_plus_de_candidats(user_id: str | int, viewer_id: str, quantite_voulue: int = 200) -> list[dict]:
    """
    Appelle l'API de réseaux sociaux de Duolingo.
    Retourne une liste de dictionnaires contenant {username, totalXp, id}.
    """
    if not DUOLINGO_JWT:
        print("\n  ⚠️ Jeton JWT manquant. Le crawler ne marchera pas.")
        return []

    candidats = []
    
    url = f"https://www.duolingo.com/2017-06-30/friends/users/{user_id}/followers"
    params = {
        "pageSize": quantite_voulue,
        "viewerId": viewer_id
    }
    
    try:
        response = SESSION.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            users_list = data.get("followers", {}).get("users", [])
            
            for u in users_list:
                username = u.get("username")
                total_xp = u.get("totalXp", 0)
                uid = u.get("userId")
                if username:
                    candidats.append({
                        "username": username,
                        "totalXp": total_xp,
                        "id": uid
                    })
        elif response.status_code in (401, 403):
            print(f"  ❌ Accès refusé sur {url}. Jeton JWT expiré ?")
    except Exception as e:
        print(f"  [ERREUR CRAWLER] {e}")
            
    return candidats


def _verifier_activite_profil(user_id, date_limite: str) -> str | None:
    """
    Vérifie si un utilisateur est actif (a joué dans les 7 derniers jours).
    Retourne le username si actif, None sinon. Thread-safe.
    """
    url = f"https://www.duolingo.com/2017-06-30/users/{user_id}"
    try:
        # Note: on réduit le délai pour ce test à 1 worker
        time.sleep(random.uniform(0.1, 0.3))
        
        res = SESSION.get(url, timeout=10)
        if res.status_code == 200:
            u_data = res.json()
            streak_data = u_data.get("streakData") or {}
            current_streak = streak_data.get("currentStreak") or {}
            last_date = current_streak.get("lastExtendedDate")
            streak_length = streak_data.get("length", 0)
            
            # STRICT FILTER: Streak active (Date récente + Longueur > 0)
            limite_active = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            
            if last_date and last_date >= limite_active and streak_length > 0:
                return u_data.get("username")
        elif res.status_code in (403, 429):
            print(f"  🛑 [{user_id}] Rate limit détecté (Status {res.status_code}). Pause 60s...")
            time.sleep(60)
    except Exception:
        pass
    return None

def _determiner_cohorte(total_xp: int) -> str:
    """Détermine la cohorte d'un utilisateur selon son XP Total."""
    if total_xp < 1000:
        return "Debutants"
    elif total_xp <= 5000:
        return "Standard"
    else:
        return "Super-Actifs"

def _sauvegarder_progressif(utilisateurs_session: list[dict]):
    """
    Sauvegarde atomique : fusionne les découvertes de la session avec le disque.
    Respecte les suppressions manuelles effectuées par l'utilisateur.
    """
    if not utilisateurs_session:
        return
    
    # 1. Charger l'état actuel du disque
    utilisateurs_disk = []
    if TARGET_USERS_FILE.exists():
        try:
            # On lit en mode sécurisé
            df_disk = pd.read_csv(TARGET_USERS_FILE)
            utilisateurs_disk = df_disk.to_dict('records')
        except Exception:
            pass

    # 2. Fusionner : On garde tout ce qui est sur le disque + nos nouvelles trouvailles
    # Username est la clé unique (insensible à la casse)
    vu = set()
    fusion = []
    
    # On commence par le disque (source de vérité pour ce qui doit rester)
    for u in utilisateurs_disk:
        name = str(u.get("Username", "")).strip()
        name_lower = name.lower()
        if name_lower and name_lower not in vu:
            fusion.append({"Username": name, "Cohort": u.get("Cohort", "Standard")})
            vu.add(name_lower)
            
    # On ajoute les trouvailles de cette session (si pas déjà là)
    for u in utilisateurs_session:
        name = str(u.get("Username", "")).strip()
        name_lower = name.lower()
        if name_lower and name_lower not in vu:
            fusion.append({"Username": name, "Cohort": u.get("Cohort", "Standard")})
            vu.add(name_lower)
    # 3. Écriture robuste
    try:
        df = pd.DataFrame(fusion)
        df.to_csv(TARGET_USERS_FILE, index=False)
        # Flush OS
        if hasattr(os, 'sync'):
            os.sync()
        print(f"  ✅ Fichier mis à jour : {len(fusion)} utilisateurs au total ({len(utilisateurs_session)} nouveaux).")
    except Exception as e:
        print(f"  ⚠️ Erreur lors de la sauvegarde : {e}")


def initialiser_cibles() -> list[str]:
    """
    PARTIE 0 : Découvre de vrais utilisateurs Duolingo très actifs
    en utilisant la méthode d'exploration de graphe (Snowball).
    """
    print("=" * 60)
    print("  PARTIE 0 — INITIALISATION (MÉTHODE SNOWBALL) - Logic v2.0")
    print(f"  Objectif : {OBJECTIF_PAR_COHORTE} par cohorte (Total = {OBJECTIF_UTILISATEURS})")
    print(f"  Mode     : Authentifié via JWT, Tri par Cohortes")
    print("=" * 60)

    if not DUOLINGO_JWT:
        print("\n  🚫 ARRÊT CRITIQUE : Jeton DUOLINGO_JWT introuvable dans .env.")
        print("  Le script a besoin de votre jeton pour lire le graphe des abonnés.")
        return []

    # Extraction dynamique du viewer ID depuis le JWT
    viewer_id = _extraire_viewer_id(DUOLINGO_JWT)
    if viewer_id:
        print(f"\n  🔑 Viewer ID extrait du JWT : {viewer_id[:6]}...")
    else:
        print("\n  ⚠️ Impossible d'extraire le viewer ID du JWT. Utilisation du fallback.")
        viewer_id = "264185143453632"

    utilisateurs_session: list[dict] = [] # Uniquement les nouveaux de cette run
    id_queue: list[int] = []
    
    # On charge les usernames existants pour éviter les doublons de comptage
    seen_usernames = set()
    if TARGET_USERS_FILE.exists():
        try:
            df_init = pd.read_csv(TARGET_USERS_FILE)
            seen_usernames.update(df_init["Username"].tolist())
        except: pass

    total_uniques = len(seen_usernames)
    print(f"\n  📂 État initial : {total_uniques} utilisateurs uniques.")

    print(f"\n  🌱 Phase 1 — Graines (Leaderboards)")
    for seed in SEED_USERNAMES:
        if total_uniques >= OBJECTIF_UTILISATEURS: break
        
        # FIX: On ajoute les graines à la queue d'exploration même si on les a déjà "vues"
        # pour s'assurer que la Phase 2 a de quoi démarrer.
        profil = _verifier_utilisateur(seed)
        if profil:
            uid = profil.get("id")
            if uid and uid not in id_queue:
                id_queue.append(uid)
            
            # Mais on ne les rajoute pas au CSV si déjà là
            if seed not in seen_usernames:
                total_xp = profil.get("totalXp", 0) or 0
                cohorte = _determiner_cohorte(total_xp)
                utilisateurs_session.append({"Username": profil.get("username", seed), "Cohort": cohorte})
                seen_usernames.add(seed)
                total_uniques += 1
    
    _sauvegarder_progressif(utilisateurs_session)
    utilisateurs_session = [] 

    # Phase 2
    vague = 1
    queue_index = 0
    Derive_IDs = list(id_queue) # Copie pour éviter de boucler sur ce qu'on ajoute
    date_limite = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    print(f"\n  🚀 Phase 2 — Nuclear Brute Force (Vitesse Maximale)")
    
    while total_uniques < OBJECTIF_UTILISATEURS and queue_index < len(id_queue):
        courant_id = id_queue[queue_index]
        queue_index += 1

        candidats = _trouver_plus_de_candidats(courant_id, viewer_id, 300)
        if not candidats: continue
            
        print(f"\n  🔍 ID:{courant_id} -> {len(candidats)} à vérifier...")
        
        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = {executor.submit(_verifier_activite_profil, c["id"], date_limite): c for c in candidats}
            for future in as_completed(futures):
                if total_uniques >= OBJECTIF_UTILISATEURS: break
                
                username = future.result()
                if username and username not in seen_usernames:
                    item = futures[future]
                    cohorte = _determiner_cohorte(item["totalXp"])
                    utilisateurs_session.append({"Username": username, "Cohort": cohorte})
                    seen_usernames.add(username)
                    total_uniques += 1
                    
                    if item["id"] not in id_queue:
                        id_queue.append(item["id"])

        if len(utilisateurs_session) >= SAVE_INTERVAL:
            _sauvegarder_progressif(utilisateurs_session)
            utilisateurs_session = [] 
            print(f"  💾 Progrès : {total_uniques} / {OBJECTIF_UTILISATEURS} uniques trouvés.")
            
        vague += 1

    _sauvegarder_progressif(utilisateurs_session)
    return []
