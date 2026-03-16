"""
Partie 1 : Scraper
Collecte quotidienne des streaks pour les utilisateurs cibles.
"""
import csv
import json
import random
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from .config import (
    DUOLINGO_PROFILE_API,
    SESSION,
    DAILY_LOG_FILE,
)

MAX_RETRIES = 2  # Nombre de tentatives supplémentaires en cas d'échec


def _recuperer_profil(username: str) -> dict | None:
    """
    Interroge l'API Duolingo pour récupérer le streak, l'XP total 
    et le statut d'abonnement (hasPlus) d'un utilisateur.
    Retry automatique sur erreur réseau.
    """
    for tentative in range(1 + MAX_RETRIES):
        try:
            response = SESSION.get(
                DUOLINGO_PROFILE_API,
                params={"username": username},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            users = data.get("users", [data])
            if users:
                user = users[0] if isinstance(users, list) else users
                streak = user.get("streak", user.get("site_streak", 0))
                total_xp = user.get("totalXp", 0)
                has_plus = user.get("hasPlus", False)
                # Max est souvent indiqué par un tier spécifique ou un champ additionnel
                # On tente de le détecter via hasMax ou subscriptionTier s'ils existent
                has_max = user.get("hasMax", False) or user.get("subscriptionTier") == "max"
                
                return {
                    "streak": int(streak) if streak is not None else 0,
                    "totalXp": int(total_xp) if total_xp is not None else 0,
                    "hasPlus": bool(has_plus),
                    "hasMax": bool(has_max)
                }

            return None

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "?"
            
            # GESTION SPÉCIFIQUE DU 429 (Trop de requêtes)
            if status_code == 429:
                wait = 30 + random.uniform(0, 30)
                print(f"  ⚠️ [RATE LIMIT] Duolingo demande de ralentir pour {username}. Pause de {wait:.0f}s...")
                time.sleep(wait)
                continue # On retente après la pause

            # Pas de retry sur autres 4xx
            if e.response is not None and 400 <= e.response.status_code < 500:
                print(f"  [ERREUR {status_code}] {username}")
                return None
            # Retry sur 5xx (erreur serveur)
            if tentative < MAX_RETRIES:
                wait = 2 ** (tentative + 1) + random.uniform(0, 1)
                print(f"  [RETRY {tentative+1}/{MAX_RETRIES}] {username} (erreur {status_code}, attente {wait:.0f}s)")
                time.sleep(wait)
                continue
            print(f"  [ERREUR {status_code}] {username} (abandon après {MAX_RETRIES} retries)")
            return None
        except requests.exceptions.RequestException as e:
            if tentative < MAX_RETRIES:
                wait = 2 ** (tentative + 1) + random.uniform(0, 1)
                print(f"  [RETRY {tentative+1}/{MAX_RETRIES}] {username} (réseau, attente {wait:.0f}s)")
                time.sleep(wait)
                continue
            print(f"  [ERREUR RÉSEAU] {username} : {e} (abandon)")
            return None
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"  [ERREUR PARSE] {username} : {e}")
            return None
    return None


def collecter_streaks_quotidiens(utilisateurs_cibles: list[dict]) -> str:
    """
    PARTIE 1 : Version Ultra-Speed (Parallel processing).
    Process 10,000 users in < 20 minutes (12 workers).
    """
    aujourdhui = datetime.now().strftime("%Y-%m-%d")
    print("\n" + "=" * 60)
    print(f"  PARTIE 1 — COLLECTE ULTRA-SPEED ({aujourdhui})")
    print(f"  Cible : {len(utilisateurs_cibles)} utilisateurs")
    print("=" * 60 + "\n")

    fichier_existe = DAILY_LOG_FILE.exists()
    
    # 1. Identifier déjà traités
    deja_traites = set()
    if fichier_existe:
        try:
            with open(DAILY_LOG_FILE, "r", encoding="utf-8") as rf:
                reader = csv.DictReader(rf)
                for row in reader:
                    if row.get("Date") == aujourdhui:
                        deja_traites.add(row.get("Username"))
        except: pass

    utilisateurs_a_faire = [u for u in utilisateurs_cibles if u["Username"] not in deja_traites]
    
    if not utilisateurs_a_faire:
        print("  ✅ Tous les utilisateurs sont déjà à jour pour aujourd'hui.")
        return aujourdhui

    print(f"  🚀 Lancement turbo : {len(utilisateurs_a_faire)} restants...")
    
    lock = threading.Lock()
    stats = {"succes": 0, "erreurs": 0}
    max_workers = 12

    def _worker_task(cible, index):
        user = cible["Username"]
        cohorte = cible["Cohort"]
        
        # 1. Pacing intelligent : On étale le démarrage des workers
        time.sleep(random.uniform(0, 5))
        
        # 2. Récupération avec retry et gestion de débit
        data = _recuperer_profil(user)
        
        # 3. Petit délai de "politesse" propre à chaque worker
        time.sleep(random.uniform(1.0, 2.5))
        
        with lock:
            if data:
                stats["succes"] += 1
                # Écriture immédiate (Thread safe via lock)
                with open(DAILY_LOG_FILE, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([aujourdhui, user, cohorte, data["streak"], data["totalXp"], data["hasPlus"], data["hasMax"]])
                
                if stats["succes"] % 25 == 0:
                    progression = (stats["succes"] + stats["erreurs"]) / len(utilisateurs_a_faire) * 100
                    print(f"  [PROGRESSION] {stats['succes'] + stats['erreurs']}/{len(utilisateurs_a_faire)} ({progression:.1f}%) | Dernier: {user}")
            else:
                stats["erreurs"] += 1

    # Création du fichier si absent
    if not fichier_existe:
        with open(DAILY_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker_task, u, i) for i, u in enumerate(utilisateurs_a_faire)]
        for _ in as_completed(futures):
            pass

    print("\n  ✅ Collecte ULTRA-SPEED terminée !")
    print(f"     → Success: {stats['succes']}, Erreurs: {stats['erreurs']}")
    return aujourdhui
