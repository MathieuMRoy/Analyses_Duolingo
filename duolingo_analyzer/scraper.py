"""
Partie 1 : Scraper
Collecte quotidienne des streaks pour les utilisateurs cibles.
"""

from __future__ import annotations

import csv
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import requests

from .config import (
    DAILY_LOG_FILE,
    DAILY_LOG_RETENTION_DAYS,
    DUOLINGO_PROFILE_API,
    MISSING_TARGET_USERS_FILE,
    SESSION,
    now_toronto,
)
from .subscription_detection import detect_has_max_from_user_payload, serialize_optional_bool

MAX_RETRIES = 2
LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus", "HasMax"]
LEGACY_LOG_COLUMNS = ["Date", "Username", "Cohort", "Streak", "TotalXP", "HasPlus"]
MISSING_STATE_COLUMNS = [
    "Username",
    "UserId",
    "Cohort",
    "FirstMissingDate",
    "LastMissingDate",
    "ConsecutiveMisses",
    "LastReason",
]


def _ensure_daily_log_schema() -> bool:
    if not DAILY_LOG_FILE.exists():
        return False

    try:
        with open(DAILY_LOG_FILE, "r", newline="", encoding="utf-8") as source:
            rows = list(csv.reader(source))
    except Exception:
        return True

    if not rows:
        return True

    header = rows[0]
    if header == LOG_COLUMNS:
        return True

    if header != LEGACY_LOG_COLUMNS:
        return True

    upgraded_rows = [LOG_COLUMNS]
    for row in rows[1:]:
        if not row:
            continue
        if row == LEGACY_LOG_COLUMNS or row == LOG_COLUMNS:
            continue
        if len(row) == 6:
            upgraded_rows.append(row + [""])
        elif len(row) >= 7:
            upgraded_rows.append(row[:7])
        else:
            upgraded_rows.append(row + [""] * (7 - len(row)))

    with open(DAILY_LOG_FILE, "w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerows(upgraded_rows)

    print("  [MIGRATION] daily_streaks_log.csv converti au schema 7 colonnes (HasMax ajoute).")
    return True


def _missing_key(username: str, user_id: str | int | None = None) -> str:
    username_value = str(username or "").strip().lower()
    if username_value:
        return f"user:{username_value}"
    return f"id:{str(user_id or '').strip()}"


def _load_missing_state() -> dict[str, dict]:
    if not MISSING_TARGET_USERS_FILE.exists():
        return {}

    try:
        with open(MISSING_TARGET_USERS_FILE, "r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            state = {}
            for row in reader:
                key = _missing_key(row.get("Username", ""), row.get("UserId", ""))
                state[key] = row
            return state
    except Exception:
        return {}


def _save_missing_state(state: dict[str, dict]) -> None:
    MISSING_TARGET_USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(MISSING_TARGET_USERS_FILE, "w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=MISSING_STATE_COLUMNS)
        writer.writeheader()
        for row in sorted(
            state.values(),
            key=lambda item: (
                str(item.get("LastMissingDate", "")),
                str(item.get("Username", "")).lower(),
            ),
        ):
            writer.writerow({column: row.get(column, "") for column in MISSING_STATE_COLUMNS})


def _register_missing(state: dict[str, dict], cible: dict, date_str: str, reason: str) -> None:
    username = str(cible.get("Username") or "").strip()
    user_id = cible.get("UserId", "")
    key = _missing_key(username, user_id)
    existing = state.get(key)

    consecutive = 1
    first_missing = date_str
    if existing:
        try:
            consecutive = int(existing.get("ConsecutiveMisses") or 0) + 1
        except (TypeError, ValueError):
            consecutive = 1
        first_missing = existing.get("FirstMissingDate") or date_str

    state[key] = {
        "Username": username,
        "UserId": user_id,
        "Cohort": cible.get("Cohort", ""),
        "FirstMissingDate": first_missing,
        "LastMissingDate": date_str,
        "ConsecutiveMisses": consecutive,
        "LastReason": reason,
    }


def _clear_missing(state: dict[str, dict], cible: dict) -> None:
    username = str(cible.get("Username") or "").strip()
    user_id = cible.get("UserId", "")
    state.pop(_missing_key(username, user_id), None)


def _extract_user_payload(payload: dict) -> dict | None:
    users = payload.get("users", [payload])
    if not users:
        return None

    user = users[0] if isinstance(users, list) else users
    streak = user.get("streak", user.get("site_streak", 0))
    total_xp = user.get("totalXp", 0)
    has_plus = user.get("hasPlus", False)
    has_max = detect_has_max_from_user_payload(user)
    resolved_username = str(user.get("username") or "").strip()
    resolved_user_id = user.get("id") or user.get("userId") or ""

    return {
        "streak": int(streak) if streak is not None else 0,
        "totalXp": int(total_xp) if total_xp is not None else 0,
        "hasPlus": bool(has_plus),
        "hasMax": has_max,
        "resolvedUsername": resolved_username,
        "resolvedUserId": resolved_user_id,
    }


def _request_profile(url: str, params: dict | None, label: str) -> tuple[dict | None, str | None]:
    for tentative in range(1 + MAX_RETRIES):
        try:
            response = SESSION.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            parsed = _extract_user_payload(data)
            if parsed:
                return parsed, None
            return None, "empty"
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "?"

            if status_code == 429:
                wait = 30 + random.uniform(0, 30)
                print(f"  [RATE LIMIT] Duolingo demande de ralentir pour {label}. Pause de {wait:.0f}s...")
                time.sleep(wait)
                continue

            if exc.response is not None and 400 <= exc.response.status_code < 500:
                return None, f"http_{status_code}"

            if tentative < MAX_RETRIES:
                wait = 2 ** (tentative + 1) + random.uniform(0, 1)
                print(f"  [RETRY {tentative + 1}/{MAX_RETRIES}] {label} (erreur {status_code}, attente {wait:.0f}s)")
                time.sleep(wait)
                continue
            return None, f"http_{status_code}"
        except requests.exceptions.RequestException:
            if tentative < MAX_RETRIES:
                wait = 2 ** (tentative + 1) + random.uniform(0, 1)
                time.sleep(wait)
                continue
            return None, "network"
        except (json.JSONDecodeError, ValueError, KeyError):
            return None, "parse"

    return None, "unknown"


def _fetch_profile_by_username(username: str) -> tuple[dict | None, str | None]:
    return _request_profile(DUOLINGO_PROFILE_API, {"username": username}, username)


def _fetch_profile_by_user_id(user_id: str | int) -> tuple[dict | None, str | None]:
    try:
        normalized_user_id = int(float(user_id))
    except (TypeError, ValueError):
        return None, "invalid_user_id"

    return _request_profile(
        f"https://www.duolingo.com/2017-06-30/users/{normalized_user_id}",
        None,
        f"id={user_id}",
    )


def _resolve_target_profile(cible: dict) -> tuple[dict | None, str, str]:
    username = str(cible.get("Username") or "").strip()
    user_id = str(cible.get("UserId") or "").strip()

    data, reason = _fetch_profile_by_username(username)
    if data:
        data["lookupMode"] = "username"
        return data, "username", ""

    if user_id:
        by_id, id_reason = _fetch_profile_by_user_id(user_id)
        if by_id:
            by_id["lookupMode"] = "user_id"
            return by_id, "user_id", reason or ""
        reason = id_reason or reason

    return None, "missing", reason or "not_found"


def collecter_streaks_quotidiens(utilisateurs_cibles: list[dict]) -> str:
    """
    Partie 1 : collecte quotidienne des streaks avec fallback UserId.
    """
    aujourdhui = now_toronto().strftime("%Y-%m-%d")
    print("\n" + "=" * 60)
    print(f"  PARTIE 1 - COLLECTE QUOTIDIENNE ({aujourdhui})")
    print(f"  Cible : {len(utilisateurs_cibles)} utilisateurs")
    print("=" * 60 + "\n")

    fichier_existe = DAILY_LOG_FILE.exists()
    if fichier_existe:
        fichier_existe = _ensure_daily_log_schema()

    deja_traites = set()
    if fichier_existe:
        try:
            with open(DAILY_LOG_FILE, "r", encoding="utf-8") as rf:
                reader = csv.DictReader(rf)
                for row in reader:
                    if row.get("Date") == aujourdhui:
                        deja_traites.add(row.get("Username"))
        except Exception:
            pass

    utilisateurs_a_faire = [
        cible for cible in utilisateurs_cibles if str(cible.get("Username")) not in deja_traites
    ]

    if not utilisateurs_a_faire:
        print("  [OK] Tous les utilisateurs sont deja a jour pour aujourd'hui.")
        return aujourdhui

    print(f"  Lancement : {len(utilisateurs_a_faire)} restants...")

    lock = threading.Lock()
    missing_state = _load_missing_state()
    stats = {"succes": 0, "erreurs": 0, "rescues_by_id": 0}
    max_workers = 12

    def _worker_task(cible: dict, index: int) -> None:
        user = str(cible.get("Username") or "").strip()
        cohorte = cible.get("Cohort", "")

        time.sleep(random.uniform(0, 5))
        data, lookup_mode, reason = _resolve_target_profile(cible)
        time.sleep(random.uniform(1.0, 2.5))

        with lock:
            if data:
                stats["succes"] += 1
                if lookup_mode == "user_id":
                    stats["rescues_by_id"] += 1
                    resolved_name = data.get("resolvedUsername") or user
                    print(f"  [RECUPERE VIA ID] {user} -> {resolved_name}", flush=True)

                with open(DAILY_LOG_FILE, "a", newline="", encoding="utf-8") as output_file:
                    writer = csv.writer(output_file)
                    writer.writerow(
                        [
                            aujourdhui,
                            user,
                            cohorte,
                            data["streak"],
                            data["totalXp"],
                            data["hasPlus"],
                            serialize_optional_bool(data["hasMax"]),
                        ]
                    )

                _clear_missing(missing_state, cible)

                if stats["succes"] % 25 == 0:
                    progression = (stats["succes"] + stats["erreurs"]) / len(utilisateurs_a_faire) * 100
                    print(
                        f"  [PROGRESSION] {stats['succes'] + stats['erreurs']}/"
                        f"{len(utilisateurs_a_faire)} ({progression:.1f}%) | Dernier: {user}"
                    )
            else:
                stats["erreurs"] += 1
                _register_missing(missing_state, cible, aujourdhui, reason)

    if not fichier_existe:
        with open(DAILY_LOG_FILE, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(LOG_COLUMNS)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker_task, cible, index) for index, cible in enumerate(utilisateurs_a_faire)]
        for _ in as_completed(futures):
            pass

    _save_missing_state(missing_state)

    print("\n  [OK] Collecte terminee !")
    print(
        f"     -> Success: {stats['succes']}, Erreurs: {stats['erreurs']}, "
        f"Recuperes via ID: {stats['rescues_by_id']}"
    )
    if missing_state:
        print(f"     -> Comptes temporairement introuvables: {len(missing_state)}")
    return aujourdhui


RETENTION_DAYS = DAILY_LOG_RETENTION_DAYS


def purger_anciennes_donnees() -> None:
    """Supprime les lignes de plus de RETENTION_DAYS jours du daily log."""
    if not DAILY_LOG_FILE.exists():
        return

    cutoff = (now_toronto() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")

    try:
        with open(DAILY_LOG_FILE, "r", encoding="utf-8", newline="") as source:
            reader = csv.reader(source)
            header = next(reader, None)
            if header is None:
                return
            rows = list(reader)
    except Exception:
        return

    kept = [row for row in rows if row and row[0] >= cutoff]
    removed = len(rows) - len(kept)

    if removed == 0:
        return

    with open(DAILY_LOG_FILE, "w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(kept)

    print(f"  [PURGE] {removed} lignes supprimees (avant {cutoff}), {len(kept)} conservees.")
