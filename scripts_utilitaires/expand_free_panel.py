from __future__ import annotations

import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import pandas as pd

from duolingo_analyzer.config import (
    DAILY_LOG_FILE,
    DISCOVERY_ACTIVE_LOOKBACK_DAYS,
    SESSION,
    TARGET_USERS_FILE,
    now_toronto,
)
from duolingo_analyzer.discovery import (
    DUOLINGO_JWT,
    TARGET_USER_COLUMNS,
    _determiner_cohorte,
    _extraire_viewer_id,
    _normalize_target_user_record,
    _trouver_plus_de_candidats,
    _verifier_utilisateur,
)


FREE_TARGET_TO_ADD = 5000
SEED_LIMIT = 400
FOLLOWERS_PER_SEED = 250
SAVE_INTERVAL = 100
HEARTBEAT_INTERVAL = 10
PROFILE_WORKERS = 12
PROFILE_DELAY_MIN = 0.03
PROFILE_DELAY_MAX = 0.12


def _load_target_users() -> pd.DataFrame:
    df = pd.read_csv(TARGET_USERS_FILE)
    for col in TARGET_USER_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[TARGET_USER_COLUMNS].copy()


def _latest_free_seed_ids() -> list[int]:
    targets = _load_target_users()
    log_df = pd.read_csv(DAILY_LOG_FILE)
    last_date = str(log_df["Date"].max())
    latest = log_df[log_df["Date"].astype(str) == last_date].copy()
    latest["HasPlus"] = latest["HasPlus"].astype(str).str.strip().str.lower()
    latest["free_flag"] = latest["HasPlus"].eq("false")
    latest["Streak"] = pd.to_numeric(latest["Streak"], errors="coerce").fillna(0)

    merged = targets.merge(
        latest[["Username", "free_flag", "Streak"]],
        on="Username",
        how="left",
    )
    merged["UserId"] = pd.to_numeric(merged["UserId"], errors="coerce")
    merged["TotalXP"] = pd.to_numeric(merged["TotalXP"], errors="coerce").fillna(0)

    seeds = merged[
        merged["free_flag"].fillna(False) & merged["UserId"].notna()
    ].sort_values(["TotalXP", "Streak"], ascending=[False, False])

    ids = seeds["UserId"].astype(int).drop_duplicates().tolist()
    return ids[:SEED_LIMIT]


def _manual_seed_ids(seed_usernames: list[str]) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    for username in seed_usernames:
        profil = _verifier_utilisateur(username)
        if not profil:
            print(f"  [SEED IGNORE] Compte introuvable: {username}")
            continue

        uid = profil.get("id")
        if uid is None:
            print(f"  [SEED IGNORE] ID manquant pour: {username}")
            continue

        try:
            uid = int(uid)
        except (TypeError, ValueError):
            print(f"  [SEED IGNORE] ID invalide pour: {username}")
            continue

        if uid not in seen:
            ids.append(uid)
            seen.add(uid)

    return ids


def _fetch_profile_by_id(user_id: int) -> dict | None:
    url = f"https://www.duolingo.com/2017-06-30/users/{user_id}"
    try:
        time.sleep(random.uniform(PROFILE_DELAY_MIN, PROFILE_DELAY_MAX))
        response = SESSION.get(url, timeout=20)
        response.raise_for_status()
        user = response.json()
    except Exception:
        return None

    streak_data = user.get("streakData") or {}
    current_streak = streak_data.get("currentStreak") or {}
    last_date = current_streak.get("lastExtendedDate") or ""
    streak_length = int(streak_data.get("length") or user.get("streak") or 0)
    active_cutoff = (now_toronto() - timedelta(days=DISCOVERY_ACTIVE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    username = str(user.get("username") or "").strip()
    total_xp = int(user.get("totalXp") or 0)
    has_plus = bool(user.get("hasPlus", False))
    is_active = bool(last_date and last_date >= active_cutoff and streak_length > 0)

    if not username:
        return None

    return {
        "Username": username,
        "UserId": int(user.get("id") or user_id),
        "TotalXP": total_xp,
        "Cohort": _determiner_cohorte(total_xp),
        "HasPlus": has_plus,
        "Active": is_active,
    }


def _fetch_profiles_batch(candidates: list[dict], max_workers: int = PROFILE_WORKERS) -> list[dict]:
    if not candidates:
        return []

    profiles: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_profile_by_id, int(candidate["id"])): candidate
            for candidate in candidates
            if candidate.get("id") is not None
        }
        for future in as_completed(futures):
            try:
                profile = future.result()
            except Exception:
                profile = None
            if profile:
                profiles.append(profile)
    return profiles


def _append_rows(new_rows: list[dict]) -> None:
    if not new_rows:
        return

    existing = _load_target_users().to_dict("records")
    seen = set()
    merged: list[dict] = []

    for row in existing + new_rows:
        normalized = _normalize_target_user_record(row)
        if not normalized:
            continue
        key = normalized["Username"].lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(normalized)

    pd.DataFrame(merged, columns=TARGET_USER_COLUMNS).to_csv(TARGET_USERS_FILE, index=False)


def expand_free_panel(
    target_to_add: int = FREE_TARGET_TO_ADD,
    seed_usernames: list[str] | None = None,
    workers: int = PROFILE_WORKERS,
) -> int:
    if not DUOLINGO_JWT:
        raise RuntimeError("DUOLINGO_JWT manquant. Impossible d'etendre le panel gratuit.")

    viewer_id = _extraire_viewer_id(DUOLINGO_JWT)
    if not viewer_id:
        raise RuntimeError("Impossible d'extraire le viewerId depuis DUOLINGO_JWT.")

    targets_df = _load_target_users()
    seen_usernames = set(targets_df["Username"].astype(str).str.strip().str.lower())
    manual_ids = _manual_seed_ids(seed_usernames or [])
    auto_ids = _latest_free_seed_ids()
    queued_ids = manual_ids + [uid for uid in auto_ids if uid not in set(manual_ids)]
    seen_ids = set(pd.to_numeric(targets_df["UserId"], errors="coerce").dropna().astype(int).tolist())

    random.shuffle(queued_ids)
    new_rows: list[dict] = []
    queue_index = 0

    print("=" * 70)
    print(f"EXPANSION PANEL GRATUIT - OBJECTIF {target_to_add} UTILISATEURS")
    if seed_usernames:
        print(f"Graines manuelles : {', '.join(seed_usernames)}")
    print(f"Graines gratuites detectees : {len(queued_ids)}")
    print("=" * 70)

    try:
        while len(new_rows) < target_to_add and queue_index < len(queued_ids):
            source_id = queued_ids[queue_index]
            queue_index += 1

            if queue_index == 1 or queue_index % HEARTBEAT_INTERVAL == 0:
                print(
                    f"[SCAN] seed {queue_index}/{len(queued_ids)} | "
                    f"ajoutes={len(new_rows)}/{target_to_add}",
                    flush=True,
                )

            candidates = _trouver_plus_de_candidats(source_id, viewer_id, FOLLOWERS_PER_SEED)
            random.shuffle(candidates)
            if not candidates:
                print(f"  [VIDE] aucun follower exploitable pour l'id {source_id}", flush=True)
                continue

            unique_candidates: list[dict] = []
            for candidate in candidates:
                candidate_id = candidate.get("id")
                if not candidate_id:
                    continue
                try:
                    candidate_id = int(candidate_id)
                except (TypeError, ValueError):
                    continue
                if candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                candidate["id"] = candidate_id
                unique_candidates.append(candidate)

            profiles = _fetch_profiles_batch(unique_candidates, max_workers=workers)

            for profile in profiles:
                key = profile["Username"].lower()
                if key in seen_usernames:
                    continue
                if profile["HasPlus"] or not profile["Active"]:
                    continue

                new_rows.append(profile)
                seen_usernames.add(key)
                queued_ids.append(profile["UserId"])

                if len(new_rows) <= 5 or len(new_rows) % 25 == 0:
                    print(
                        f"  [AJOUT] {profile['Username']} | XP={profile['TotalXP']} | "
                        f"total={len(new_rows)}/{target_to_add}",
                        flush=True,
                    )

                if len(new_rows) % SAVE_INTERVAL == 0:
                    _append_rows(new_rows)
                    print(f"  -> {len(new_rows)} gratuits ajoutes / {target_to_add}", flush=True)

                if len(new_rows) >= target_to_add:
                    break
    except KeyboardInterrupt:
        print("\n[ARRET] Interruption detectee. Sauvegarde du progres en cours...", flush=True)
        _append_rows(new_rows)
        print(f"[ARRET] {len(new_rows)} gratuits deja sauves.", flush=True)
        return len(new_rows)

    _append_rows(new_rows)
    print(f"\nTermine : {len(new_rows)} utilisateurs gratuits ajoutes.")
    print(f"Nouveau total cible : {len(_load_target_users())}")
    return len(new_rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ajoute des utilisateurs gratuits au panel Duolingo.")
    parser.add_argument("--target", type=int, default=FREE_TARGET_TO_ADD, help="Nombre de comptes gratuits a ajouter.")
    parser.add_argument(
        "--seed-username",
        action="append",
        default=[],
        help="Compte de depart a explorer. Option repetable.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=PROFILE_WORKERS,
        help="Nombre de verifications de profils en parallele.",
    )
    args = parser.parse_args()

    expand_free_panel(
        target_to_add=args.target,
        seed_usernames=args.seed_username,
        workers=max(1, int(args.workers)),
    )
