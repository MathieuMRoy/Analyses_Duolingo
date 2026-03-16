
import json
import requests
from datetime import datetime, timedelta
from duolingo_analyzer.config import SESSION

def diagnose():
    # 1. Vérifier les followers de Luis (ID 14)
    user_id = 14
    url = f"https://www.duolingo.com/2017-06-30/friends/users/{user_id}/followers?pageSize=50"
    print(f"--- Diagnostic Followers pour ID {user_id} ---")
    r = SESSION.get(url)
    if r.status_code != 200:
        print(f"Error followers: {r.status_code}")
        return
    
    data = r.json()
    users = data.get("followers", {}).get("users", [])
    print(f"Nombre de followers récupérés : {len(users)}")
    
    active_count = 0
    date_limite = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    for i, user in enumerate(users[:10]):
        uid = user.get("userId")
        uname = user.get("username")
        # Check profile
        profile_url = f"https://www.duolingo.com/2017-06-30/users/{uid}"
        pr = SESSION.get(profile_url)
        if pr.status_code == 200:
            pdata = pr.json()
            streak_data = pdata.get("streakData", {})
            last_date = (streak_data.get("currentStreak") or {}).get("lastExtendedDate")
            is_active = last_date and last_date >= date_limite
            print(f"  [{i}] {uname} (ID:{uid}) -> Last active: {last_date} -> Active? {is_active}")
            if is_active: active_count += 1
        else:
            print(f"  [{i}] {uname} (ID:{uid}) -> Error {pr.status_code}")
    
    print(f"Diagnostic terminé. Actifs sur les 10 premiers : {active_count}")

if __name__ == "__main__":
    diagnose()
