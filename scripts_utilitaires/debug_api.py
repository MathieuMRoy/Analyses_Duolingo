
import requests
import json
from datetime import datetime, timedelta
from duolingo_analyzer.config import SESSION, DUOLINGO_PROFILE_API

def test_api():
    # Test checking Luis's profile activity (ID 14)
    user_id = 14
    url = f"https://www.duolingo.com/2017-06-30/users/{user_id}"
    print(f"Checking URL: {url}")
    r = SESSION.get(url)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"Username: {data.get('username')}")
        
        # Structure de streakData
        streak_data = data.get("streakData", {})
        print(f"Streak Data: {json.dumps(streak_data, indent=2)}")
        
        # Structure de activity (si elle existe)
        print(f"Has calendar: {'calendar' in data}")
        if 'calendar' in data:
            print(f"Calendar (last 2): {json.dumps(data['calendar'][-2:], indent=2)}")
            
        # Structure de courses
        courses = data.get("courses", [])
        if courses:
            print(f"First course activity: {json.dumps(courses[0].get('xp'), indent=2)}")

    else:
        print(f"Error: {r.status_code} - {r.text}")

if __name__ == "__main__":
    test_api()
