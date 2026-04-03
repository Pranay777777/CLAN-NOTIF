import requests

BASE = "http://localhost:8000/notifications/build"

payloads = [
    {
        "user_id": 101,
        "user_name": "Aarav",
        "region": "Mumbai",
        "language": "en",
        "campaign_day": 2,
    },
    {
        "user_id": 102,
        "user_name": "Priya",
        "region": "Mumbai",
        "language": "en",
        "campaign_day": 4,
    },
    {
        "user_id": 103,
        "user_name": "Aarav",
        "region": "Mumbai",
        "language": "en",
        "campaign_day": 12,
        "creator_name": "Shikha",
        "creator_team": "Andheri branch",
        "outcome_hint": "double conversion rate",
        "video_id": "vid_298",
        "video_title": "How to qualify leads quickly",
    },
]

for p in payloads:
    r = requests.post(BASE, json=p, timeout=30)
    print("-" * 72)
    print("day:", p["campaign_day"], "status:", r.status_code)
    if r.status_code != 200:
        print(r.text)
        continue
    data = r.json()
    print("cohort_key:", data.get("cohort_key"))
    print("strategy:", data.get("audience_strategy"))
    print("title:", data.get("notification_title"))
    print("body:", data.get("notification_body"))
