import requests

payload = {
    "items": [
        {
            "user_id": 201,
            "user_name": "Aarav",
            "region": "Mumbai",
            "language": "en",
            "campaign_day": 2
        },
        {
            "user_id": 202,
            "user_name": "Priya",
            "region": "Mumbai",
            "language": "en",
            "campaign_day": 4
        },
        {
            "user_id": 203,
            "user_name": "Aarav",
            "region": "Mumbai",
            "language": "en",
            "campaign_day": 12,
            "creator_name": "Shikha",
            "creator_team": "Andheri branch",
            "outcome_hint": "double conversion rate",
            "video_id": "vid_298",
            "video_title": "How to qualify leads quickly"
        }
    ]
}

r = requests.post("http://localhost:8000/notifications/build-batch", json=payload, timeout=30)
print("status:", r.status_code)
if r.status_code != 200:
    print(r.text)
    raise SystemExit(1)

data = r.json()
print("total:", data["total"])
for i, item in enumerate(data["results"], 1):
    print("-" * 72)
    print(f"#{i} day={item['campaign_day']} key={item['cohort_key']}")
    print("title:", item["notification_title"])
    print("body:", item["notification_body"])
