import requests

cases = [
    {
        "user_id": 401,
        "user_name": "Aarav",
        "region": "Jabalpur",
        "language": "Hindi",
        "campaign_day": 2,
    },
    {
        "user_id": 402,
        "user_name": "Priya",
        "region": "Jabalpur",
        "language": "Hindi",
        "campaign_day": 4,
    },
    {
        "user_id": 403,
        "user_name": "Ravi",
        "region": "Vijayawada",
        "language": "Telugu",
        "campaign_day": 12,
        "outcome_hint": "improve lead conversion",
    },
]

for payload in cases:
    r = requests.post("http://localhost:8000/notifications/build", json=payload, timeout=30)
    print("-" * 72)
    print("status", r.status_code, "day", payload["campaign_day"])
    data = r.json()
    print("cohort", data.get("cohort_key"))
    print("video_id", data.get("video_id"))
    print("video_title", data.get("video_title"))
    print("creator", data.get("creator_name"))
    print("title", data.get("notification_title"))
    print("body", data.get("notification_body"))
