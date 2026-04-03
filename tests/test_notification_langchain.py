import requests

payload = {
    "user_id": 901,
    "user_name": "Aarav",
    "role": "RM",
    "region": "North",
    "weak_indicator": "customer_generation",
    "journey_day": 10,
    "watched_ids": [1, 2],
    "months_in_role": 2,
}

r = requests.post("http://localhost:8000/recommend-video", json=payload, timeout=60)
print("status:", r.status_code)
if r.status_code != 200:
    print(r.text)
    raise SystemExit(1)

data = r.json()
print("notification_title:", data.get("notification_title", ""))
print("notification_body:", data.get("notification_body", ""))
print("title_words:", len(str(data.get("notification_title", "")).split()))
print("title_len:", len(str(data.get("notification_title", ""))))
print("body_len:", len(str(data.get("notification_body", ""))))
print("contains_name:", "aarav" in str(data.get("notification_body", "")).lower())
