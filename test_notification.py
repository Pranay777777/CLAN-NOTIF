import requests
import json

# Test Day 2 (Video day)
response = requests.post(
    'http://localhost:8080/notifications/send',
    headers={'Content-Type': 'application/json'},
    json={
        'user_id': 953,
        'campaign_day': 2
    }
)

print("Status Code:", response.status_code)
print("\nResponse:")
print(json.dumps(response.json(), indent=2))