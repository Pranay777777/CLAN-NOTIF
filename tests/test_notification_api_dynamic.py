import requests
import json

def test_api_notification():
    url = "http://127.0.0.1:8000/notifications/build"
    
    # Request for Day 5 (which requires yesterday_count and last_7_days_activity_count)
    # We omit the metrics so the API has to fetch them from DB
    payload = {
        "user_id": 204,
        "user_name": "Test User",
        "campaign_day": 5,
        "region": "South",
        "language": "hindi"
    }
    
    print(f"Sending request to {url}...")
    try:
        response = requests.post(url, json=payload)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("\nNotification Result:")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Connection Error: {e}")

if __name__ == "__main__":
    test_api_notification()
