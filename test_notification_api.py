"""
Direct test of local notification API
"""

import requests
import json
from sqlalchemy import text
from database.db_config import engine as db_engine

# Test local API
LOCAL_API = "http://127.0.0.1:8080/notifications/send-notifications"


def main():
    user_id = 1020
    
    print("\n" + "="*70)
    print(f"TESTING LOCAL NOTIFICATION API")
    print("="*70)
    
    # Get user details
    query = text(
        """
        SELECT u.id, u.name
        FROM public."user" u
        WHERE u.id = :user_id
        """
    )
    
    with db_engine.connect() as conn:
        result = conn.execute(query, {"user_id": user_id}).mappings().first()
    
    if not result:
        print(f"❌ User not found")
        return
    
    user = dict(result)
    print(f"\n✓ User: {user['name']} (ID: {user['id']})")
    
    # Build notification request
    payload = {
        "user_id": user_id,
        "user_name": user['name'],
        "weak_indicator": "customer_generation",
        "campaign_day": 2,
    }
    
    print(f"\n📤 Sending to: {LOCAL_API}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(
            LOCAL_API,
            json=payload,
            timeout=30,
        )
        
        print(f"\n✓ Response Status: {response.status_code}")
        
        try:
            data = response.json()
            print(f"\nResponse JSON:")
            print(json.dumps(data, indent=2))
        except:
            print(f"Response Text:\n{response.text}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS!")
        else:
            print(f"\n⚠️  HTTP {response.status_code}")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
