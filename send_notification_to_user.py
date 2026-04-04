"""
Send notification to a user with ID 1020 using campaign day 2 (notification path 2)
"""

import requests
import json
from sqlalchemy import text
from database.db_config import engine as db_engine

# API endpoint
API_URL = "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications"
INDICATORS_URL = "http://127.0.0.1:8080/indicators"


def get_user_details(user_id: int):
    """Fetch user details from PostgreSQL"""
    query = text(
        """
        SELECT 
            u.id,
            u.name,
            e.account_id
        FROM public."user" u
        LEFT JOIN public.expert_user e ON e.user_id = u.id
        WHERE u.id = :user_id
        LIMIT 1
        """
    )
    
    try:
        with db_engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id}).mappings().first()
        
        if result:
            return dict(result)
        return None
    except Exception as e:
        print(f"Error fetching user: {e}")
        return None


def send_notification(user_id: int, user_name: str, weak_indicator: str = None, campaign_day: int = 2, valid_indicators: set = None):
    """Send notification to user"""
    
    if not valid_indicators:
        valid_indicators = set()
    
    # If weak_indicator not provided, use default
    if not weak_indicator:
        weak_indicator = "customer_generation"
    
    # Validate weak_indicator
    if valid_indicators and weak_indicator not in valid_indicators:
        print(f"\n❌ ERROR: Invalid weak_indicator '{weak_indicator}'")
        print(f"Valid indicators are: {sorted(valid_indicators)}")
        return False
    
    # Build request payload (API expects a list with title, notification_type, reference_id)
    notification_obj = {
        "user_id": user_id,
        "user_name": user_name,
        "weak_indicator": weak_indicator,
        "watched_video_ids": [],
        "months_in_role": None,
        "campaign_day": campaign_day,
        "title": f"Notification for {user_name}",  # Required by external API
        "notification_type": "campaign_day_2",  # Required by external API
        "reference_id": user_id * 1000 + campaign_day,  # Required - as integer
    }
    
    # Wrap in list for API
    payload = [notification_obj]
    
    print("\n" + "="*70)
    print(f"SENDING NOTIFICATION TO USER {user_id}")
    print("="*70)
    print(f"\nRequest payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        print(f"\nCalling API: POST {API_URL}")
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        print(f"Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # API returns a list of responses
            if isinstance(data, list) and len(data) > 0:
                notification = data[0]
                print(f"\n✅ SUCCESS!")
                print(f"\nNotification Details:")
                print(f"  Campaign Day: {notification.get('notification', {}).get('campaign_day')}")
                print(f"  Title: {notification.get('notification', {}).get('notification_title')}")
                print(f"  Body: {notification.get('notification', {}).get('notification_body')}")
                print(f"  Video: {notification.get('notification', {}).get('video_title')}")
                print(f"  Creator: {notification.get('notification', {}).get('creator_name')}")
                
                if notification.get('remote_send_response'):
                    remote = notification['remote_send_response']
                    print(f"\nRemote Send Status: {remote.get('status_code')}")
            else:
                print(f"\n✅ SUCCESS!")
            
            print(f"\nFull Response:")
            print(json.dumps(data, indent=2))
            
            return True
        else:
            print(f"\n❌ ERROR: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error sending notification: {e}")
        return False


def main():
    user_id = 1020
    campaign_day = 2
    
    print("\n" + "="*70)
    print(f"NOTIFICATION SENDER - User {user_id}, Campaign Day {campaign_day}")
    print("="*70)
    
    # Fetch available indicators from API
    print(f"\n📡 Fetching valid indicators from API...")
    valid_indicators = set()
    try:
        response = requests.get(INDICATORS_URL, timeout=10)
        if response.status_code == 200:
            indicators_data = response.json()
            # Extract indicator names from the structure
            if isinstance(indicators_data, dict):
                valid_indicators = set(indicators_data.keys())
            elif isinstance(indicators_data, list):
                valid_indicators = set(indicators_data)
            print(f"✓ Found {len(valid_indicators)} indicators")
    except Exception as e:
        print(f"⚠️  Could not fetch indicators from API: {e}")
        print("   Using default indicators...")
        valid_indicators = {"customer_generation", "sales_conversion", "customer_retention"}
    
    # Fetch user details
    print(f"\n🔍 Fetching user details for user_id={user_id}...")
    user = get_user_details(user_id)
    
    if not user:
        print(f"❌ User {user_id} not found in database")
        return False
    
    print(f"✓ User found:")
    print(f"  ID: {user.get('id')}")
    print(f"  Name: {user.get('name')}")
    print(f"  Account ID: {user.get('account_id')}")
    
    # Use user's name from database
    user_name = user.get('name') or f"User {user_id}"
    
    # Available weak indicators
    print(f"\n📋 Available weak indicators:")
    for indicator in sorted(valid_indicators):
        print(f"  • {indicator}")
    
    # Use first available indicator (customer_generation)
    weak_indicator = "customer_generation"
    print(f"\n📌 Using weak_indicator: {weak_indicator}")
    
    # Send notification
    success = send_notification(
        user_id=user_id,
        user_name=user_name,
        weak_indicator=weak_indicator,
        campaign_day=campaign_day,
        valid_indicators=valid_indicators,
    )
    
    if success:
        print("\n✅ Notification sent successfully!")
    else:
        print("\n⚠️  Failed to send notification")
    
    print("\n" + "="*70)
    
    return success


if __name__ == "__main__":
    main()
