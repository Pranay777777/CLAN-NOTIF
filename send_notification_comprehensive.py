"""
Fetch all valid parameters from DB for user 1020 and send notification
"""

import requests
import json
from sqlalchemy import text
from database.db_config import engine as db_engine

# API endpoint
API_URL = "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications"


def fetch_user_complete_data(user_id: int):
    """Fetch complete user data from PostgreSQL"""
    query = text(
        """
        SELECT 
            u.id,
            u.name,
            u.email,
            u.account_id,
            u.branch,
            u.zone as region,
            u.app_language_id,
            ml.language_code,
            e.videos_count,
            e.status as expert_status
        FROM public."user" u
        LEFT JOIN public.expert_user e ON e.user_id = u.id
        LEFT JOIN public.md_app_languages ml ON ml.id = u.app_language_id
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
        print(f"❌ Error fetching user: {e}")
        return None


def fetch_weak_indicators():
    """Fetch all valid weak indicators from database"""
    query = text(
        """
        SELECT DISTINCT 
            indicator_code,
            indicator_label,
            indicator_description
        FROM public.kpi_indicators
        ORDER BY indicator_label
        """
    )
    
    indicators = {}
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query).mappings().all()
        
        for row in results:
            code = row.get('indicator_code', '').lower()
            label = row.get('indicator_label', '')
            if code:
                indicators[code] = label
        
        return indicators if indicators else None
    except Exception as e:
        print(f"⚠️  Could not fetch indicators from kpi_indicators: {e}")
        return None


def fetch_user_indicators(user_id: int):
    """Fetch weak indicators specific to the user"""
    query = text(
        """
        SELECT 
            indicator_code,
            indicator_label,
            score,
            is_weak
        FROM public.user_indicators
        WHERE user_id = :user_id
        ORDER BY score ASC
        LIMIT 5
        """
    )
    
    indicators = {}
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query, {"user_id": user_id}).mappings().all()
        
        for row in results:
            code = row.get('indicator_code', '').lower()
            label = row.get('indicator_label', '')
            score = row.get('score')
            is_weak = row.get('is_weak')
            
            if code:
                indicators[code] = {
                    "label": label,
                    "score": score,
                    "is_weak": is_weak,
                }
        
        return indicators if indicators else None
    except Exception as e:
        print(f"⚠️  Could not fetch user indicators: {e}")
        return None


def determine_campaign_day(user_id: int):
    """Determine the appropriate campaign day for the user"""
    query = text(
        """
        SELECT 
            journey_day,
            days_in_journey,
            last_notification_day
        FROM public.user_journey
        WHERE user_id = :user_id
        ORDER BY last_update DESC
        LIMIT 1
        """
    )
    
    try:
        with db_engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id}).mappings().first()
        
        if result:
            journey = dict(result)
            # Default campaign day is 2
            campaign_day = journey.get('journey_day', 2) or 2
            return campaign_day
    except Exception as e:
        print(f"⚠️  Could not fetch journey: {e}")
    
    return 2  # Default


def send_notification_to_user(user_id: int):
    """Fetch all parameters from DB and send notification"""
    
    print("\n" + "="*70)
    print(f"COMPREHENSIVE NOTIFICATION SENDER - User {user_id}")
    print("="*70)
    
    # Step 1: Fetch user details
    print(f"\n📋 STEP 1: Fetching user details from database...")
    user_data = fetch_user_complete_data(user_id)
    
    if not user_data:
        print(f"❌ User {user_id} not found in database")
        return False
    
    print(f"✓ User found:")
    print(f"  ID: {user_data.get('id')}")
    print(f"  Name: {user_data.get('name')}")
    print(f"  Email: {user_data.get('email')}")
    print(f"  Account ID: {user_data.get('account_id')}")
    print(f"  Branch: {user_data.get('branch')}")
    print(f"  Region: {user_data.get('region')}")
    print(f"  Language: {user_data.get('language_code')}")
    print(f"  Videos Count: {user_data.get('videos_count')}")
    
    # Step 2: Fetch weak indicators (KIIs)
    print(f"\n📊 STEP 2: Fetching user's Key Input Indicators (KIIs)...")
    query = text(
        """
        SELECT DISTINCT 
            ki.kii_id,
            k.name,
            k.code,
            AVG(ki.kill_value) as avg_value
        FROM public.key_input_indicators ki
        LEFT JOIN public.kii k ON k.id = ki.kii_id
        WHERE ki.user_id = :user_id
        AND ki.status = 1
        GROUP BY ki.kii_id, k.name, k.code
        ORDER BY avg_value ASC
        LIMIT 5
        """
    )
    
    user_kii = {}
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query, {"user_id": user_id}).mappings().all()
        
        for row in results:
            code = row.get('code', '').lower() if row.get('code') else f"kii_{row.get('kii_id')}"
            name = row.get('name', '')
            avg_val = row.get('avg_value')
            
            if code:
                user_kii[code] = {
                    "name": name,
                    "avg_value": float(avg_val) if avg_val else 0,
                }
    except Exception as e:
        print(f"⚠️  Error fetching KIIs: {e}")
    
    if user_kii:
        print(f"✓ User's weak indicators (KIIs):")
        for code, details in user_kii.items():
            print(f"  • {code}: {details['name']} (avg value: {details['avg_value']:.2f})")
        
        # Use the weakest indicator (lowest value)
        weak_indicator = min(user_kii.items(), key=lambda x: x[1]['avg_value'])[0]
        print(f"\n  Using weakest indicator: {weak_indicator}")
    else:
        print(f"⚠️  No KIIs found, using default")
        weak_indicator = "customer_generation"
        print(f"  Using default: {weak_indicator}")
    
    # Step 3: Fetch campaign day
    print(f"\n📅 STEP 3: Determining campaign day...")
    campaign_day = 2  # Default to day 2
    print(f"✓ Campaign day: {campaign_day}")
    
    # Step 4: Build notification payload
    print(f"\n📤 STEP 4: Building notification payload...")
    
    notification_obj = {
        "user_id": user_id,
        "user_name": user_data.get('name') or f"User {user_id}",
        "weak_indicator": weak_indicator,
        "campaign_day": campaign_day,
        "branch": user_data.get('branch'),
        "region": user_data.get('region'),
        "language": user_data.get('language_code') or "en",
        "watched_video_ids": [],
        "months_in_role": None,
        # External API required fields
        "title": f"Day {campaign_day} Notification for {user_data.get('name')}",
        "notification_type": f"campaign_day_{campaign_day}",
        "reference_id": user_id * 1000 + campaign_day,
    }
    
    payload = [notification_obj]
    
    print(f"\n✓ Payload ready:")
    print(json.dumps(payload, indent=2))
    
    # Step 5: Send notification
    print(f"\n📡 STEP 5: Sending notification...")
    print(f"Calling: POST {API_URL}")
    
    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        print(f"Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ SUCCESS!")
            print(f"\nResponse:")
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
    
    print("\n" + "="*80)
    print("FETCH ALL PARAMETERS FROM DB AND SEND NOTIFICATION")
    print("="*80)
    
    success = send_notification_to_user(user_id)
    
    print("\n" + "="*80)
    if success:
        print("✅ NOTIFICATION SENT SUCCESSFULLY!")
    else:
        print("⚠️  NOTIFICATION SEND FAILED - See errors above")
    print("="*80 + "\n")
    
    return success


if __name__ == "__main__":
    main()
