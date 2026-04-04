"""
Send Day 2 notification with complete payload including video recommendation
"""

import requests
import json
from sqlalchemy import text
from database.db_config import engine as db_engine

# API endpoint
API_URL = "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications"


def fetch_user_data(user_id: int):
    """Fetch complete user data"""
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


def fetch_video_recommendation(user_id: int, weak_indicator: str = None):
    """Fetch a recommended video for the user based on weak indicator"""
    
    if not weak_indicator:
        weak_indicator = "customer_generation"
    
    # Query to find videos matching the weak indicator
    query = text(
        """
        SELECT 
            c.id,
            c.title,
            c.description,
            u_creator.name as creator_name,
            ml.language_code,
            c.thumbnail_url,
            c.length_mins
        FROM public.content c
        LEFT JOIN public."user" u_creator ON u_creator.id = c.created_by
        LEFT JOIN public.md_app_languages ml ON ml.id = c.language_id
        LEFT JOIN public."user" u_target ON u_target.id = :user_id
        WHERE c.language_id = u_target.app_language_id
        AND c.created_at IS NOT NULL
        ORDER BY c.created_at DESC
        LIMIT 5
        """
    )
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(
                query, 
                {"user_id": user_id}
            ).mappings().all()
        
        if results:
            # Return first/best matching video
            video = dict(results[0])
            return video
        
        return None
    except Exception as e:
        print(f"⚠️  Error fetching video: {e}")
        return None


def generate_day2_notification(user_data: dict, video_data: dict = None):
    """Generate Day 2 notification with title and description"""
    
    user_name = user_data.get('name', 'User')
    
    if video_data:
        video_title = video_data.get('title', 'Check out this video')
        creator_name = video_data.get('creator_name', 'someone from your region')
        
        title = f"See how {creator_name} closes more deals"
        description = f"See the exact approach {creator_name} uses to close more deals in the field. {video_title}"
    else:
        title = f"Day 2: Learn from top performers"
        description = f"Discover proven techniques from top relationship managers to boost your performance"
    
    return {
        "title": title,
        "description": description,
    }


def send_day2_notification(user_id: int):
    """Fetch user data, recommend video, and send Day 2 notification"""
    
    print("\n" + "="*80)
    print("SEND DAY 2 NOTIFICATION WITH VIDEO RECOMMENDATION")
    print("="*80)
    
    # Step 1: Fetch user data
    print(f"\n📋 STEP 1: Fetching user {user_id} details...")
    user_data = fetch_user_data(user_id)
    
    if not user_data:
        print(f"❌ User {user_id} not found")
        return False
    
    print(f"✓ User found: {user_data.get('name')} ({user_data.get('email')})")
    print(f"  Account: {user_data.get('account_id')} | Branch: {user_data.get('branch')} | Region: {user_data.get('region')}")
    
    # Step 2: Determine weak indicator
    print(f"\n📊 STEP 2: Determining weak indicator...")
    weak_indicator = "customer_generation"  # Default for Day 2
    print(f"✓ Weak indicator: {weak_indicator}")
    
    # Step 3: Fetch video recommendation
    print(f"\n🎬 STEP 3: Fetching video recommendation...")
    video_data = fetch_video_recommendation(user_id, weak_indicator)
    
    if video_data:
        print(f"✓ Video found: {video_data.get('title')}")
        print(f"  Creator: {video_data.get('creator_name')}")
        print(f"  Language: {video_data.get('language_code')}")
    else:
        print(f"⚠️  No video found, will use generic notification")
        video_data = None
    
    # Step 4: Generate notification copy
    print(f"\n✍️  STEP 4: Generating Day 2 notification copy...")
    notification_copy = generate_day2_notification(user_data, video_data)
    print(f"✓ Title: {notification_copy.get('title')}")
    print(f"✓ Description: {notification_copy.get('description')[:60]}...")
    
    # Step 5: Build complete payload with all required fields
    print(f"\n📤 STEP 5: Building complete notification payload...")
    
    campaign_day = 2
    reference_id = user_id * 1000 + campaign_day
    
    notification_obj = {
        "user_id": user_id,
        "title": notification_copy.get('title'),
        "description": notification_copy.get('description'),
        "notification_type": "PUSH_VIDEO_FOR_USER",  # Day 2 notification type
        "reference_id": reference_id,
        "video_popup": True,
        "image": None,
        # Additional metadata for internal use
        "user_name": user_data.get('name'),
        "weak_indicator": weak_indicator,
        "campaign_day": campaign_day,
        "branch": user_data.get('branch'),
        "region": user_data.get('region'),
        "language": user_data.get('language_code') or "en",
    }
    
    payload = [notification_obj]
    
    print(f"\n✓ Complete payload:")
    print(json.dumps(payload, indent=2))
    
    # Step 6: Send notification
    print(f"\n📡 STEP 6: Sending notification to external API...")
    print(f"API Endpoint: {API_URL}")
    
    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        print(f"\nResponse Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ SUCCESS!")
            print(f"\nAPI Response:")
            print(json.dumps(data, indent=2))
            
            print(f"\n" + "="*80)
            print("NOTIFICATION DELIVERY SUMMARY")
            print("="*80)
            print(f"✓ User: {user_data.get('name')} (ID: {user_id})")
            print(f"✓ Notification Type: Day 2 - PUSH_VIDEO_FOR_USER")
            print(f"✓ Title: {notification_copy.get('title')}")
            print(f"✓ Reference ID: {reference_id}")
            print(f"✓ Status: Delivered")
            
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
    
    success = send_day2_notification(user_id)
    
    print("\n" + "="*80)
    if success:
        print("✅ NOTIFICATION SENT SUCCESSFULLY!")
    else:
        print("⚠️  FAILED TO SEND NOTIFICATION")
    print("="*80 + "\n")
    
    return success


if __name__ == "__main__":
    main()
