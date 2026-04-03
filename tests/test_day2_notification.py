"""
Test Day 2 Notification Sender

This script demonstrates how to use the send_notifications() function
to build and test Day 2 notifications.

Usage:
    .\.venv\Scripts\python.exe test_day2_notification.py

Output:
    - Prints notification details
    - Saves to test_notifications_log.json
    - Ready for manual testing in app
"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Import resolver
from notificationschema.resolver import get_resolver

def test_day2_notification():
    """Test building Day 2 notification for a user."""
    
    print("=" * 80)
    print("🔔 DAY 2 NOTIFICATION TEST")
    print("=" * 80)
    
    # Initialize resolver
    resolver = get_resolver()
    print("✓ Resolver initialized")
    
    # Parameters for notification
    user_id = 953
    user_name = 'Shashank'
    weak_indicator = 'customer_generation'
    campaign_day = 2
    watched_video_ids = []
    
    print(f"\n📝 Building notification with parameters:")
    print(f"   user_id: {user_id}")
    print(f"   user_name: {user_name}")
    print(f"   weak_indicator: {weak_indicator}")
    print(f"   campaign_day: {campaign_day}")
    
    # Build notification
    print(f"\n⏳ Building notification...")
    response = resolver.send_notifications(
        user_id=user_id,
        user_name=user_name,
        weak_indicator=weak_indicator,
        watched_video_ids=watched_video_ids,
        campaign_day=campaign_day,
    )
    
    # Check result
    if response['success']:
        print("✅ SUCCESS: Notification built successfully\n")
        
        notification = response['notification']
        
        print("📱 NOTIFICATION DETAILS:")
        print("-" * 80)
        print(f"Campaign Day:        {notification['campaign_day']}")
        print(f"Title:               {notification['notification_title']}")
        print(f"Body:                {notification['notification_body']}")
        print(f"Audience Strategy:   {notification['audience_strategy']}")
        print(f"Cohort Key:          {notification['cohort_key']}")
        print(f"Video Title:         {notification['video_title']}")
        print(f"Creator Name:        {notification['creator_name']}")
        print(f"Action:              {notification['action']}")
        print(f"Deep Link:           {notification['deep_link']}")
        print(f"Should Send:         {notification['should_send']}")
        print("-" * 80)
        
        print(f"\n💾 Test Log Path:     {response['test_file_path']}")
        
        # Print full JSON
        print("\n📋 FULL JSON RESPONSE:")
        print("-" * 80)
        print(json.dumps(response, indent=2, default=str))
        print("-" * 80)
        
        # Read and display test log
        test_log_path = Path(response['test_file_path'])
        if test_log_path.exists():
            print(f"\n📂 Test log file contents ({test_log_path}):")
            print("-" * 80)
            with open(test_log_path, 'r', encoding='utf-8') as f:
                log_data = json.load(f)
                print(json.dumps(log_data, indent=2, ensure_ascii=False))
            print("-" * 80)
        
        print("\n✅ TEST COMPLETE")
        print("\n🧪 Next Steps:")
        print("1. Open your app")
        print(f"2. Login as user {user_id} ({user_name})")
        print("3. Verify Day 2 notification appears")
        print("4. Click notification to view video")
        
    else:
        print(f"❌ FAILED: {response['error']}\n")
        print(json.dumps(response, indent=2, default=str))
    
    print("\n" + "=" * 80)


def test_batch_users():
    """Test building notifications for multiple users (from database)."""
    
    print("\n" + "=" * 80)
    print("📊 BATCH USER TEST (Get all users at campaign day 2)")
    print("=" * 80)
    
    resolver = get_resolver()
    
    # Get all users at day 2
    print(f"\n⏳ Fetching all users at campaign day 2...")
    users = resolver._get_all_users(campaign_day=2)
    
    print(f"✓ Found {len(users)} users\n")
    
    if users:
        print("Sample users:")
        print("-" * 80)
        for user in users[:5]:  # Show first 5
            print(f"  ID: {user.get('id'):<5} | Name: {user.get('name'):<20} | Region: {user.get('zone', 'unknown')}")
        
        if len(users) > 5:
            print(f"  ... and {len(users) - 5} more")
        print("-" * 80)
    else:
        print("⚠️  No users found at campaign day 2")
    
    print("\n" + "=" * 80)


def main():
    """Main test function."""
    
    # Test 1: Single user Day 2 notification
    test_day2_notification()
    
    # Test 2: Batch users query (optional)
    test_batch_users()


if __name__ == "__main__":
    main()
