#!/usr/bin/env python3
"""
Test script for the new POST /notifications/send-notifications endpoint.

This script tests the endpoint with a sample user and verifies the response format.
"""

import requests
import json
from typing import Dict, Any
import time

# Configuration
API_BASE = "http://localhost:8000"
ENDPOINT = f"{API_BASE}/notifications/send-notifications"

def test_send_notification_endpoint():
    """Test the send_notifications endpoint with a real user."""
    
    # Sample request - using user 953 with confirmed data
    payload = {
        "user_id": 953,
        "user_name": "Shashank",
        "weak_indicator": "potential_channel_partners_identified",
        "watched_video_ids": [100, 200],
        "months_in_role": 6,
        "campaign_day": 2,
    }
    
    print("=" * 80)
    print("TESTING: POST /notifications/send-notifications")
    print("=" * 80)
    print(f"\nEndpoint: {ENDPOINT}")
    print(f"\nPayload:\n{json.dumps(payload, indent=2)}")
    
    try:
        print(f"\n[*] Sending request...")
        start = time.time()
        response = requests.post(ENDPOINT, json=payload, timeout=30)
        elapsed = time.time() - start
        
        print(f"\n[✓] Response received in {elapsed:.2f}s")
        print(f"Status Code: {response.status_code}")
        
        # Parse response
        if response.status_code == 200:
            data = response.json()
            print(f"\n[✓] SUCCESS! Notification sent.")
            print(f"\nResponse Body:\n{json.dumps(data, indent=2)}")
            
            # Validate response structure
            assert data.get("success") == True, "success should be True"
            assert data.get("user_id") == 953, f"user_id should be 953, got {data.get('user_id')}"
            assert data.get("notification") is not None, "notification should not be None"
            assert data.get("test_file_path") is not None, "test_file_path should not be None"
            
            notif = data.get("notification", {})
            assert notif.get("campaign_day") == 2, "campaign_day should be 2"
            assert notif.get("notification_title"), "notification_title should not be empty"
            assert notif.get("notification_body"), "notification_body should not be empty"
            
            print("\n[✓] Response structure validated successfully!")
            print(f"\nNotification Title: {notif.get('notification_title')}")
            print(f"Notification Body: {notif.get('notification_body')}")
            print(f"Video Title: {notif.get('video_title')}")
            print(f"Creator: {notif.get('creator_name')}")
            print(f"Deep Link: {notif.get('deep_link')}")
            print(f"\nTest Log File: {data.get('test_file_path')}")
            
            return True
            
        else:
            print(f"[✗] ERROR! Status {response.status_code}")
            print(f"\nResponse:\n{response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"[✗] ERROR: Cannot connect to {API_BASE}")
        print("Make sure the API is running: .venv\\Scripts\\python.exe -m uvicorn api:app --reload")
        return False
    except Exception as e:
        print(f"[✗] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_invalid_indicator():
    """Test the endpoint with invalid indicator."""
    
    payload = {
        "user_id": 953,
        "user_name": "Shashank",
        "weak_indicator": "invalid_indicator_xyz",
        "campaign_day": 2,
    }
    
    print("\n" + "=" * 80)
    print("TEST 2: Invalid Indicator")
    print("=" * 80)
    
    try:
        response = requests.post(ENDPOINT, json=payload, timeout=30)
        
        if response.status_code == 422:
            print(f"[✓] Correctly rejected invalid indicator")
            print(f"Error: {response.json().get('detail')}")
            return True
        else:
            print(f"[✗] Unexpected status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[✗] ERROR: {str(e)}")
        return False


def test_invalid_user_id():
    """Test the endpoint with invalid user_id."""
    
    payload = {
        "user_id": -999,
        "user_name": "Shashank",
        "weak_indicator": "potential_channel_partners_identified",
        "campaign_day": 2,
    }
    
    print("\n" + "=" * 80)
    print("TEST 3: Invalid User ID")
    print("=" * 80)
    
    try:
        response = requests.post(ENDPOINT, json=payload, timeout=30)
        
        if response.status_code == 422:
            print(f"[✓] Correctly rejected negative user_id")
            print(f"Error: {response.json().get('detail')}")
            return True
        else:
            print(f"[✗] Unexpected status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"[✗] ERROR: {str(e)}")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("CLAN NOTIFICATION SEND ENDPOINT TESTS")
    print("=" * 80)
    
    results = []
    results.append(("Main Test", test_send_notification_endpoint()))
    results.append(("Invalid Indicator", test_invalid_indicator()))
    results.append(("Invalid User ID", test_invalid_user_id()))
    
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(r for _, r in results)
    print("\n" + ("✓ ALL TESTS PASSED!" if all_passed else "✗ SOME TESTS FAILED"))
    print("=" * 80)
