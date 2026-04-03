"""
Unified Notification Schema Package

Provides single, clean entry point for:
1. Building notifications in real-time for users
2. Batch processing campaigns
3. Managing notification request/response lifecycle
4. Day-specific notification sending (Day 2, etc.)

## Quick Start

### Single User (Real-Time)
```python
from notificationschema.schema import BuildNotificationRequest
from notificationschema.resolver import resolve_notification

request = BuildNotificationRequest(
    user_id=953,
    user_name="Shashank",
    role="RM",
    region="south",
    language="hi",
    journey_day=10,
)

response = resolve_notification(request)
print(response.notification_title)
print(response.notification_body)
print(response.video.video_id)
```

### Send Day 2 Notification (Test Mode)
```python
from notificationschema.resolver import get_resolver

resolver = get_resolver()

response = resolver.send_notifications(
    user_id=953,
    user_name='Shashank',
    weak_indicator='customer_generation',
    watched_video_ids=[100, 200],
    campaign_day=2
)

print(response['notification']['notification_title'])
# Output: "Today's 2-minute tip from a top performer near you"

# Check test log
cat ./test_notifications_log.json
```

### Batch Campaign
```python
from notificationschema.schema import BatchBuildNotificationRequest
from notificationschema.resolver import resolve_notifications_batch

batch = BatchBuildNotificationRequest(
    items=[
        BuildNotificationRequest(...),
        BuildNotificationRequest(...),
    ]
)

response = resolve_notifications_batch(batch)
print(f"Sent: {response.successful}/{response.total}")
```

## Architecture

### Data Flow (Real-Time User)
```
User Opens App
    ↓
Frontend sends: user_id, name, role, region, language
    ↓
[API Route] /notifications/build
    ↓
[Resolver.resolve_single]
    1. Enrich user context from PostgreSQL
    2. Fetch weak indicator (KII query)
    3. Recommend video from Qdrant
    4. Generate day-specific copy
    5. Format response
    ↓
Response: title, body, video_id, action
    ↓
Frontend shows notification, user clicks → video plays
```

### Data Flow (Day 2 Notification Test)
```
Call resolver.send_notifications(user_id=953, ...)
    ↓
STEP 1: Get user from DB
STEP 2: Recommend video from Qdrant
STEP 3: Generate Day 2 copy
STEP 4: Build notification object with all fields
STEP 5: Save to test_notifications_log.json
    ↓
Log saved for manual testing
    ↓
Login as user 953 in app
    ↓
Manually check notification in app
```

### Folder Structure
```
notificationschema/
├── __init__.py          # This file
├── schema.py            # Request/Response Pydantic models
├── resolver.py          # Core business logic + DB queries + Day 2 sending
```

### Key Components

**schema.py:**
- `BuildNotificationRequest`: Validated input schema
- `BuildNotificationResponse`: Output with title, body, video
- `BatchBuildNotificationRequest`: Multi-user request
- `BatchBuildNotificationResponse`: Multi-user response

**resolver.py (Main Worker):**
- `NotificationResolver`: Main worker class
- `resolve_notification(request)`: Single-user shortcut
- `resolve_notifications_batch(batch)`: Batch shortcut
- `resolver.send_notifications(user_id, ...)`: Day 2 notification sender
- Helper functions: `_get_user_by_id()`, `_recommend_video_for_user()`, 
  `_generate_day2_copy()`, `_build_notification_object()`, `_save_to_test_log()`

### Supported Campaign Days
- Day 1-7: Daily onboarding sequence
- Day 10-12: Mid-campaign engagement  
- Day 16: Final push
- Day 2: "Today's 2-minute tip" (social proof)

Each day has custom copy generation based on user metrics and weak indicator.

## Real-Time Workflow for Users

### What Happens When User Opens App

1. **Frontend collects user context:**
   - user_id, name, role (from login session)
   - region, language (from app settings)

2. **Backend enriches with DB data:**
   - journey_day computed from profile_activation_date
   - weak_indicator fetched from kii_master (user's lowest weekly score)
   - role details from jobrole table
   - Optionally: metrics from daily_activity, team_performance

3. **Resolver processes request:**
   - Validates all inputs (Pydantic schema)
   - Connects to Qdrant to find best video
   - Calls NotificationEngine for day-specific copy
   - Returns notification ready to display

4. **Frontend displays notification:**
   - Title + Body as in-app banner
   - Video metadata ready for deep link
   - User clicks → opens video player

5. **Backend tracks:**
   - Notification sent (timestamp, content_id, user_id)
   - Video watched (if user clicks)
   - Engagement metrics for future recommendations

## Day 2 Notification Testing

### Build Notification (No FCM Send)
```python
resolver = get_resolver()
response = resolver.send_notifications(
    user_id=953,
    user_name='Shashank',
    weak_indicator='customer_generation',
    campaign_day=2
)

# Check response
if response['success']:
    print("✅ Notification built")
    print(response['notification'])
else:
    print("❌ Error:", response['error'])
```

### Test Output
```python
{
    'success': True,
    'notification': {
        'campaign_day': 2,
        'notification_title': "Today's 2-minute tip from a top performer near you",
        'notification_body': 'See the exact approach they use to close more deals in the field.',
        'audience_strategy': 'same_video_for_region_language',
        'cohort_key': 'day2_953_south_hi',
        'video_title': 'How important are marketing activities...',
        'creator_name': 'Abhishek Sahu',
        'action': 'open_video',
        'deep_link': 'https://app.clan.video/watch/300',
        'should_send': True
    },
    'user_id': 953,
    'test_file_path': './test_notifications_log.json'
}
```

### Manual Testing Workflow
1. Build notification: `response = resolver.send_notifications(user_id=953, ...)`
2. Check test log: `cat ./test_notifications_log.json`
3. Login as user 953 in app
4. Open app and verify notification appears
5. Click notification → video should play

## Edge Cases Handled

- **User not found:** Returns error dict with 'success': False
- **No weak indicator:** Auto-retrieves from DB if not provided
- **No video found:** Returns error (should not happen with diverse catalog)
- **Invalid language:** Falls back to "hi" (Hindi)
- **Invalid region:** Uses "all" for catalog matching
- **Batch failures:** Continues processing, returns errors dict
- **Test log missing:** Creates new file automatically

## Configuration

Set these environment variables:
```bash
DATABASE_URL=postgresql://...
ACCOUNT_ID=14
QDRANT_URL=http://localhost:6333
```

## Performance Notes

- Single notification: ~200-500ms (DB + Qdrant query + copy generation)
- Batch (1000 users): ~5-10 min (runs sequentially; can parallelize)
- Bottleneck: Qdrant semantic search (~100-200ms per user)
- Optimization: Cache video recommendations per weak_indicator per day

## Testing with PowerShell

```powershell
$code = @'
from notificationschema.resolver import get_resolver
import json

resolver = get_resolver()
response = resolver.send_notifications(
    user_id=953,
    user_name='Shashank',
    weak_indicator='customer_generation',
    campaign_day=2
)

print(json.dumps(response, indent=2, default=str))
'@

.\.venv\Scripts\python.exe -c $code
```

"""

from notificationschema.schema import (
    BuildNotificationRequest,
    BuildNotificationResponse,
    VideoReference,
    BatchBuildNotificationRequest,
    BatchBuildNotificationResponse,
    CampaignDay,
)
from notificationschema.resolver import (
    NotificationResolver,
    resolve_notification,
    resolve_notifications_batch,
    get_resolver,
)

__all__ = [
    # Schemas
    "BuildNotificationRequest",
    "BuildNotificationResponse",
    "VideoReference",
    "BatchBuildNotificationRequest",
    "BatchBuildNotificationResponse",
    "CampaignDay",
    # Resolvers
    "NotificationResolver",
    "resolve_notification",
    "resolve_notifications_batch",
    "get_resolver",
]
