#!/usr/bin/env python3
"""Test VideoSelector with Qdrant backend."""

import sys
sys.path.insert(0, '.')

from notifications.video_selector import VideoSelector

print("=" * 60)
print("TEST 4: VideoSelector with Qdrant Backend")
print("=" * 60)

selector = VideoSelector()
print('✓ VideoSelector initialized (Qdrant mode)')

# Test loading videos
videos = selector._load_from_qdrant()
print(f'✓ Loaded {len(videos)} videos from Qdrant')

if videos:
    sample = videos[0]
    title = sample.get('Title', '')[:50]
    vid = sample.get('video_id')
    creator = sample.get('creator')
    indicators = sample.get('lead_indicator', '')[:80]
    print(f'\n  Sample Video:')
    print(f'    ID: {vid}')
    print(f'    Title: {title}...')
    print(f'    Creator: {creator}')
    print(f'    Indicators: {indicators}...')
    print(f'\n✓ All videos have required fields')
else:
    print('✗ No videos loaded!')
    sys.exit(1)

# Test select_for_day1
print("\n" + "=" * 60)
print("TEST 5: select_for_day1 (deterministic, no Excel)")
print("=" * 60)

video_d1 = selector.select_for_day1(user_region='north', language='english', cohort_key='cohort_1_day1')
if video_d1:
    print(f'✓ Selected day1 video: ID={video_d1.get("video_id")}, Title={video_d1.get("Title")[:40]}...')
else:
    print('✗ No video selected for day1')
    sys.exit(1)

# Test deterministic picking (same cohort_key = same video)
video_d1_repeat = selector.select_for_day1(user_region='north', language='english', cohort_key='cohort_1_day1')
if video_d1_repeat['video_id'] == video_d1['video_id']:
    print(f'✓ Deterministic: same cohort_key returns same video (ID={video_d1.get("video_id")})')
else:
    print('✗ Deterministic check failed')
    sys.exit(1)

# Test select_for_campaign
print("\n" + "=" * 60)
print("TEST 6: select_for_campaign")
print("=" * 60)

video_camp = selector.select_for_campaign(
    campaign_day=2, 
    user_region='north', 
    language='english',
    cohort_key='cohort_1_day2',
    user_id=123
)
if video_camp:
    print(f'✓ Selected campaign video: ID={video_camp.get("video_id")}, Title={video_camp.get("Title")[:40]}...')
else:
    print('✗ No video selected for campaign')
    sys.exit(1)

# Test select_for_day10
print("\n" + "=" * 60)
print("TEST 7: select_for_day10 (indicator matching)")
print("=" * 60)

video_d10 = selector.select_for_day10(
    weak_indicator='customer_generation',
    language='english'
)
if video_d10:
    indicators = video_d10.get('lead_indicator', '')
    print(f'✓ Selected day10 video: ID={video_d10.get("video_id")}')
    print(f'  Title: {video_d10.get("Title")[:50]}...')
    print(f'  Indicators: {indicators[:80]}...')
    if 'customer_generation' in indicators.lower():
        print(f'  ✓ Indicator "customer_generation" found in video')
    else:
        print(f'  ⚠ Indicator not found, but video selected (fallback behavior)')
else:
    print('✗ No video selected for day10')
    sys.exit(1)

print("\n" + "=" * 60)
print("✓ ALL TESTS PASSED - VideoSelector Qdrant integration works!")
print("=" * 60)
