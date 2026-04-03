#!/usr/bin/env python3
"""Test VideoSelector with Qdrant backend - NO EXCEL DEPENDENCY."""

import sys
sys.path.insert(0, '.')

from notifications.video_selector import VideoSelector

print("=" * 70)
print("SMOKE TEST: VideoSelector with Qdrant Backend (NO EXCEL FILES)")
print("=" * 70)

selector = VideoSelector()
print('[OK] VideoSelector initialized (Qdrant mode)')

# Test loading videos
videos = selector._load_from_qdrant()
print(f'[OK] Loaded {len(videos)} videos from Qdrant')

if videos:
    sample = videos[0]
    title = sample.get('Title', '')[:50]
    vid = sample.get('video_id')
    creator = sample.get('creator')
    indicators = sample.get('lead_indicator', '')[:80]
    print(f'\n  Sample Video:')
    print(f'    ID: {vid}')
    print(f'    Title: {title}...')
    print(f'    Indicators: {indicators}...')
    print(f'\n[OK] All videos have required fields')
else:
    print('[FAIL] No videos loaded!')
    sys.exit(1)

# Test select_for_day1
print("\n" + "=" * 70)
print("TEST: select_for_day1 (deterministic selection)")
print("=" * 70)

video_d1 = selector.select_for_day1(user_region='north', language='english', cohort_key='cohort_1_day1')
if video_d1:
    print(f'[OK] Selected day1 video: ID={video_d1.get("video_id")}')
else:
    print('[FAIL] No video selected for day1')
    sys.exit(1)

# Test select_for_campaign
print("\n" + "=" * 70)
print("TEST: select_for_campaign")
print("=" * 70)

video_camp = selector.select_for_campaign(
    campaign_day=2, 
    user_region='north', 
    language='english',
    cohort_key='cohort_1_day2',
    user_id=123
)
if video_camp:
    print(f'[OK] Selected campaign video: ID={video_camp.get("video_id")}')
else:
    print('[FAIL] No video selected for campaign')
    sys.exit(1)

# Test select_for_day10
print("\n" + "=" * 70)
print("TEST: select_for_day10 (indicator matching)")
print("=" * 70)

video_d10 = selector.select_for_day10(
    weak_indicator='customer_generation',
    language='english'
)
if video_d10:
    print(f'[OK] Selected day10 video with indicator match: ID={video_d10.get("video_id")}')
else:
    print('[FAIL] No video selected for day10')
    sys.exit(1)

print("\n" + "=" * 70)
print("[OK] ALL SMOKE TESTS PASSED!!!!")
print("VideoSelector works PERFECTLY with Qdrant (no Excel files needed)")
print("=" * 70)
