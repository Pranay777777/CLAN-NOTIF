"""Debug script to check VideoSelector issues."""

import logging
from notifications.video_selector import VideoSelector
from constants import is_excluded_video, normalize_language

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# THING #1: Check if videos load from Qdrant
# ─────────────────────────────────────────────────────────
print("=" * 80)
print("THING #1: Are videos loading from Qdrant?")
print("=" * 80)

selector = VideoSelector()
videos = selector._load_from_qdrant()

print(f"\n[OK] Total videos loaded from Qdrant: {len(videos)}")

if videos:
    print(f"\nFirst 3 videos:")
    for i, v in enumerate(videos[:3]):
        print(f"\n  Video {i+1}:")
        print(f"    video_id: {v.get('video_id')}")
        print(f"    Title: {v.get('Title')}")
        print(f"    creator_name: {v.get('creator_name')}")
        print(f"    language_name: {v.get('language_name')}")
else:
    print("\n✗ NO VIDEOS LOADED!")


# ─────────────────────────────────────────────────────────
# THING #2: Check language filtering
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("THING #2: Language filtering logic")
print("=" * 80)

# Test with user language = "hi" (Hindi)
test_language = "hi"
normalized = normalize_language(test_language)
print(f"\nUser language: '{test_language}' → normalized: '{normalized}'")

filtered = selector._match_language(videos, test_language)
print(f"Videos after language filter for '{test_language}': {len(filtered)} out of {len(videos)}")

if len(filtered) < len(videos):
    print(f"\nLanguages in Qdrant videos:")
    langs = set()
    for v in videos:
        lang = v.get('language_name', 'UNKNOWN')
        langs.add(lang)
    print(f"  {sorted(langs)}")

if filtered and len(filtered) <= 5:
    print(f"\nFiltered videos:")
    for v in filtered[:5]:
        print(f"  - {v.get('Title')} (lang: {v.get('language_name')})")


# ─────────────────────────────────────────────────────────
# THING #3: Check excluded video titles
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("THING #3: Excluded video titles filter")
print("=" * 80)

from constants import EXCLUDED_VIDEO_TITLES
print(f"\nExcluded title patterns: {EXCLUDED_VIDEO_TITLES}")

excluded_count = 0
excluded_examples = []
for v in videos:
    title = v.get('Title', '')
    if is_excluded_video(title):
        excluded_count += 1
        if len(excluded_examples) < 5:
            excluded_examples.append(title)

print(f"\nVideos excluded by title filter: {excluded_count} out of {len(videos)}")

if excluded_examples:
    print(f"\nExamples of excluded videos:")
    for title in excluded_examples:
        print(f"  [EXCLUDED] {title}")


# ─────────────────────────────────────────────────────────
# FINAL: Test select_for_day1()
# ─────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("FINAL TEST: select_for_day1() with user language 'hi'")
print("=" * 80)

selected = selector.select_for_day1(
    user_region="Eluru",
    language="hi",
    cohort_key="day1:eluru:hi:2026-04-04"
)

print(f"\nSelected video:")
if selected:
    print(f"  [OK] video_id: {selected.get('video_id')}")
    print(f"  [OK] Title: {selected.get('Title')}")
    print(f"  [OK] creator_name: {selected.get('creator_name')}")
    print(f"  [OK] language_name: {selected.get('language_name')}")
else:
    print(f"  [FAIL] NONE - Video selection FAILED!")
