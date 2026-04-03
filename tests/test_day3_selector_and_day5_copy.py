import unittest

from notifications.engine import NotificationEngine
from notifications.models import NotificationRequest
from notifications.video_selector import VideoSelector


class Day3Day5BehaviorTests(unittest.TestCase):
    def test_day3_prefers_branch_rm_video(self):
        selector = VideoSelector()
        selector._videos = [
            {"video_id": 1, "Title": "Lead Closing Basics", "creator_region": "north", "creator_role": "RM", "language_name": "hindi", "summary": "", "key_lesson": "", "problem_solved": ""},
            {"video_id": 2, "Title": "Closing Faster", "creator_region": "south", "creator_role": "BM", "language_name": "hindi", "summary": "", "key_lesson": "", "problem_solved": ""},
        ]

        chosen = selector.select_for_day3(
            user_branch="north",
            language="hindi",
            cohort_key="day3:north:hindi:2026-04-02",
            user_id=1020,
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.get("video_id"), 1)

    def test_day5_uses_indicator_label_not_generic_activities(self):
        engine = NotificationEngine()
        req = NotificationRequest(
            user_id=1020,
            user_name="Test User",
            role="RM",
            branch="north",
            region="north",
            language="hi",
            campaign_day=5,
            weak_indicator="customer_generation",
            yesterday_count=2,
            last_7_days_activity_count=[1, 2, 2, 3, 2, 2, 2],
            team_average_activity=3,
        )
        out = engine.generate_day5(req)
        body = str(out.get("body", "")).lower()
        self.assertIn("customers generated", body)
        self.assertNotIn("activities", body)


if __name__ == "__main__":
    unittest.main()
