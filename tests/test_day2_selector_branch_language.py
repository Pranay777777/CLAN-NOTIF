import unittest

from notifications.video_selector import VideoSelector


class Day2SelectorTests(unittest.TestCase):
    def setUp(self):
        self.selector = VideoSelector()
        self.selector._videos = [
            {"video_id": 1, "Title": "A", "creator_region": "north", "language_name": "hindi", "lead_indicator": "customer_generation", "creator_name": "Creator A"},
            {"video_id": 2, "Title": "B", "creator_region": "south", "language_name": "hindi", "lead_indicator": "customer_generation", "creator_name": "Creator B"},
            {"video_id": 3, "Title": "C", "creator_region": "north", "language_name": "english", "lead_indicator": "customer_generation", "creator_name": "Creator C"},
            {"video_id": 4, "Title": "D", "creator_region": "east", "language_name": "hindi", "lead_indicator": "marketing_activities_conducted", "creator_name": "Creator D"},
        ]

    def test_day2_prefers_branch_and_language(self):
        chosen = self.selector.select_for_day2(
            user_branch="north",
            language="hindi",
            weak_indicator="marketing_activities_conducted",
            cohort_key="day2:north:hindi:marketing_activities_conducted:2026-04-02",
            user_id=1020,
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen["creator_region"], "north")
        self.assertEqual(chosen["language_name"], "hindi")

    def test_day2_falls_back_to_weak_indicator(self):
        self.selector._videos = [
            {"video_id": 10, "Title": "X", "creator_region": "south", "language_name": "english", "lead_indicator": "customer_generation", "creator_name": "Creator X"},
            {"video_id": 11, "Title": "Y", "creator_region": "south", "language_name": "english", "lead_indicator": "marketing_activities_conducted", "creator_name": "Creator Y"},
        ]
        chosen = self.selector.select_for_day2(
            user_branch="north",
            language="hindi",
            weak_indicator="marketing_activities_conducted",
            cohort_key="day2:north:hindi:marketing_activities_conducted:2026-04-02",
            user_id=1020,
        )
        self.assertIsNotNone(chosen)
        self.assertIn("marketing_activities_conducted", chosen["lead_indicator"])


if __name__ == "__main__":
    unittest.main()
