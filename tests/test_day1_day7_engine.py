import unittest

from notifications.engine import NotificationEngine
from notifications.models import NotificationRequest


class DayNotificationEngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = NotificationEngine()

    def _req(self, day: int) -> NotificationRequest:
        return NotificationRequest(
            user_id=1020,
            user_name="Test User",
            role="RM",
            region="north",
            language="hi",
            campaign_day=day,
            weak_indicator="customer_generation",
            video_id="294",
            video_title="Test Video",
            creator_name="Top RM",
        )

    def test_day_1_to_7_have_title_body_and_enum(self):
        expected = {
            1: "PUSH_VIDEO_FOR_USER",
            2: "PUSH_VIDEO_FOR_USER",
            3: "JOURNEY_HOME",
            4: "PUSH_VIDEO_FOR_USER",
            5: "FILL_DARTS",
            6: "LEADING_BETA_OF_THE_DAY",
            7: "JOURNEY_SET",
        }

        for day, enum_value in expected.items():
            with self.subTest(day=day):
                result = self.engine.generate(self._req(day))
                self.assertTrue(result.get("title"))
                self.assertTrue(result.get("body"))
                self.assertEqual(result.get("notification_type"), enum_value)

    def test_video_days_have_deep_link(self):
        for day in (1, 2, 4):
            with self.subTest(day=day):
                result = self.engine.generate(self._req(day))
                self.assertIn("/watch/294", result.get("deep_link", ""))


if __name__ == "__main__":
    unittest.main()
