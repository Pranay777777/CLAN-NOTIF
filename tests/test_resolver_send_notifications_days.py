import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from notifications.models import NotificationResponse
from notificationschema.resolver import NotificationResolver


class ResolverDayFlowTests(unittest.TestCase):
    @patch.object(NotificationResolver, "_save_to_test_log")
    @patch.object(NotificationResolver, "_get_user_by_id")
    def test_send_notifications_includes_notification_type(self, mock_user, mock_save):
        mock_user.return_value = {
            "id": 1020,
            "name": "Test User",
            "branch": "branch1",
            "zone": "north",
            "language_code": "hi",
        }
        mock_save.return_value = Path("test_notifications_log.json")

        resolver = NotificationResolver()

        resolver.notification_service.build_notification = Mock(
            return_value=NotificationResponse(
                campaign_day=5,
                notification_title="Can you beat yesterday's effort?",
                notification_body="Test User, yesterday you hit 2 activities. Can you reach 3 today?",
                audience_strategy="personalised_daily_challenge",
                cohort_key="day5:user:1020:2026-04-02",
                action="Update 1 activity",
                deep_link="clan://darts/home",
                notification_type="FILL_DARTS",
                should_send=True,
                video_id="294",
                video_title="Video",
                creator_name="Top RM",
            )
        )

        out = resolver.send_notifications(
            user_id=1020,
            user_name="Test User",
            weak_indicator="customer_generation",
            watched_video_ids=[],
            campaign_day=5,
        )

        self.assertTrue(out["success"])
        self.assertEqual(out["notification"]["campaign_day"], 5)
        self.assertEqual(out["notification"]["notification_type"], "FILL_DARTS")
        self.assertEqual(out["notification"]["deep_link"], "clan://darts/home")


if __name__ == "__main__":
    unittest.main()
