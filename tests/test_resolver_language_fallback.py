import unittest
from unittest.mock import patch

from notifications.models import NotificationResponse
import notificationschema.resolver as resolver_mod


class DummyNotificationService:
    captured_req = None

    def build_notification(self, req):
        DummyNotificationService.captured_req = req
        return NotificationResponse(
            campaign_day=2,
            notification_title="title",
            notification_body="body",
            audience_strategy="strategy",
            cohort_key="cohort",
            action="Open video",
            deep_link="https://app.clan.video/watch/1",
            notification_type="PUSH_VIDEO_FOR_USER",
            should_send=True,
            video_id="1",
            video_title="Video",
            creator_name="Creator",
        )


class ResolverLanguageFallbackTests(unittest.TestCase):
    def test_send_notifications_defaults_to_hi_when_language_missing(self):
        with patch.object(resolver_mod, "NotificationService", DummyNotificationService):
            resolver = resolver_mod.NotificationResolver()

        with patch.object(
            resolver_mod.NotificationResolver,
            "_get_user_by_id",
            return_value={
                "id": 1,
                "name": "User",
                "branch": "north",
                "zone": "north",
                "language_code": None,
            },
        ), patch.object(
            resolver_mod.NotificationResolver,
            "_save_to_test_log",
            return_value="tmp.json",
        ):
            result = resolver.send_notifications(
                user_id=1,
                user_name="User",
                weak_indicator="customer_generation",
                campaign_day=2,
            )

        self.assertTrue(result["success"])
        self.assertEqual(DummyNotificationService.captured_req.language, "hi")
        self.assertEqual(DummyNotificationService.captured_req.user_language, "hi")


if __name__ == "__main__":
    unittest.main()
