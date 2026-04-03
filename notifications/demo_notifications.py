from notifications.models import NotificationRequest
from notifications.service import NotificationService


def main():
    service = NotificationService()

    samples = [
        NotificationRequest(
            user_id=11,
            user_name="Nisha",
            region="Mumbai",
            language="en",
            campaign_day=1,
        ),
        NotificationRequest(
            user_id=1,
            user_name="Aarav",
            region="Mumbai",
            language="en",
            campaign_day=2,
        ),
        NotificationRequest(
            user_id=2,
            user_name="Priya",
            region="Mumbai",
            language="en",
            campaign_day=4,
        ),
        NotificationRequest(
            user_id=3,
            user_name="Aarav",
            region="Mumbai",
            language="en",
            campaign_day=12,
            creator_name="Shikha",
            creator_team="Andheri branch",
            outcome_hint="double conversion rate",
            video_id="vid_298",
            video_title="How to qualify leads quickly",
        ),
    ]

    for req in samples:
        out = service.build_notification(req)
        print("-" * 72)
        print(f"day={out.campaign_day} | cohort={out.cohort_key}")
        print(f"strategy={out.audience_strategy}")
        print(f"title: {out.notification_title}")
        print(f"body : {out.notification_body}")


if __name__ == "__main__":
    main()
