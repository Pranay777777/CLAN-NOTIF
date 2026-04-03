import logging
import os
import argparse
from dotenv import load_dotenv
from sqlalchemy import create_engine

from user_segmentation import get_eligible_users
from weak_indicator    import get_weak_indicator
from notification_sender import already_sent, mark_sent, send_notification

# Import recommend directly — no HTTP overhead
from recommend import recommend_video

load_dotenv()
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/notifications.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("scheduler")


def _get_database_url() -> str:
    db_url = (os.getenv("DATABASE_URL", "") or "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL not set in environment/.env")
    return db_url


def run(dry_run: bool = True):
    logger.info("=" * 60)
    logger.info("NOTIFICATION SCHEDULER START | dry_run=%s", dry_run)

    engine = create_engine(_get_database_url())
    users  = get_eligible_users(engine)

    if not users:
        logger.info("No eligible users today. Done.")
        return

    sent      = 0
    skipped   = 0
    no_result = 0
    failed    = 0

    for user in users:
        user_id      = user["user_id"]
        campaign_day = user["campaign_day"]

        logger.info(
            "Processing user=%d name=%s day=%d branch=%s lang=%s",
            user_id, user["name"], campaign_day,
            user["branch"], user["language_name"]
        )

        # Step 1: get weak indicator from KII data
        weak_indicator = get_weak_indicator(engine, user_id)

        # Step 2: get recommendation
        try:
            # Build watched_ids from sent log to avoid repeats
            from notification_sender import _load_log
            sent_log   = _load_log()
            watched_ids = sent_log.get(str(user_id), [])

            recommendation_output = recommend_video(
                user_name      = user["name"],
                weak_indicator = weak_indicator,
                user_role      = user["role"],
                user_region    = user["zone"] or user["branch"],
                journey_day    = campaign_day,
                watched_ids    = watched_ids,
                months_in_role = 1,  # default — refine later
            )

            generated_notification = None
            if isinstance(recommendation_output, tuple):
                recommendation, generated_notification = recommendation_output
            else:
                recommendation = recommendation_output
        except Exception as e:
            logger.error("Recommend failed for user=%d: %s", user_id, e)
            no_result += 1
            continue

        if not recommendation:
            logger.warning("No recommendation for user=%d", user_id)
            no_result += 1
            continue

        content_id = str(recommendation.get("video_id", ""))
        title      = recommendation.get("notification_title", "Your daily video is here!")
        body       = recommendation.get("notification_body", generated_notification or "Watch today's recommended video.")

        if not content_id:
            logger.warning("Empty video_id for user=%d", user_id)
            no_result += 1
            continue

        # Step 3: dedup check
        if already_sent(user_id, content_id):
            logger.info(
                "SKIP | user=%d already sent content=%s",
                user_id, content_id
            )
            skipped += 1
            continue

        # Step 4: send
        success = send_notification(
            fcm_token  = user["fcm_token"],
            title      = title,
            body       = body,
            content_id = content_id,
            dry_run    = dry_run,
        )

        if success:
            mark_sent(user_id, content_id)
            sent += 1
            logger.info(
                "SENT | user=%d content=%s day=%d weak_kii=%s",
                user_id, content_id, campaign_day, weak_indicator
            )
        else:
            failed += 1
            logger.error("FAILED | user=%d content=%s", user_id, content_id)

    logger.info("=" * 60)
    logger.info(
        "DONE | sent=%d skipped=%d no_result=%d failed=%d",
        sent, skipped, no_result, failed
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--send",    action="store_true")
    args    = parser.parse_args()
    dry_run = not args.send
    run(dry_run=dry_run)