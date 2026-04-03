import logging
import os
from datetime import date
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("segmentation")

# Campaign day window configuration
CAMPAIGN_DAY_MIN = int(os.getenv("CAMPAIGN_DAY_MIN", "1"))
CAMPAIGN_DAY_MAX = int(os.getenv("CAMPAIGN_DAY_MAX", "7"))

# Database URL configuration
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in environment/.env")

logger.info(
    "Campaign day window configured: %d to %d",
    CAMPAIGN_DAY_MIN,
    CAMPAIGN_DAY_MAX,
)


def get_eligible_users(db_engine) -> list[dict]:
    """
    Fetches all active users who:
    - Have profile activated
    - Have an FCM token
    - Are on campaign day (CAMPAIGN_DAY_MIN to CAMPAIGN_DAY_MAX)
    Returns full context needed for recommend endpoint.
    """
    today = date.today()

    query = text("""
        SELECT
            u.id                        AS user_id,
            u.name,
            u.branch,
            u.zone,
            u.app_language_id           AS language_id,
            u.profile_activation_date,
            u.designation,
            ml.language_code            AS language_name,
            ud.fcm_token,
            (
                CAST(:today AS date)
                - u.profile_activation_date::date
                + 1
            )                           AS campaign_day
        FROM "user" u
        JOIN user_device ud
            ON ud.user_id = u.id
            AND ud.status = 1
            AND ud.fcm_token IS NOT NULL
            AND ud.fcm_token != ''
        LEFT JOIN md_app_languages ml
            ON ml.id = u.app_language_id
        WHERE
            u.status = 1
            AND u.account_id = 14
            AND u.profile_activated = true
            AND u.profile_activation_date IS NOT NULL
            AND (
                CAST(:today AS date)
                - u.profile_activation_date::date
                + 1
            ) BETWEEN :day_min AND :day_max
    """)

    with db_engine.connect() as conn:
        rows = conn.execute(
            query,
            {
                "today": today,
                "day_min": CAMPAIGN_DAY_MIN,
                "day_max": CAMPAIGN_DAY_MAX,
            },
        ).fetchall()

    users = []
    for row in rows:
        users.append({
            "user_id":           row.user_id,
            "name":              row.name or "there",
            "branch":            row.branch or "",
            "zone":              row.zone or "",
            "language_id":       row.language_id,
            "language_name":     (row.language_name or "hindi").lower(),
            "role":              "RM",
            "campaign_day":      int(row.campaign_day),
            "fcm_token":         row.fcm_token,
        })

    logger.info(
        "Fetched %d eligible users for %s", len(users), today
    )
    return users