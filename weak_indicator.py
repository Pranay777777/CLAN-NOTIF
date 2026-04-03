import logging
import re
from datetime import date, timedelta
from sqlalchemy import text

logger = logging.getLogger("weak_indicator")


def normalize_kii_name(name: str) -> str:
    """Converts 'customer generation' -> 'customer_generation'"""
    return re.sub(r'\s+', '_', name.lower().strip())


def get_weak_indicator(db_engine, user_id: int) -> str:
    """
    Finds the weakest KII for a user this week.
    Weak = lowest (actual / weekly_target) ratio.
    Falls back to 'customer_generation' if no data found.
    """
    week_start = date.today() - timedelta(days=date.today().weekday())
    week_end   = week_start + timedelta(days=6)

    query = text("""
        SELECT
            km.id           AS kii_id,
            km.kii_name,
            km.daily_target * 7  AS weekly_target,
            COALESCE(SUM(ki.kill_value), 0) AS actual_value
        FROM kii_master km
        LEFT JOIN key_input_indicators ki
            ON ki.kii_id = km.id
            AND ki.user_id = :user_id
            AND ki.kii_date BETWEEN :week_start AND :week_end
            AND ki.status = 1
                WHERE km.status = 1
                    AND km.account_id = (
                            SELECT u.account_id
                            FROM "user" u
                            WHERE u.id = :user_id
                    )
        GROUP BY km.id, km.kii_name, km.daily_target
        ORDER BY
            CASE
                WHEN km.daily_target * 7 = 0 THEN 1
                ELSE 0
            END,
            (COALESCE(SUM(ki.kill_value), 0)::float
             / NULLIF(km.daily_target * 7, 0)) ASC
    """)

    with db_engine.connect() as conn:
        rows = conn.execute(query, {
            "user_id":    user_id,
            "week_start": week_start,
            "week_end":   week_end,
        }).fetchall()

    if not rows:
        logger.warning(
            "No KII data for user=%d, defaulting to customer_generation",
            user_id
        )
        return "customer_generation"

    weakest = rows[0]
    normalized = normalize_kii_name(weakest.kii_name)

    logger.info(
        "User=%d weakest KII: %s | actual=%d | weekly_target=%d",
        user_id,
        normalized,
        int(weakest.actual_value),
        int(weakest.weekly_target),
    )
    return normalized