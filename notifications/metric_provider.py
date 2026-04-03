import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine, URL
from .database_config import PostgresConfig

load_dotenv()

class PostgresMetricProvider:
    """Provides performance metrics from PostgreSQL for notifications."""

    def __init__(self, account_id: int = 14):
        self.account_id = account_id
        self.config = PostgresConfig(account_id=account_id)
        # Fetch dynamic mapping from database (with name-matching fallback)
        self.indicator_activity_mapping = self.config.get_activity_mapping()
        
        self.conn_params = {
            "host": os.getenv("PG_HOST"),
            "port": int(os.getenv("PG_PORT", 5432)),
            "dbname": os.getenv("PG_DATABASE"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "connect_timeout": 10,
        }
        self._engine: Engine | None = None

    def _get_engine(self) -> Engine:
        if self._engine is not None:
            return self._engine

        url = URL.create(
            drivername="postgresql+psycopg2",
            username=self.conn_params["user"],
            password=self.conn_params["password"],
            host=self.conn_params["host"],
            port=self.conn_params["port"],
            database=self.conn_params["dbname"],
        )
        self._engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": self.conn_params["connect_timeout"]},
        )
        return self._engine

    def fetch_user_metrics(self, user_id: str | int, indicator: Optional[str] = None) -> Dict[str, Any]:
        """Fetches all metrics for a given user, optionally filtered by lead indicator."""
        # Convert user_id if needed (system uses strings for user_ids sometimes)
        try:
            uid = int(str(user_id).replace("RM_", "").replace("user_", ""))
        except ValueError:
            return {}

        metrics = {}
        # Get target activity types for filtering from dynamic mapping
        target_types = self.indicator_activity_mapping.get(indicator, None) if indicator else None
        
        # Keep one dynamic query fragment for optional activity type filtering.
        type_filter_sql = ""
        if target_types:
            type_filter_sql = " AND activity_type IN :target_types"

        with self._get_engine().connect() as conn:
            params = {"uid": uid}
            if target_types:
                params["target_types"] = list(target_types)

            # 1. yesterday_count (FILTERED BY TYPE)
            yesterday_stmt = text(
                f"""
                SELECT COUNT(*)
                FROM ldms_activity_log
                WHERE user_id = :uid
                  AND DATE(activity_timestamp) = CURRENT_DATE - 1{type_filter_sql}
                """
            )
            if target_types:
                yesterday_stmt = yesterday_stmt.bindparams(bindparam("target_types", expanding=True))
            metrics["yesterday_count"] = conn.execute(yesterday_stmt, params).scalar_one()

            # 2. last_7_days_activity_count (FILTERED BY TYPE)
            last_7_stmt = text(
                f"""
                SELECT DATE(activity_timestamp) as d, COUNT(*)
                FROM ldms_activity_log
                WHERE user_id = :uid
                  AND activity_timestamp >= CURRENT_DATE - 7{type_filter_sql}
                GROUP BY d
                ORDER BY d ASC
                """
            )
            if target_types:
                last_7_stmt = last_7_stmt.bindparams(bindparam("target_types", expanding=True))
            rows = conn.execute(last_7_stmt, params).all()

            counts_map = {row[0]: row[1] for row in rows}
            today = datetime.now().date()
            metrics["last_7_days_activity_count"] = [
                counts_map.get(today - timedelta(days=i), 0) for i in range(7, 0, -1)
            ]

            # 3. user_streak (Only up to yesterday)
            streak_stmt = text(
                """
                WITH daily_activity AS (
                    SELECT DATE(activity_timestamp) as d
                    FROM ldms_activity_log
                    WHERE user_id = :uid
                    GROUP BY d
                    ORDER BY d DESC
                )
                SELECT d FROM daily_activity
                """
            )
            active_dates = [row[0] for row in conn.execute(streak_stmt, {"uid": uid}).all()]
            streak = 0
            check_date = today - timedelta(days=1)
            for d in active_dates:
                if d == check_date:
                    streak += 1
                    check_date -= timedelta(days=1)
                elif d < check_date:
                    break
            metrics["user_streak"] = streak

            # 4. Team Context (branch and zone)
            user_info_stmt = text("""SELECT branch, zone FROM "user" WHERE id = :uid""")
            user_info = conn.execute(user_info_stmt, {"uid": uid}).first()
            if user_info:
                branch, zone = user_info

                team_total_stmt = text("""SELECT COUNT(*) FROM "user" WHERE branch = :branch""")
                metrics["team_total_members"] = conn.execute(team_total_stmt, {"branch": branch}).scalar_one()

                team_login_stmt = text(
                    """
                    SELECT COUNT(DISTINCT ld.user_id)
                    FROM user_logs ld
                    JOIN "user" u ON ld.user_id = u.id
                    WHERE u.branch = :branch AND DATE(ld.created_at) = CURRENT_DATE
                    """
                )
                metrics["team_logged_in_today"] = conn.execute(
                    team_login_stmt, {"branch": branch}
                ).scalar_one() or 0

                team_avg_stmt = text(
                    """
                    SELECT COUNT(*)::float / (30 * NULLIF((SELECT COUNT(*) FROM "user" WHERE branch = :branch), 0))
                    FROM ldms_activity_log al
                    JOIN "user" u ON al.user_id = u.id
                    WHERE u.branch = :branch AND al.activity_timestamp >= CURRENT_DATE - 30
                    """
                )
                metrics["team_average_activity"] = conn.execute(
                    team_avg_stmt, {"branch": branch}
                ).scalar_one() or 0.0

                region_total_stmt = text("""SELECT COUNT(*) FROM "user" WHERE zone = :zone""")
                metrics["total_users_in_region"] = conn.execute(
                    region_total_stmt, {"zone": zone}
                ).scalar_one()

            # 5. Weekly Ranks
            rank_stmt = text(
                """
                SELECT value
                FROM user_metrics
                WHERE user_id = :uid AND metric = 'rank' AND month >= CURRENT_DATE - 14
                ORDER BY month DESC
                LIMIT 2
                """
            )
            rank_rows = conn.execute(rank_stmt, {"uid": uid}).all()
            metrics["current_rank"] = rank_rows[0][0] if len(rank_rows) > 0 else None
            metrics["previous_rank"] = rank_rows[1][0] if len(rank_rows) > 1 else None

            # 6. this_week_activities
            weekly_stmt = text(
                """
                SELECT activity_type, COUNT(*)
                FROM ldms_activity_log
                WHERE user_id = :uid AND activity_timestamp >= date_trunc('week', current_date)
                GROUP BY activity_type
                """
            )
            metrics["this_week_activities"] = {
                row[0]: row[1] for row in conn.execute(weekly_stmt, {"uid": uid}).all()
            }

        return metrics
