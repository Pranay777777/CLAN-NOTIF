from collections import OrderedDict
from datetime import datetime
from typing import Dict, Optional

from .engine import NotificationEngine
from .models import BatchNotificationResponse, NotificationRequest, NotificationResponse
from .video_selector import VideoSelector
from .metric_provider import PostgresMetricProvider


class NotificationService:
    """Campaign service for day-based notifications.

    Video selection is intentionally kept separate for now.
    This service only builds final user-facing copy + campaign metadata.
    """

    def __init__(self):
        self.engine = NotificationEngine()
        self.selector = VideoSelector()
        self.metrics = PostgresMetricProvider()
        self._cohort_video_cache: dict[str, dict] = {}
        self._idempotency_cache: OrderedDict[str, NotificationResponse] = OrderedDict()
        self._idempotency_cache_limit = 5000

    def refresh_catalog(self):
        self.selector.refresh()
        self._cohort_video_cache = {}

    @staticmethod
    def _norm(v: str) -> str:
        return str(v or "").strip().lower()

    def _build_idempotency_key(self, req: NotificationRequest) -> str:
        date_token = datetime.utcnow().strftime("%Y-%m-%d")
        pieces = [
            date_token,
            str(req.user_id),
            self._norm(req.user_name),
            str(req.campaign_day),
            self._norm(req.region),
            self._norm(req.language),
            self._norm(req.video_id or ""),
            self._norm(req.video_title or ""),
            self._norm(req.creator_name or ""),
            self._norm(req.creator_region or ""),
            self._norm(req.creator_team or ""),
            self._norm(req.outcome_hint or ""),
            self._norm(req.weak_indicator or "") if req.campaign_day == 2 else "",
        ]
        return "|".join(pieces)

    def _build_cohort_key(self, req: NotificationRequest) -> str:
        date_token = datetime.utcnow().strftime("%Y-%m-%d")
        if req.campaign_day == 2:
            return (
                f"day2:{self._norm(req.region)}:{self._norm(req.language)}:"
                f"{self._norm(req.weak_indicator or '')}:{date_token}"
            )
        if req.campaign_day in (1, 4):
            return f"day{req.campaign_day}:{self._norm(req.region)}:{self._norm(req.language)}:{date_token}"
        if req.campaign_day in (3, 5, 6, 7, 10, 11, 16):
            return f"day{req.campaign_day}:user:{req.user_id}:{date_token}"
        return f"day12:user:{req.user_id}:{date_token}"

    def _audience_strategy(self, req: NotificationRequest) -> str:
        if req.campaign_day == 1:
            return "welcome_star_video_same_region_language"
        if req.campaign_day in (2, 4):
            return "same_video_for_region_language"
        if req.campaign_day == 3:
            return "performance_snapshot_prompt"
        if req.campaign_day == 5:
            return "personalised_daily_challenge"
        if req.campaign_day == 6:
            return "team_social_proof"
        if req.campaign_day == 7:
            return "seven_day_milestone_no_action"
        if req.campaign_day == 10:
            return "personalised_top_performer_challenge"
        if req.campaign_day == 11:
            return "leaderboard_movement"
        if req.campaign_day == 16:
            return "performance_insight"
        return "personalized_same_team_or_region"

    def _get_cached_idempotency(self, key: str) -> Optional[NotificationResponse]:
        cached = self._idempotency_cache.get(key)
        if cached is not None:
            self._idempotency_cache.move_to_end(key)
        return cached

    def _set_cached_idempotency(self, key: str, value: NotificationResponse) -> None:
        self._idempotency_cache[key] = value
        self._idempotency_cache.move_to_end(key)
        while len(self._idempotency_cache) > self._idempotency_cache_limit:
            self._idempotency_cache.popitem(last=False)

    def build_notification(self, req: NotificationRequest) -> NotificationResponse:
        # Idempotency cache only applies to cohort-level days (1, 2, 4)
        # Personalized days (5, 6, 10, 11, 16) must recompute every call
        use_cache = req.campaign_day in (1, 2, 4)
        idempotency_key = self._build_idempotency_key(req)
        if use_cache:
            cached = self._get_cached_idempotency(idempotency_key)
            if cached is not None:
                return cached

        cohort_key = self._build_cohort_key(req)

        # 1. Fetch performance metrics from DB if missing and needed (Days 5, 6, 10, 11, 16)
        if req.campaign_day in (5, 6, 10, 11, 16):
            # Check if critical metrics are missing
            if req.yesterday_count is None or not req.last_7_days_activity_count:
                db_metrics = self.metrics.fetch_user_metrics(req.user_id, indicator=req.weak_indicator)
                # Update request with DB metrics only if they are not already provided
                updates = {k: v for k, v in db_metrics.items() if getattr(req, k, None) in (None, [], {})}
                if updates:
                    req = req.model_copy(update=updates)

        # 2. If caller has not provided a video...
        selected = None
        if not req.video_id:
            if req.campaign_day in (1, 2, 3, 4, 10):
                if req.campaign_day in (1, 2, 4) and cohort_key in self._cohort_video_cache:
                    selected = self._cohort_video_cache[cohort_key]
                else:
                    # Fallback between language and user_language
                    effective_language = req.user_language or req.language

                    # User ID must be passed as int if possible, or 0 if string like "RM_204"
                    try:
                        uid_int = int(str(req.user_id).replace("RM_", ""))
                    except ValueError:
                        uid_int = 0

                    if req.campaign_day == 1:
                        selected = self.selector.select_for_day1(
                            user_region=req.region,
                            language=effective_language,
                            cohort_key=cohort_key,
                        )
                    elif req.campaign_day == 2:
                        selected = self.selector.select_for_day2(
                            user_branch=req.branch or req.region,
                            language=effective_language,
                            weak_indicator=req.weak_indicator or "",
                            cohort_key=cohort_key,
                            user_id=uid_int,
                        )
                    elif req.campaign_day == 3:
                        selected = self.selector.select_for_day3(
                            user_branch=req.branch or req.region,
                            language=effective_language,
                            cohort_key=cohort_key,
                            user_id=uid_int,
                        )
                    elif req.campaign_day == 10:
                        selected = self.selector.select_for_day10(
                            weak_indicator=req.weak_indicator or "",
                            language=effective_language
                        )
                    else:
                        selected = self.selector.select_for_campaign(
                            campaign_day=req.campaign_day or 2,
                            user_region=req.region,
                            language=effective_language,
                            cohort_key=cohort_key,
                            user_id=uid_int,
                        )
                        if selected and req.campaign_day in (1, 2, 4):
                            self._cohort_video_cache[cohort_key] = selected

        selected_video_id = req.video_id or (str(selected.get("video_id")) if selected else None)
        selected_video_title = req.video_title or (str(selected.get("Title")) if selected else None)
        selected_creator_name = req.creator_name or (str(selected.get("creator_name")) if selected else None)
        selected_creator_region = req.creator_region or (str(selected.get("creator_region")) if selected else None)

        resolved_req = req.model_copy(
            update={
                "video_id": selected_video_id,
                "video_title": selected_video_title,
                "creator_name": selected_creator_name,
                "creator_region": selected_creator_region,
            }
        )

        copy = self.engine.generate(resolved_req)
            
        response = NotificationResponse(
            campaign_day=resolved_req.campaign_day,
            notification_title=copy.get("title", ""),
            notification_body=copy.get("body", ""),
            action=copy.get("action", ""),
            notification_type=copy.get("notification_type", "JOURNEY"),
            should_send=copy.get("should_send", True),
            deep_link=copy.get("deep_link", None),
            audience_strategy=self._audience_strategy(resolved_req),
            cohort_key=cohort_key,
            video_id=resolved_req.video_id,
            video_title=resolved_req.video_title,
            creator_name=resolved_req.creator_name,
        )
        if use_cache:
            self._set_cached_idempotency(idempotency_key, response)
        return response

    def build_notifications_batch(self, items: list[NotificationRequest]) -> BatchNotificationResponse:
        results = [self.build_notification(item) for item in items]
        return BatchNotificationResponse(total=len(results), results=results)
