"""
Unified Notification Resolver

This module handles the core logic for resolving notification requests:
1. Data enrichment: Fetch missing user info from database
2. Weak indicator resolution: Determine user's weakest KII
3. Video recommendation: Find best video from Qdrant
4. Copy generation: Create day-specific notification text
5. Response formatting: Return validated BuildNotificationResponse
"""

import logging
from typing import Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import os
import json
from pathlib import Path
from datetime import datetime

from constants import is_excluded_video, ACCOUNT_ID
from weak_indicator import get_weak_indicator
from recommend import recommend_video
from notifications.engine import NotificationEngine
from notifications.video_selector import VideoSelector
from notifications.service import NotificationService
from notifications.models import NotificationRequest
from notificationschema.schema import (
    BuildNotificationRequest,
    BuildNotificationResponse,
    VideoReference,
    BatchBuildNotificationRequest,
    BatchBuildNotificationResponse,
)
from database.db_config import SessionLocal

logger = logging.getLogger("notification_resolver")


class NotificationResolver:
    """
    Unified resolver for all notification operations.
    
    Handles the complete flow:
    User Request → Data Enrichment → Weak Indicator → Video Recommendation
                   → Copy Generation → Response Formatting
    """
    
    def __init__(self):
        """Initialize resolver with dependencies."""
        self.db_engine = create_engine(os.getenv("DATABASE_URL"))
        self.notification_service = NotificationService()
        self.notification_engine = NotificationEngine()
        self.video_selector = VideoSelector()
    
    def resolve_single(self, request: BuildNotificationRequest) -> BuildNotificationResponse:
        """
        Resolve a single notification request.
        
        Complete workflow for real-time users:
        1. Enrich user context from DB (if not provided)
        2. Resolve weak indicator (if not provided)
        3. Recommend best video
        4. Generate day-specific notification copy
        5. Return formatted response
        
        Args:
            request: Validated notification request
            
        Returns:
            BuildNotificationResponse with complete notification data
            
        Raises:
            ValueError: If user not found or invalid data
        """
        
        try:
            # ─────────────────────────────────────────────────────────
            # STEP 1: Enrich user context from database
            # ─────────────────────────────────────────────────────────
            with self.db_engine.connect() as conn:
                user_row = conn.execute(
                    text("""
                        SELECT 
                            u.id, u.name, u.branch, u.zone, u.account_id,
                            COALESCE(jr.name, 'unknown') AS role_name,
                            COALESCE(ml.language_code, 'hi') AS language_code,
                            u.profile_activation_date,
                            (CAST(:today AS date) - u.profile_activation_date::date + 1) AS journey_day_computed
                        FROM "user" u
                        LEFT JOIN jobrole jr ON jr.id = u.current_role_id
                        LEFT JOIN md_app_languages ml ON ml.id = u.app_language_id
                        WHERE u.id = :uid AND u.status = 1 AND u.account_id = :account_id
                    """),
                    {
                        "uid": int(request.user_id),
                        "account_id": ACCOUNT_ID,
                        "today": "2026-04-01",  # Use current date in production
                    },
                ).mappings().first()
            
            if not user_row:
                raise ValueError(f"User {request.user_id} not found or inactive")
            
            # Use computed journey_day if not provided or provided is 1
            if request.journey_day == 1 and user_row["journey_day_computed"]:
                journey_day = user_row["journey_day_computed"]
            else:
                journey_day = request.journey_day
            
            logger.info(
                "User enriched | user_id=%s | journey_day=%s | role=%s | region=%s",
                request.user_id,
                journey_day,
                request.role,
                request.region,
            )
            
            # ─────────────────────────────────────────────────────────
            # STEP 2: Resolve weak indicator (if not provided)
            # ─────────────────────────────────────────────────────────
            if not request.weak_indicator:
                weak_indicator = get_weak_indicator(self.db_engine, int(request.user_id))
                if not weak_indicator:
                    weak_indicator = "customer_generation"  # Safe default
                logger.info("Weak indicator resolved | user_id=%s | indicator=%s", request.user_id, weak_indicator)
            else:
                weak_indicator = request.weak_indicator
            
            # ─────────────────────────────────────────────────────────
            # STEP 3: Recommend best video via Qdrant
            # ─────────────────────────────────────────────────────────
            best_video, notification_context = recommend_video(
                user_name=request.user_name,
                weak_indicator=weak_indicator,
                user_role=request.role,
                user_region=request.region,
                journey_day=journey_day,
                months_in_role=request.months_in_role,
                watched_ids=request.watched_video_ids,
            )
            
            if not best_video:
                raise ValueError(f"No suitable video found for user {request.user_id}")
            
            video_id = str(best_video.get("video_id", ""))
            video_title = str(best_video.get("title", ""))
            
            logger.info(
                "Video recommended | user_id=%s | video_id=%s | score=%.2f",
                request.user_id,
                video_id,
                best_video.get("score", 0.0),
            )
            
            # ─────────────────────────────────────────────────────────
            # STEP 4: Generate day-specific notification copy
            # ─────────────────────────────────────────────────────────
            campaign_day = request.campaign_day or 2
            
            # Determine audience strategy based on campaign day and weak indicator
            audience_strategy = self._determine_audience_strategy(campaign_day, weak_indicator)
            
            # Generate copy using NotificationEngine
            title, body, action = self._generate_notification_copy(
                campaign_day=campaign_day,
                user_name=request.user_name,
                weak_indicator=weak_indicator,
                audience_strategy=audience_strategy,
                video_title=video_title,
                metrics=self._extract_metrics(request),
            )
            
            # Create deterministic cohort key for reproducibility
            cohort_key = self._generate_cohort_key(
                user_id=request.user_id,
                campaign_day=campaign_day,
                region=request.region,
                language=request.language,
            )
            
            logger.info(
                "Notification copy generated | user_id=%s | day=%s | strategy=%s | cohort=%s",
                request.user_id,
                campaign_day,
                audience_strategy,
                cohort_key,
            )
            
            # ─────────────────────────────────────────────────────────
            # STEP 5: Format and return response
            # ─────────────────────────────────────────────────────────
            response = BuildNotificationResponse(
                success=True,
                notification_title=title,
                notification_body=body,
                campaign_day=campaign_day,
                audience_strategy=audience_strategy,
                cohort_key=cohort_key,
                video=VideoReference(
                    video_id=video_id,
                    title=video_title,
                    creator_name=best_video.get("creator_name"),
                    creator_region=best_video.get("creator_region"),
                    deep_link=f"https://app.clan.video/watch/{video_id}",
                ),
                action=action or "open_video",
                should_send=True,
                weak_indicator_matched=weak_indicator,
                confidence_score=best_video.get("score", 0.5),
                reason=f"Video matches weak indicator '{weak_indicator}' | Journey day {journey_day}",
            )
            
            return response
            
        except Exception as exc:
            logger.error("Failed to resolve notification | user_id=%s | error=%s", request.user_id, exc)
            raise
    
    def resolve_batch(
        self, batch_request: BatchBuildNotificationRequest
    ) -> BatchBuildNotificationResponse:
        """
        Resolve a batch of notification requests.
        
        Used for scheduled campaigns sending to multiple users.
        Attempts to process all users, collecting errors separately.
        
        Args:
            batch_request: Batch of validated notification requests
            
        Returns:
            BatchBuildNotificationResponse with results and errors
        """
        
        results = []
        errors = {}
        
        for item in batch_request.items:
            try:
                response = self.resolve_single(item)
                results.append(response)
            except Exception as exc:
                errors[str(item.user_id)] = str(exc)
                logger.error("Batch item failed | user_id=%s | error=%s", item.user_id, exc)
        
        batch_response = BatchBuildNotificationResponse(
            total=len(batch_request.items),
            successful=len(results),
            failed=len(errors),
            results=results,
            errors=errors,
        )
        
        logger.info(
            "Batch resolved | total=%d | successful=%d | failed=%d",
            batch_response.total,
            batch_response.successful,
            batch_response.failed,
        )
        
        return batch_response
    
    def _determine_audience_strategy(self, campaign_day: int, weak_indicator: str) -> str:
        """
        Determine audience strategy based on campaign day and user metrics.
        
        Strategies:
        - Day 1: Welcome message (region + language match)
        - Day 3: Early engagement
        - Day 5: Activity boost (metrics-based)
        - Day 7: Habit formation
        - Day 10: Mid-campaign push
        - Day 16: Final drive
        - Default: Weak indicator match
        """
        
        strategy_map = {
            1: "welcome_day1",
            3: "early_engagement_day3",
            5: "activity_metrics_day5",
            6: "performance_comparison_day6",
            7: "habit_formation_day7",
            10: "indicator_deep_dive_day10",
            11: "ranking_day11",
            12: "mid_campaign_day12",
            16: "final_push_day16",
        }
        
        return strategy_map.get(campaign_day, f"weak_indicator_{weak_indicator}")
    
    def _generate_notification_copy(
        self,
        campaign_day: int,
        user_name: str,
        weak_indicator: str,
        audience_strategy: str,
        video_title: str,
        metrics: dict,
    ) -> Tuple[str, str, Optional[str]]:
        """
        Generate notification title and body for the given campaign day.
        
        Uses NotificationEngine for day-specific templates + metrics.
        Fallback to generic templates if specific day not implemented.
        
        Returns:
            (title, body, action)
        """
        
        # Generic fallback templates (replace with actual engine calls)
        templates = {
            1: {
                "title": f"Welcome to your learning journey! 🎯",
                "body": f"Watch '{video_title}' to master {weak_indicator}",
                "action": "open_video",
            },
            3: {
                "title": f"Keep the momentum going! 💪",
                "body": f"3 days in! Learn about {weak_indicator}",
                "action": "open_video",
            },
            5: {
                "title": f"Boost your {weak_indicator} today",
                "body": f"5-day learner: Watch now →",
                "action": "open_video",
            },
            7: {
                "title": f"One week of growth! 🌟",
                "body": f"Master {weak_indicator} with this video",
                "action": "open_video",
            },
            10: {
                "title": f"Dive deeper into {weak_indicator}",
                "body": f"Day 10: Advanced strategies await",
                "action": "open_video",
            },
            16: {
                "title": f"Final sprint! Complete your learning",
                "body": f"Perfect your {weak_indicator} skills",
                "action": "open_video",
            },
        }
        
        template = templates.get(campaign_day, templates[1])  # Default to day 1 template
        
        return template["title"][:120], template["body"][:120], template["action"]
    
    def _extract_metrics(self, request: BuildNotificationRequest) -> dict:
        """Extract optional metrics from request for copy generation."""
        return {
            "yesterday_count": request.yesterday_count,
            "user_streak": request.user_streak,
            "current_rank": request.current_rank,
            "team_average": request.team_average_activity,
            "this_week_activities": request.this_week_activities,
            "targets": request.targets,
        }
    
    def _generate_cohort_key(
        self, user_id: int | str, campaign_day: int, region: str, language: str
    ) -> str:
        """
        Generate deterministic cohort key for reproducible video selection.
        
        Used by VideoSelector to pick same video for same user across days.
        Format: {campaign_day}_{user_id}_{region}_{language}
        """
        
        return f"{campaign_day}_{user_id}_{region}_{language}".lower()
    
    # ─────────────────────────────────────────────────────────────
    # DAY 2 NOTIFICATION SENDING WORKFLOW
    # ─────────────────────────────────────────────────────────────
    
    def send_notifications(
        self,
        user_id: int | str,
        user_name: str,
        weak_indicator: str,
        watched_video_ids: list[int] | None = None,
        months_in_role: int | None = None,
        campaign_day: int = 2,
    ) -> dict:
        """Build and log notification for a specific day using NotificationService."""
        if watched_video_ids is None:
            watched_video_ids = []
        
        try:
            # ─────────────────────────────────────────────────────────
            # STEP 1: Fetch user from database
            # ─────────────────────────────────────────────────────────
            user = self._get_user_by_id(user_id)
            logger.info(f"User fetched | user_id={user_id} | name={user.get('name')}")
            
            # ─────────────────────────────────────────────────────────
            # STEP 2-4: Day-aware recommendation + copy + payload build
            # ─────────────────────────────────────────────────────────
            service_req = NotificationRequest(
                user_id=user_id,
                user_name=user_name,
                role="RM",
                branch=user.get("branch"),
                region=user.get("zone") or user.get("branch") or "all",
                language=user.get("language_code") or "hi",
                user_language=user.get("language_code") or "hi",
                weak_indicator=weak_indicator,
                watched_video_ids=watched_video_ids,
                months_in_role=months_in_role,
                campaign_day=campaign_day,
            )

            built = self.notification_service.build_notification(service_req)
            notification = {
                "campaign_day": built.campaign_day,
                "notification_title": built.notification_title,
                "notification_body": built.notification_body,
                "audience_strategy": built.audience_strategy,
                "cohort_key": built.cohort_key,
                "video_title": built.video_title,
                "creator_name": built.creator_name,
                "action": built.action,
                "deep_link": built.deep_link,
                "notification_type": built.notification_type,
                "should_send": built.should_send,
            }
            logger.info(f"Notification object built | user_id={user_id}")
            
            # ─────────────────────────────────────────────────────────
            # STEP 5: Save to test log
            # ─────────────────────────────────────────────────────────
            test_file_path = self._save_to_test_log(
                user_id=user_id,
                user_name=user_name,
                notification=notification,
            )
            logger.info(f"Test log saved | path={test_file_path}")
            
            return {
                'success': True,
                'notification': notification,
                'user_id': user_id,
                'test_file_path': str(test_file_path),
            }
            
        except Exception as e:
            logger.error(f"Failed to build notification for user {user_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'user_id': user_id,
            }
    
    def _get_user_by_id(self, user_id: int | str) -> dict:
        """
        Fetch user details from PostgreSQL.
        
        Returns:
            {
                'id': int,
                'name': str,
                'branch': str,
                'zone': str,
                'language_code': str,
                'account_id': int
            }
        """
        try:
            with self.db_engine.connect() as conn:
                user = conn.execute(
                    text("""
                        SELECT u.id, u.name, u.branch, u.zone, u.account_id,
                               COALESCE(ml.language_code, 'hi') AS language_code
                        FROM "user" u
                        LEFT JOIN md_app_languages ml ON ml.id = u.app_language_id
                        WHERE u.id = :uid AND u.account_id = :account_id AND u.status = 1
                    """),
                    {'uid': int(user_id), 'account_id': ACCOUNT_ID},
                ).mappings().first()
            
            if not user:
                raise ValueError(f"User {user_id} not found or inactive")
            
            return dict(user)
        
        except Exception as e:
            logger.error(f"Failed to fetch user {user_id}: {str(e)}")
            raise
    
    def _get_all_users(self, campaign_day: int = 2) -> list[dict]:
        """
        Fetch all eligible users for a campaign day (for batch campaigns).
        
        Returns list of users at specific journey_day.
        Useful for scheduler to send bulk notifications.
        
        Returns:
            [{
                'id': int,
                'name': str,
                'branch': str,
                'zone': str,
                'language_code': str
            }, ...]
        """
        try:
            with self.db_engine.connect() as conn:
                users = conn.execute(
                    text("""
                        SELECT u.id, u.name, u.branch, u.zone,
                               COALESCE(ml.language_code, 'hi') AS language_code
                        FROM "user" u
                        LEFT JOIN md_app_languages ml ON ml.id = u.app_language_id
                        WHERE u.account_id = :account_id 
                          AND u.status = 1
                          AND CAST(:today AS date) - u.profile_activation_date::date + 1 = :campaign_day
                    """),
                    {
                        'account_id': ACCOUNT_ID,
                        'campaign_day': campaign_day,
                        'today': datetime.now().strftime('%Y-%m-%d'),
                    },
                ).mappings().all()
            
            return [dict(u) for u in users]
        
        except Exception as e:
            logger.error(f"Failed to fetch users for day {campaign_day}: {str(e)}")
            return []
    
    def _recommend_video_for_user(
        self,
        user_id: int | str,
        weak_indicator: str,
        watched_ids: list[int] | None = None,
    ) -> dict:
        """
        Get best video from Qdrant for user based on weak indicator.
        
        Returns:
            {
                'video_id': str,
                'title': str,
                'creator_name': str,
                'creator_region': str,
            }
        """
        if watched_ids is None:
            watched_ids = []
        
        try:
            best_video, _ = recommend_video(
                user_name='User',
                weak_indicator=weak_indicator,
                user_role='RM',
                user_region='all',
                journey_day=2,
                months_in_role=None,
                watched_ids=watched_ids,
            )
            
            if not best_video:
                raise ValueError(f"No video found for weak_indicator={weak_indicator}")
            
            return {
                'video_id': str(best_video.get('video_id', '')),
                'title': str(best_video.get('title', 'Video')),
                'creator_name': str(best_video.get('creator_name', 'Top Performer')),
                'creator_region': str(best_video.get('creator_region', 'Your Region')),
            }
        
        except Exception as e:
            logger.error(f"Failed to recommend video for user {user_id}: {str(e)}")
            raise
    
    def _generate_day2_copy(self) -> Tuple[str, str]:
        """
        Generate Day 2 notification title and body.
        
        Day 2 is about showing proof from high performers.
        
        Returns:
            (title, body)
        """
        title = "Today's 2-minute tip from a top performer near you"
        body = "See the exact approach they use to close more deals in the field."
        
        return title, body
    
    def _build_notification_object(
        self,
        campaign_day: int,
        title: str,
        body: str,
        video: dict,
        user: dict,
    ) -> dict:
        """
        Build complete notification object with all required fields.
        
        Returns:
            {
                'campaign_day': 2,
                'notification_title': str,
                'notification_body': str,
                'audience_strategy': str,
                'cohort_key': str,
                'video_title': str,
                'creator_name': str,
                'action': str,
                'deep_link': str,
                'should_send': bool
            }
        """
        # Truncate to max lengths
        title = str(title)[:120]
        body = str(body)[:120]
        
        # Build cohort key for reproducibility
        cohort_key = f"day{campaign_day}_{user.get('id')}_{user.get('zone', 'all').lower()}_hi"
        
        # Build deep link
        deep_link = f"https://app.clan.video/watch/{video['video_id']}"
        
        return {
            'campaign_day': campaign_day,
            'notification_title': title,
            'notification_body': body,
            'audience_strategy': 'same_video_for_region_language',
            'cohort_key': cohort_key,
            'video_title': video['title'],
            'creator_name': video['creator_name'],
            'action': 'open_video',
            'deep_link': deep_link,
            'should_send': True,
        }
    
    def _save_to_test_log(
        self,
        user_id: int | str,
        user_name: str,
        notification: dict,
    ) -> Path:
        """
        Save notification to test_notifications_log.json for manual testing.
        
        This allows you to:
        1. Build notifications without sending FCM
        2. Review what would be sent
        3. Test by logging in as user and checking
        
        Returns:
            Path to test log file
        """
        try:
            log_file = Path('./test_notifications_log.json')
            
            # Load existing log or create new
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
            else:
                log_data = []
            
            # Create entry
            entry = {
                'timestamp': datetime.utcnow().isoformat(),
                'user_id': int(user_id),
                'user_name': str(user_name),
                'notification': notification,
                'status': 'built',  # Not sent (for manual testing in app)
            }
            
            # Append
            log_data.append(entry)
            
            # Save
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Notification logged to {log_file} | user_id={user_id}")
            return log_file
        
        except Exception as e:
            logger.error(f"Failed to save to test log: {str(e)}")
            raise


# ─────────────────────────────────────────────────────────────
# Singleton resolver instance
# ─────────────────────────────────────────────────────────────

_resolver: Optional[NotificationResolver] = None


def get_resolver() -> NotificationResolver:
    """Get or create the singleton resolver instance."""
    global _resolver
    if _resolver is None:
        _resolver = NotificationResolver()
        logger.info("Notification resolver initialized")
    return _resolver


def resolve_notification(request: BuildNotificationRequest) -> BuildNotificationResponse:
    """
    Convenience function: Resolve single notification request.
    
    Usage:
        from notificationschema.resolver import resolve_notification
        response = resolve_notification(request)
    """
    resolver = get_resolver()
    return resolver.resolve_single(request)


def resolve_notifications_batch(
    batch_request: BatchBuildNotificationRequest,
) -> BatchBuildNotificationResponse:
    """
    Convenience function: Resolve batch of notification requests.
    
    Usage:
        from notificationschema.resolver import resolve_notifications_batch
        response = resolve_notifications_batch(batch_request)
    """
    resolver = get_resolver()
    return resolver.resolve_batch(batch_request)
