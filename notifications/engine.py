import re
from typing import Dict, Optional, Any

from constants import ACCOUNT_ID
from langchain_core.prompts import PromptTemplate

from .database_config import PostgresConfig
from .models import NotificationRequest


class NotificationEngine:
    """Deterministic notification generator.

    Uses LangChain PromptTemplate for structured formatting only.
    No LLM/API call is made here, so runtime cost remains near-zero.
    """

    day2_template = PromptTemplate.from_template(
        "See the exact approach {creator_name} uses to close more deals in the field."
    )

    day4_template = PromptTemplate.from_template(
        "{creator_name} is closing deals faster than anyone else in your region."
    )

    day12_template = PromptTemplate.from_template(
        "{creator_name} from {network_scope} shares tips on {outcome_hint}."
    )

    fallback_day12_template = PromptTemplate.from_template(
        "Someone from {network_scope} shares tips on {outcome_hint}."
    )

    _GENERIC_PATTERNS = [
        "new video from someone",
        "top rm from your region",
        "today's 2-minute tip",
    ]

    # Notification type enums expected by downstream sender.
    _DAY_NOTIFICATION_TYPES = {
        1: "PUSH_VIDEO_FOR_USER",
        2: "PUSH_VIDEO_FOR_USER",
        3: "JOURNEY_HOME",
        4: "PUSH_VIDEO_FOR_USER",
        5: "FILL_DARTS",
        6: "LEADING_BETA_OF_THE_DAY",
        7: "JOURNEY_SET",
        10: "TRENDING_VIDEO",
        11: "LEADING_BETA",
        12: "NEW_VIDEO",
        16: "DARTS_HOME",
    }

    _DAY5_INDICATOR_LABELS = {
        "customer_generation": "customers generated",
        "customer_interested": "interested customers converted",
        "customer_document_uploaded": "customer documents uploaded",
        "customer_loan_approved": "loans approved",
        "customer_loan_disbursed": "loans disbursed",
        "product_trainings_attended": "product trainings attended",
        "marketing_activities_conducted": "marketing activities conducted",
        "daily_huddle_meeting_attended": "daily huddles attended",
        "potential_channel_partners_identified": "potential channel partners identified",
        "channel_partners_empanelled": "channel partners empanelled",
    }

    def __init__(self):
        self._indicator_action_labels: Optional[dict[str, str]] = None

    @staticmethod
    def _normalize_space(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _short_name(self, user_name: str, max_chars: int = 28) -> str:
        text = self._normalize_space(user_name)
        if len(text) <= max_chars:
            return text
        clipped = text[: max_chars - 3].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0]
        return (clipped or text[: max_chars - 3]).rstrip() + "..."

    def _safe_truncate(self, text: str, max_chars: int = 120) -> str:
        text = self._normalize_space(text)
        if len(text) <= max_chars:
            return text

        cutoff = max_chars - 3
        clipped = text[:cutoff].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0]
        clipped = clipped.rstrip(" ,;:-")
        return (clipped or text[:cutoff]).rstrip() + "..."

    def _is_generic(self, text: str) -> bool:
        body = self._normalize_space(text).lower()
        return any(pattern in body for pattern in self._GENERIC_PATTERNS)

    def _de_genericize(self, req: NotificationRequest) -> str:
        user_name = self._short_name(req.user_name)
        if req.campaign_day == 2:
            return "Day 2 action: identify one customer need before your first pitch today."
        if req.campaign_day == 4:
            return "Day 4 action: use one proven regional opening line and push to next step."

        network_scope = self._normalize_space(req.creator_team or req.creator_region or req.region or "")
        if network_scope.lower() in {"", "all", "unknown", "na", "n/a"}:
            network_scope = "your network"
        outcome_hint = self._normalize_space(req.outcome_hint or "improving conversions").lower()
        return f"Someone from {network_scope} shares tips on {outcome_hint}."

    def _enforce_limits(
        self,
        title: str,
        body: str,
        action: str = "",
        should_send: bool = True,
        deep_link: str = "",
        notification_type: str = "JOURNEY",
    ) -> Dict[str, Any]:
        title = self._safe_truncate(title, max_chars=120)
        body = self._safe_truncate(body, max_chars=120)

        return {
            "title": title,
            "body": body,
            "action": action,
            "should_send": should_send,
            "deep_link": deep_link,
            "notification_type": notification_type,
        }

    def _notification_type_for_day(self, campaign_day: int) -> str:
        return self._DAY_NOTIFICATION_TYPES.get(int(campaign_day), "JOURNEY")

    @staticmethod
    def _video_deep_link(video_id: Optional[str]) -> str:
        value = str(video_id or "").strip()
        if not value:
            return ""
        return f"https://app.clan.video/watch/{value}"

    def generate_day1(self, req: NotificationRequest) -> Dict[str, Any]:
        title = "Welcome to Clan - India's first community of top performers."
        body = "Watch 1 short video from a star."
        return self._enforce_limits(
            title=title,
            body=body,
            action="Watch 1 short video from a star",
            deep_link=self._video_deep_link(req.video_id),
            notification_type=self._notification_type_for_day(1),
        )

    def generate_day2(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        title = "Today's 2-minute tip from a top performer near you"
        creator_name = self._normalize_space(req.creator_name or "a top performer")
        body = self.day2_template.format(user_name=user_name, creator_name=creator_name)
        if self._is_generic(body):
            body = self._de_genericize(req)
        return self._enforce_limits(
            title=title,
            body=body,
            action="Open video",
            deep_link=self._video_deep_link(req.video_id),
            notification_type=self._notification_type_for_day(2),
        )

    def generate_day3(self, req: NotificationRequest) -> Dict[str, Any]:
        title = "Day 3 Performance Check"
        body = "Compare your effort vs top performers."
        return self._enforce_limits(
            title=title,
            body=body,
            action="Open My Performance Snapshot",
            deep_link="clan://journey/home",
            notification_type=self._notification_type_for_day(3),
        )

    def generate_day4(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        title = "Top RM from your region shares how they close leads faster."
        creator_name = self._normalize_space(req.creator_name or "a top performer")
        body = self.day4_template.format(user_name=user_name, creator_name=creator_name)
        if self._is_generic(body):
            body = self._de_genericize(req)
        return self._enforce_limits(
            title=title,
            body=body,
            action="Open video",
            deep_link=self._video_deep_link(req.video_id),
            notification_type=self._notification_type_for_day(4),
        )

    def _indicator_to_action_label(self, name: str) -> str:
        cleaned = self._normalize_space(name).lower()
        return cleaned or "activities"

    def _get_indicator_action_labels(self) -> dict[str, str]:
        if self._indicator_action_labels is not None:
            return self._indicator_action_labels

        labels: dict[str, str] = {}
        try:
            indicators = PostgresConfig(account_id=ACCOUNT_ID).get_indicators()
            for indicator in indicators:
                code = str(indicator.get("code", "")).strip().lower().replace(" ", "_")
                if not code:
                    continue
                labels[code] = self._indicator_to_action_label(str(indicator.get("name", "")))
        except Exception:
            labels = {}

        self._indicator_action_labels = labels
        return self._indicator_action_labels

    def generate_day5(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        counts = req.last_7_days_activity_count or []
        team_avg = req.team_average_activity or 0.0
        yesterday_count = req.yesterday_count if req.yesterday_count is not None else (counts[-1] if counts else 0)
        weak = self._normalize_space((req.weak_indicator or "").replace(" ", "_")).lower()
        indicator_label = self._DAY5_INDICATOR_LABELS.get(weak, "activities")
        
        if not counts:
            rolling_avg = team_avg
        else:
            rolling_avg = sum(counts) / len(counts)
            
        if yesterday_count == 0:
            # "use team_average as baseline" without the 1.15 multiplier if they did 0 yesterday
            target = max(1, round(team_avg))
        else:
            target = round(rolling_avg * 1.15)
            target = max(1, target)
            
            # Ensure target represents a logically increasing challenge where possible
            if target <= yesterday_count:
                target = yesterday_count + 1
            
        if team_avg > 0:
            target = min(target, round(team_avg * 2))
            
        if yesterday_count == 0:
            title = "Let's get active today"
            body = f"yesterday you hit 0 {indicator_label}. Can you reach {target} today?"
        elif target > yesterday_count:
            title = "Can you beat yesterday's effort?"
            body = f"yesterday you hit {yesterday_count} {indicator_label}. Can you reach {target} today?"
        else:
            title = "Maintaining High Performance"
            body = f"yesterday you hit {yesterday_count} {indicator_label}. Great consistency. Can you hit at least {target} today as well?"
            
        if user_name.lower() not in body.lower():
            body = f"{user_name}, {body}"
        return self._enforce_limits(
            title=title,
            body=body,
            action="Update 1 activity",
            deep_link="clan://darts/home",
            notification_type=self._notification_type_for_day(5),
        )

    def generate_day6(self, req: NotificationRequest) -> Dict[str, Any]:
        logged_in = req.team_logged_in_today or 0
        total = req.team_total_members or 0
        
        if total == 0:
            return self._enforce_limits(
                title="Lead the day from the start",
                body="You can set the pace today. Log in now and complete your first activity.",
                action="Log in and start",
                deep_link="clan://darts/home",
                notification_type=self._notification_type_for_day(6),
            )
            
        pct = round((logged_in / total) * 100)
        
        if pct == 0:
            title = "Be the first to log in!"
            body = "None of your team members are active yet. Take the lead today."
        elif pct < 50:
            title = f"{pct}% of your team logged in today."
            body = "Your team is getting started. Open the app and join them."
        elif pct < 100:
            title = f"{pct}% of your team logged in today."
            body = "Don't fall behind! Over half your team is already active. Start your day now."
        else:
            title = "100% of your team logged in today."
            body = "They are already closing deals! Open the app and join them."
            
        return self._enforce_limits(
            title=title,
            body=body,
            action="Do one activity on the app",
            deep_link="clan://darts/home",
            notification_type=self._notification_type_for_day(6),
        )

    def generate_day7(self, req: NotificationRequest) -> Dict[str, Any]:
        title = "7 Days Completed"
        body = "You completed 7 days on Clan."
        return self._enforce_limits(
            title=title,
            body=body,
            action="NO ACTION TODAY.",
            deep_link="clan://journey/home",
            notification_type=self._notification_type_for_day(7),
        )

    def generate_day10(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        counts = req.last_7_days_activity_count or []
        team_avg = req.team_average_activity or 0.0
        rolling_avg = sum(counts) / len(counts) if counts else team_avg
        target = max(1, round(rolling_avg * 1.15))
        if team_avg > 0:
            target = min(target, round(team_avg * 2))

        indicator_labels = self._get_indicator_action_labels()
        weak = req.weak_indicator or "activities"
        readable_indicator = indicator_labels.get(weak, self._normalize_space(weak.replace("_", " ")) or "activities")
        
        title = "Top performer challenge"
        
        if not req.video_id:
            body = f"your target today: {target} {readable_indicator}."
        elif not req.creator_name:
            body = f"your target today: {target} {readable_indicator}. Try this method."
        else:
            creator = self._short_name(req.creator_name)
            body = f"your target today: {target} {readable_indicator}. {creator}'s method gets it done in 2 hours."
            
        if user_name.lower() not in body.lower():
            body = f"{user_name}, {body}"
            return self._enforce_limits(
                title=title,
                body=body,
                action="Watch + attempt",
                deep_link=self._video_deep_link(req.video_id),
                notification_type=self._notification_type_for_day(10),
            )

    def generate_day11(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        current_rank = req.current_rank
        previous_rank = req.previous_rank
        
        if current_rank is None or previous_rank is None:
            return self._enforce_limits(
                title="Leaderboard Movement",
                body="Skipped: rank missing",
                should_send=False,
                notification_type=self._notification_type_for_day(11),
            )
            
        rank_change = previous_rank - current_rank
        if rank_change <= 0:
            return self._enforce_limits(
                title="Leaderboard Movement",
                body="Skipped: rank dropped",
                should_send=False,
                notification_type=self._notification_type_for_day(11),
            )
            
        if current_rank == 1:
            body = f"you're #1 this week. Defend your spot."
        else:
            body = f"you jumped from #{previous_rank} to #{current_rank} this week. Keep pushing."
            
        title = "You moved up on the leaderboard!"
        if user_name.lower() not in body.lower():
            body = f"{user_name}, {body}"
        return self._enforce_limits(
            title=title,
            body=body,
            action="Check ranking",
            deep_link="clan://journey/home",
            notification_type=self._notification_type_for_day(11),
        )

    def generate_day16(self, req: NotificationRequest) -> Dict[str, Any]:
        user_name = self._short_name(req.user_name)
        this_week: dict[str, int] = req.this_week_activities or {}
        targets: dict[str, int] = req.targets or {}
        last_week: dict[str, int] = req.last_week_activities or {}
        team_avg_dict: dict[str, float] = req.team_average or {}
        
        if not this_week and not targets:
            body = "no activities logged yet. Your target is 7 — start today."
            if user_name.lower() not in body.lower(): body = f"{user_name}, {body}"
            return self._enforce_limits(
                title="Performance Insight",
                body=body,
                action="Review Insights",
                deep_link="clan://darts/home",
                notification_type=self._notification_type_for_day(16),
            )
            
        all_zeros = len(this_week) > 0 and all(v == 0 for v in this_week.values())
        if all_zeros or not this_week:
            body = "no activities logged yet. Your target is 7 — start today."
            if user_name.lower() not in body.lower(): body = f"{user_name}, {body}"
            return self._enforce_limits(
                title="Performance Insight",
                body=body,
                action="Review Insights",
                deep_link="clan://darts/home",
                notification_type=self._notification_type_for_day(16),
            )
        
        biggest_gap_activity: Optional[str] = None
        max_gap = -9999
        all_hit = True
        
        for activity, acc in this_week.items():
            t = targets.get(activity, team_avg_dict.get(activity, 0))
            if acc < t:
                all_hit = False
            gap = t - acc
            if gap > max_gap:
                max_gap = gap
                biggest_gap_activity = activity
                
        if biggest_gap_activity is None:
            biggest_gap_activity = "activities"
            max_gap = 0
            
        display_activity = str(biggest_gap_activity).replace("_", " ")
        if not display_activity.endswith('s') and display_activity != "activities":
            display_activity += "s"
            
        current = this_week.get(biggest_gap_activity, 0)
        target = targets.get(biggest_gap_activity, team_avg_dict.get(biggest_gap_activity, 0))
        previous = last_week.get(biggest_gap_activity, 0)
        team_avg = team_avg_dict.get(biggest_gap_activity, 0)
        
        total_this_week = sum(this_week.values()) if this_week else 0
        total_last_week = sum(last_week.values()) if last_week else 0
        
        title = "Performance Insight"
        if total_last_week > 0 and total_this_week > total_last_week:
            overall_pct = round(((total_this_week - total_last_week) / total_last_week) * 100)
            title = f"Your effort improved {overall_pct}% this week."
            
        if all_hit and targets:
            body = f"you've hit all targets this week. Outstanding."
        elif max_gap == 1:
            body = f"just 1 more {display_activity.replace('s', '', 1) if display_activity.endswith('s') else display_activity} closes your week."
        elif max_gap > 0 and previous > 0 and current > previous:
            pct_improvement = round(((current - previous) / previous) * 100)
            singular = display_activity[:-1] if display_activity.endswith('s') and display_activity != "activities" else display_activity
            body = f"{pct_improvement}% {singular} rate! {max_gap} more to reach your goal."
        elif max_gap > 0 and current <= previous and previous > 0:
            body = f"{max_gap} {display_activity} behind — today and tomorrow to close it."
        elif current > target and max_gap > 0:
            exceeded = [k for k, v in this_week.items() if targets.get(k, 0) > 0 and v > targets.get(k, 0)]
            if exceeded:
                exc_display = exceeded[0].replace("_", " ")
                if not exc_display.endswith('s'): exc_display += "s"
                body = f"{exc_display} above target. {display_activity.capitalize()} need attention."
            else:
                body = f"you're at {current} activities. Target is {target} — {max_gap} to go."
        elif current > team_avg and current < target:
            body = f"above team average but {max_gap} {display_activity} short of your target."
        else:
            body = f"you're at {current} activities. Target is {target} — {max_gap} to go."
            
        if user_name.lower() not in body.lower():
            body = f"{user_name}, {body}"
        return self._enforce_limits(
            title=title,
            body=body,
            action="Review Insights",
            deep_link="clan://darts/home",
            notification_type=self._notification_type_for_day(16),
        )

    def generate(self, req: NotificationRequest) -> Dict[str, Any]:
        if req.campaign_day == 1:
            return self.generate_day1(req)
        if req.campaign_day == 2:
            return self.generate_day2(req)
        if req.campaign_day == 3:
            return self.generate_day3(req) 
        if req.campaign_day == 4:
            return self.generate_day4(req)
        if req.campaign_day == 7:
            return self.generate_day7(req)

        if req.campaign_day == 5: return self.generate_day5(req)
        if req.campaign_day == 6: return self.generate_day6(req)
        if req.campaign_day == 10: return self.generate_day10(req)
        if req.campaign_day == 11: return self.generate_day11(req)
        if req.campaign_day == 16: return self.generate_day16(req)

        # Day 12
        creator_name = self._normalize_space(req.creator_name or "a colleague")
        network_scope = self._normalize_space(req.creator_team or req.creator_region or req.region or "")
        if network_scope.lower() in {"", "all", "unknown", "na", "n/a"}:
            network_scope = "your network"
        outcome_hint = self._normalize_space(req.outcome_hint or "improving conversions").lower()
        short_video_title = self._safe_truncate(req.video_title or "", max_chars=42)

        if req.creator_name:
            title = "New video from someone you know"
            body = self.day12_template.format(
                user_name=user_name,
                creator_name=creator_name,
                network_scope=network_scope,
                outcome_hint=outcome_hint,
            )
        else:
            title = "New video from someone you know"
            body = self.fallback_day12_template.format(network_scope=network_scope, outcome_hint=outcome_hint)

        if short_video_title:
            body = f"{body} Watch: {short_video_title}"

        if self._is_generic(body):
            body = self._de_genericize(req)

        return self._enforce_limits(
            title=title,
            body=body,
            action="Open video",
            deep_link=self._video_deep_link(req.video_id),
            notification_type=self._notification_type_for_day(12),
        )
