from typing import Literal, Optional

from constants import normalize_language
from pydantic import BaseModel, Field, field_validator, model_validator

CampaignDay = Literal[1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 16]


class NotificationRequest(BaseModel):
    user_id: str | int
    user_name: str
    role: Optional[str] = None
    branch: Optional[str] = None
    region: Optional[str] = None
    language: str = "hi"
    user_language: Optional[str] = None # Fallback / alias for language
    journey_day: Optional[int] = None
    weak_indicator: Optional[str] = None
    watched_video_ids: list[int] = []
    months_in_role: Optional[int] = None
    campaign_day: Optional[CampaignDay] = None 

    # Fields for Days 5, 6, 10, 11, 16
    yesterday_count: Optional[int] = None
    user_streak: Optional[int] = None
    last_7_days_activity_count: list[int] = []
    team_average_activity: Optional[float] = None
    team_logged_in_today: Optional[int] = None
    team_total_members: Optional[int] = None
    current_rank: Optional[int] = None
    previous_rank: Optional[int] = None
    total_users_in_region: Optional[int] = None
    this_week_activities: dict[str, int] = {}
    targets: dict[str, int] = {}
    last_week_activities: dict[str, int] = {}
    team_average: dict[str, float] = {}

    # Keep video selection separate for now; these are optional inputs that
    # can be supplied by a scheduler/backend when available.
    video_id: Optional[str] = None
    video_title: Optional[str] = None
    creator_name: Optional[str] = None
    creator_region: Optional[str] = None
    creator_team: Optional[str] = None
    outcome_hint: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def infer_campaign_day(cls, data: dict):
        if isinstance(data, dict):
            if data.get("campaign_day") is None:
                jd = data.get("journey_day")
                if jd in (1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 16):
                    data["campaign_day"] = jd
                else:
                    data["campaign_day"] = 2
        return data

    @field_validator("user_name")
    @classmethod
    def validate_user_name(cls, v: str):
        text = " ".join(str(v).split())
        if not text:
            raise ValueError("user_name cannot be empty")
        return text

    @field_validator("region")
    @classmethod
    def validate_region(cls, v: Optional[str]):
        text = str(v or "").strip()
        if text.lower() in {"", "null", "none", "na", "n/a"}:
            return "all"
        return text

    @field_validator("language", "user_language")
    @classmethod
    def validate_language(cls, v: Optional[str]):
        if v is None:
            return None
        return normalize_language(v)

    @field_validator("video_id", "video_title", "creator_name", "creator_region", "creator_team", "outcome_hint")
    @classmethod
    def normalize_optional_text(cls, v: Optional[str]):
        if v is None:
            return None
        text = " ".join(str(v).split()).strip()
        return text or None

class NotificationResponse(BaseModel):
    campaign_day: CampaignDay
    notification_title: str = Field(max_length=120)
    notification_body: str = Field(max_length=120)

    audience_strategy: str
    cohort_key: str

    action: Optional[str] = None
    deep_link: Optional[str] = None
    notification_type: Optional[str] = None
    should_send: bool = True

    # Optional values for downstream storage/sending
    video_id: Optional[str] = None
    video_title: Optional[str] = None
    creator_name: Optional[str] = None

    @field_validator("notification_title")
    @classmethod
    def validate_title(cls, v: str):
        text = str(v).strip()
        if not text:
            raise ValueError("notification_title cannot be empty")
        return text

    @field_validator("notification_body")
    @classmethod
    def validate_body(cls, v: str):
        text = str(v).strip()
        if not text:
            raise ValueError("notification_body cannot be empty")
        return text


class BatchNotificationRequest(BaseModel):
    items: list[NotificationRequest]

    @field_validator("items")
    @classmethod
    def validate_items(cls, v: list[NotificationRequest]):
        if not v:
            raise ValueError("items cannot be empty")
        if len(v) > 5000:
            raise ValueError("items cannot exceed 5000 in one call")
        return v


class BatchNotificationResponse(BaseModel):
    total: int
    results: list[NotificationResponse]
