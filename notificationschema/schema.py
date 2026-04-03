"""
Unified Notification Schema for Real-Time User Notifications

This module defines the single, unified endpoint for building and fetching
notifications for real-time users. All input validation and response formatting
happens here.
"""

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from constants import normalize_language

CampaignDay = Literal[1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 16]


class BuildNotificationRequest(BaseModel):
    """
    Request schema for the unified notification endpoint.
    
    Real-time workflow:
    1. User opens app → frontend sends user context (id, name, role, region, language)
    2. Backend enriches with DB data (journey_day, weak_indicator, metrics)
    3. This schema validates and normalizes all inputs
    4. Resolver generates notification content + video selection
    """
    
    # Required: User identity
    user_id: str | int = Field(..., description="Unique user identifier")
    user_name: str = Field(..., description="User's display name")
    
    # Required: User context
    role: str = Field(..., description="Job role: RM, BM, SUPERVISOR, etc.")
    region: str = Field(default="all", description="Geographic region")
    language: str = Field(default="hi", description="User's app language")
    
    # Journey tracking
    journey_day: int = Field(default=1, ge=1, le=365, description="Days since profile activation")
    months_in_role: Optional[int] = Field(None, ge=0, description="Months in current role")
    
    # Performance indicators
    weak_indicator: Optional[str] = Field(None, description="User's weakest KII to improve")
    watched_video_ids: list[int] = Field(default_factory=list, description="Videos already shown to user")
    
    # Campaign day (auto-inferred from journey_day if not provided)
    campaign_day: Optional[CampaignDay] = Field(None, description="Target campaign day")
    
    # Metrics for enhanced copy (days 5, 6, 10, 11, 16)
    yesterday_count: Optional[int] = None
    user_streak: Optional[int] = None
    last_7_days_activity_count: list[int] = Field(default_factory=list)
    team_average_activity: Optional[float] = None
    team_logged_in_today: Optional[int] = None
    team_total_members: Optional[int] = None
    current_rank: Optional[int] = None
    previous_rank: Optional[int] = None
    total_users_in_region: Optional[int] = None
    this_week_activities: dict[str, int] = Field(default_factory=dict)
    targets: dict[str, int] = Field(default_factory=dict)
    last_week_activities: dict[str, int] = Field(default_factory=dict)
    team_average: dict[str, float] = Field(default_factory=dict)
    
    @model_validator(mode="before")
    @classmethod
    def infer_campaign_day(cls, data: dict):
        """Auto-infer campaign_day from journey_day if not explicitly set."""
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
        """Normalize user name: strip whitespace, fail if empty."""
        text = " ".join(str(v).split()).strip()
        if not text:
            raise ValueError("user_name cannot be empty")
        return text
    
    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str):
        """Normalize role to uppercase."""
        return str(v).strip().upper()
    
    @field_validator("region")
    @classmethod
    def validate_region(cls, v: str):
        """Map empty/null regions to 'all' for fallback."""
        text = str(v or "").strip()
        if text.lower() in {"", "null", "none", "na", "n/a"}:
            return "all"
        return text.lower()
    
    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str):
        """Normalize language code (e.g., 'hin' → 'hi')."""
        if v is None:
            return "hi"
        return normalize_language(v)
    
    @field_validator("weak_indicator")
    @classmethod
    def normalize_weak_indicator(cls, v: Optional[str]):
        """Normalize weak indicator: lowercase, underscore-separated."""
        if v is None:
            return None
        return str(v).strip().lower().replace(" ", "_")


class VideoReference(BaseModel):
    """Embedded video metadata in notification response."""
    video_id: str
    title: str
    creator_name: Optional[str] = None
    creator_region: Optional[str] = None
    deep_link: Optional[str] = None


class BuildNotificationResponse(BaseModel):
    """
    Response schema for the unified notification endpoint.
    
    Contains all information needed for:
    1. Frontend: Display title and body
    2. Backend: Track sent notifications and link to video
    3. Analytics: Understand user engagement
    """
    
    # Success indicator
    success: bool = Field(default=True, description="Request processed successfully")
    
    # Notification content (240 chars max: 120 title + 120 body)
    notification_title: str = Field(..., max_length=120, description="Notification title")
    notification_body: str = Field(..., max_length=120, description="Notification body text")
    
    # Metadata
    campaign_day: CampaignDay = Field(..., description="Which campaign day this is for")
    audience_strategy: str = Field(..., description="Segmentation strategy used (e.g., 'weak_indicator_match')")
    cohort_key: str = Field(..., description="Deterministic key for reproducible selection (MD5 hash)")
    
    # Video to show if notification is clicked
    video: VideoReference = Field(..., description="Recommended video metadata")
    
    # Actions
    action: Optional[str] = Field(None, description="Action type: 'open_video', 'open_profile', etc.")
    should_send: bool = Field(default=True, description="Whether to actually send this notification")
    
    # Debug/audit
    weak_indicator_matched: Optional[str] = None
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Recommendation confidence")
    reason: Optional[str] = Field(None, description="Why this video was selected")
    
    @field_validator("notification_title")
    @classmethod
    def validate_title(cls, v: str):
        """Title must be non-empty."""
        text = str(v).strip()
        if not text:
            raise ValueError("notification_title cannot be empty")
        return text
    
    @field_validator("notification_body")
    @classmethod
    def validate_body(cls, v: str):
        """Body must be non-empty."""
        text = str(v).strip()
        if not text:
            raise ValueError("notification_body cannot be empty")
        return text


class BatchBuildNotificationRequest(BaseModel):
    """Batch request for multiple users at once (for scheduled campaigns)."""
    items: list[BuildNotificationRequest] = Field(..., max_items=5000)
    
    @field_validator("items")
    @classmethod
    def validate_items(cls, v: list[BuildNotificationRequest]):
        """Validate batch size."""
        if not v:
            raise ValueError("items cannot be empty")
        if len(v) > 5000:
            raise ValueError("items cannot exceed 5000 in one batch")
        return v


class BatchBuildNotificationResponse(BaseModel):
    """Batch response with all notifications built and ready to send."""
    total: int = Field(..., description="Total notifications requested")
    successful: int = Field(..., description="Successfully built")
    failed: int = Field(..., description="Failed to build")
    results: list[BuildNotificationResponse] = Field(..., description="Built notifications")
    errors: dict[str, str] = Field(default_factory=dict, description="Error details by user_id")
