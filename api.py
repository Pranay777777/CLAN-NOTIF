import logging
import os
import re
import time
import sqlite3
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import json

from fastapi import FastAPI, HTTPException, Request
import requests
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchAny

from constants import ACCOUNT_ID, is_excluded_video, normalize_language
from notifications.models import NotificationRequest as CampaignNotificationRequest
from notifications.models import NotificationResponse as CampaignNotificationResponse
from notifications.models import BatchNotificationRequest as CampaignBatchNotificationRequest
from notifications.models import BatchNotificationResponse as CampaignBatchNotificationResponse
from notifications.service import NotificationService
from qdrant.indicator_sync import sync_qdrant_payload_from_postgres
from qdrant.query import get_all_video_ids, query_points, scroll_points
from notifications.database_config import PostgresConfig
from database.db_config import SessionLocal
from notificationschema.resolver import NotificationResolver
from send_notification_to_user import get_user_details
from weak_indicator import get_weak_indicator

# ════════════════════════════════════════════════════════════════════════════════
# SETUP LOGGING
# ════════════════════════════════════════════════════════════════════════════════

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/clan_api.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("api")
load_dotenv()

# ════════════════════════════════════════════════════════════════════════════════
# INITIALIZE FASTAPI AND CORE COMPONENTS
# ════════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="CLAN Video Recommendation API")
model = SentenceTransformer("all-MiniLM-L6-v2")
notification_resolver = NotificationResolver()

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

NOTIFICATION_API_URL = "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications"

REMOTE_NOTIFICATION_SEND_URL = os.getenv(
    "REMOTE_NOTIFICATION_SEND_URL",
    "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications",
).strip()

REMOTE_NOTIFICATION_TYPE = os.getenv("REMOTE_NOTIFICATION_TYPE", "video_recommendation").strip()

REMOTE_NOTIFICATION_TIMEOUT_SECONDS = float(
    os.getenv("REMOTE_NOTIFICATION_TIMEOUT_SECONDS", "30")
)

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333").strip()

qdrant_client = QdrantClient(url=QDRANT_URL)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

DB_FILE = os.getenv("DATABASE_PATH", "your_database.db")

# ════════════════════════════════════════════════════════════════════════════════
# INDICATOR CONFIGURATION (Dynamic)
# ════════════════════════════════════════════════════════════════════════════════

INDICATOR_LABELS = {}
INDICATOR_PROBLEM_KEYWORDS = {}
VALID_INDICATORS = set()
_INDICATOR_CONFIG_LOADED = False

VALID_ROLES = {"RM", "BM", "SUPERVISOR"}
VALID_LANGUAGE_MATCH_TYPES = {"exact", "english_fallback", "other_fallback"}

# ── QDRANT WATCHED VIDEOS TRACKING ────────────────────────

def get_user_watched_videos(user_id: int) -> list[int]:
    """Get watched videos from Qdrant"""
    try:
        from qdrant_client import QdrantClient
        
        client = QdrantClient(url="http://172.20.3.65:6333")
        watched_point_id = user_id * 1000000
        
        try:
            points = client.retrieve(
                collection_name="clan_videos",
                ids=[watched_point_id],
                with_payload=True
            )
            
            if points and len(points) > 0:
                payload = points[0].payload or {}
                watched_ids = payload.get("watched_video_ids", [])
                logger.info(f"User {user_id} watched: {watched_ids}")
                return watched_ids
        except Exception as inner_e:
            logger.debug(f"No watched point for user {user_id}: {inner_e}")
        
        return []
        
    except Exception as e:
        logger.error(f"Error in get_user_watched_videos: {e}", exc_info=True)
        return []


def save_watched_video(user_id: int, video_id: int, campaign_day: int) -> None:
    """Save watched video to Qdrant"""
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import PointStruct
        from datetime import datetime
        
        client = QdrantClient(url="http://172.20.3.65:6333")
        watched_point_id = user_id * 1000000
        
        # Get current watched list
        watched_ids = get_user_watched_videos(user_id)
        
        # Add new video
        video_id_int = int(video_id)
        if video_id_int not in watched_ids:
            watched_ids.append(video_id_int)
        
        # Update point
        payload = {
            "watched_video_ids": watched_ids,
            "user_id": user_id,
            "last_updated": str(datetime.now()),
            "total_watched": len(watched_ids),
        }
        
        point = PointStruct(
            id=watched_point_id,
            vector=[0.0] * 384,
            payload=payload
        )
        
        client.upsert(
            collection_name="clan_videos",
            points=[point]
        )
        
        logger.info(f"Saved video {video_id} for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error saving watched video: {e}", exc_info=True)
        # Don't crash, just log error

# ════════════════════════════════════════════════════════════════════════════════
# 21-DAY NOTIFICATION TEMPLATES
# ════════════════════════════════════════════════════════════════════════════════

NOTIFICATION_TEMPLATES = {
    # PHASE 1: Days 1-7 (Awareness & Curiosity)
    1: {
        "type": "welcome",
        "has_video": False,
        "title": "Welcome to Clan — India's first community of top performers",
        "body": "{user_name}, join thousands of top performers who use Clan to win at work. Let's start your journey!",
    },
    2: {
        "type": "video_recommendation",
        "has_video": True,
        "title": "Today's 2-minute tip from a top performer near you",
        "body": "{user_name}, watch this quick insight from {creator_name} on how to improve {indicator}",
    },
    3: {
        "type": "self_discovery",
        "has_video": False,
        "title": "Compare your effort vs top performers",
        "body": "{user_name}, see how your effort stacks up. Check your performance snapshot and discover what you're great at!",
    },
    4: {
        "type": "peer_learning",
        "has_video": True,
        "title": "Top RM from your region shares how he closes leads faster",
        "body": "{user_name}, {creator_name} from {region} reveals their secret. Watch and share your thoughts!",
    },
    5: {
        "type": "challenge",
        "has_video": False,
        "title": "Can you beat yesterday's effort?",
        "body": "{user_name}, yesterday you logged 5 activities. Can you log 6 today? Challenge accepted?",
    },
    6: {
        "type": "social_proof",
        "has_video": False,
        "title": "87% of your team logged in today",
        "body": "{user_name}, your team is crushing it! Join the momentum and log 1 activity today.",
    },
    7: {
        "type": "weekly_celebration",
        "has_video": False,
        "title": "You completed 7 days on Clan",
        "body": "{user_name}, you're a legend! You've logged into Clan for a full week. That's the winning habit we want!",
    },
    # PHASE 2: Days 8-14 (Engagement)
    8: {
        "type": "interactive_engagement",
        "has_video": False,
        "title": "Today's question: What works best for lead conversion?",
        "body": "{user_name}, share your winning strategy in today's poll. Your peers want to learn from you!",
    },
    9: {
        "type": "supervisor_push",
        "has_video": True,
        "title": "Your supervisor recommended video for you!",
        "body": "{user_name}, your supervisor picked {video_title} just for you. Watch and impress them!",
    },
    10: {
        "type": "challenge",
        "has_video": True,
        "title": "Top performer challenge — try this method today",
        "body": "{user_name}, {creator_name} shares a game-changing technique. Try it today and report back!",
    },
    11: {
        "type": "leaderboard_movement",
        "has_video": False,
        "title": "You moved up on the leaderboard",
        "body": "{user_name}, congratulations! You climbed 3 spots this week. Keep the momentum going!",
    },
    12: {
        "type": "new_video_alert",
        "has_video": True,
        "title": "New video from someone you know!",
        "body": "{user_name}, {creator_name} just dropped a fresh video on {indicator}. Don't miss it!",
    },
    13: {
        "type": "feedback",
        "has_video": False,
        "title": "Give your Feedback on Clan, what more do you want",
        "body": "{user_name}, your voice matters! Tell us what features you'd love to see on Clan.",
    },
    14: {
        "type": "weekly_celebration",
        "has_video": False,
        "title": "Two weeks on Clan",
        "body": "{user_name}, you've been with us for 14 days! The habits are forming. You're unstoppable!",
    },
    # PHASE 3: Days 15-21 (Habit Formation)
    15: {
        "type": "darts_habit",
        "has_video": False,
        "title": "Check your effort vs target",
        "body": "{user_name}, you're {metric_value}% towards your weekly target. Check your performance snapshot and push harder!",
    },
    16: {
        "type": "performance_insights",
        "has_video": False,
        "title": "Your effort improved 10% this week",
        "body": "{user_name}, amazing progress! Your activity logging improved by 10%. The habit is sticking!",
    },
    17: {
        "type": "help_team",
        "has_video": False,
        "title": "Help someone in your team",
        "body": "{user_name}, {team_member} is struggling. Share a video with them and help them level up!",
    },
    18: {
        "type": "supervisor_challenge",
        "has_video": False,
        "title": "Supervisor challenge announced",
        "body": "{user_name}, your supervisor just announced a challenge for this week. Sign up and show them what you've got!",
    },
    19: {
        "type": "leader_message",
        "has_video": True,
        "title": "Watch this message from your leader today!",
        "body": "{user_name}, your leader {creator_name} has a special message just for you on {indicator}.",
    },
    20: {
        "type": "inspirational_story",
        "has_video": True,
        "title": "Watch this inspirational story",
        "body": "{user_name}, {creator_name} was once where you are. See how they climbed to the top!",
    },
    21: {
        "type": "cbo_message",
        "has_video": True,
        "title": "CBO message on Clan! Use to Win at Work!",
        "body": "{user_name}, your CBO has a final message. Watch it and commit to winning at work!",
    },
}

# ════════════════════════════════════════════════════════════════════════════════
# REQUEST/RESPONSE MODELS
# ════════════════════════════════════════════════════════════════════════════════

class RecommendRequest(BaseModel):
    user_id: int
    user_name: str
    role: str
    region: str
    weak_indicator: str
    user_language: str = "english"
    journey_day: int
    watched_ids: List[int] = []
    months_in_role: Optional[int] = None

    @field_validator("user_name")
    @classmethod
    def validate_user_name(cls, v):
        if not v or not v.strip():
            raise ValueError("user_name cannot be empty")
        return v.strip()

    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        role = str(v).strip().upper()
        if role not in VALID_ROLES:
            raise ValueError("role must be RM, BM, or Supervisor")
        return role

    @field_validator("journey_day")
    @classmethod
    def validate_journey_day(cls, v):
        if v < 1 or v > 31:
            raise ValueError("journey_day must be between 1 and 31")
        return v

    @field_validator("user_language")
    @classmethod
    def validate_user_language(cls, v):
        return normalize_language(v)

    @field_validator("watched_ids")
    @classmethod
    def validate_watched_ids(cls, v):
        if not isinstance(v, list):
            raise ValueError("watched_ids must be a list of integers")
        return [int(x) for x in v]


class RecommendResponse(BaseModel):
    video_id: str
    title: str
    creator_name: str
    summary: str
    key_lesson: str
    problem_solved: str
    sales_phase: str
    experience_level: str
    notification_title: str
    notification_body: str
    score: float
    matched_indicator: str
    language_match_type: str


class NotificationText(BaseModel):
    notification_title: str
    notification_body: str

    @field_validator("notification_title")
    @classmethod
    def validate_title(cls, v):
        text = str(v).strip()
        if not text:
            raise ValueError("notification_title cannot be empty")
        if len(text) > 120:
            raise ValueError("notification_title must be <= 120 characters")
        if len(text.split()) > 12:
            raise ValueError("notification_title must be <= 12 words")
        return text

    @field_validator("notification_body")
    @classmethod
    def validate_body(cls, v):
        text = str(v).strip()
        if not text:
            raise ValueError("notification_body cannot be empty")
        if len(text) > 120:
            raise ValueError("notification_body must be <= 120 characters")
        return text


class IndicatorSyncRequest(BaseModel):
    dry_run: bool = True
    clear_unmapped: bool = False
    limit: Optional[int] = None


class IndicatorSyncResponse(BaseModel):
    dry_run: bool
    account_id: int
    collection: str
    scanned_points: int
    matched_points: int
    updated_points: int
    unchanged_points: int
    skipped_unmapped: int
    db_mapped_content_ids: int
    limit_reached: bool
    samples: list[dict]


class SendNotificationRequest(BaseModel):
    user_id: int
    user_name: str
    weak_indicator: str
    watched_video_ids: list[int] = []
    months_in_role: Optional[int] = None
    campaign_day: int

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError("user_id must be positive")
        return v

    @field_validator("user_name")
    @classmethod
    def validate_user_name(cls, v):
        if not v or not v.strip():
            raise ValueError("user_name cannot be empty")
        return v.strip()

    @field_validator("weak_indicator")
    @classmethod
    def validate_indicator(cls, v):
        if not v or not v.strip():
            raise ValueError("weak_indicator cannot be empty")
        return v.strip().lower().replace(" ", "_")

    @field_validator("campaign_day")
    @classmethod
    def validate_campaign_day(cls, v):
        if v < 1 or v > 21:
            raise ValueError("campaign_day must be between 1 and 21")
        return v


class SimpleNotificationRequest(BaseModel):
    user_id: int
    campaign_day: int = 2
    weak_indicator_override: Optional[str] = None

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError("user_id must be positive")
        return v

    @field_validator("campaign_day")
    @classmethod
    def validate_campaign_day(cls, v):
        if v < 1 or v > 21:
            raise ValueError("campaign_day must be between 1 and 21")
        return v


class NotificationObject(BaseModel):
    campaign_day: int
    notification_title: str
    notification_body: str
    audience_strategy: Optional[str] = None
    cohort_key: Optional[str] = None
    video_title: Optional[str] = None
    creator_name: Optional[str] = None
    action: Optional[str] = None
    deep_link: Optional[str] = None
    notification_type: Optional[str] = None
    should_send: Optional[bool] = None


class SendNotificationResponse(BaseModel):
    success: bool
    user_id: int
    notification: NotificationObject
    test_file_path: Optional[str] = None
    error: Optional[str] = None
    remote_send_status: Optional[str] = None
    remote_send_response: Optional[dict] = None


# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

def _normalize_indicator_code(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        text = str(item or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _build_problem_keywords(code: str, name: str) -> list[str]:
    text = f"{name} {code.replace('_', ' ')}".lower()
    tokens = re.findall(r"[a-z0-9]+", text)
    return _dedupe_keep_order([name.lower().strip(), code.replace("_", " ")] + tokens)


def _load_indicator_configuration() -> None:
    global _INDICATOR_CONFIG_LOADED, VALID_INDICATORS
    if _INDICATOR_CONFIG_LOADED:
        return

    try:
        db_indicators = PostgresConfig(account_id=ACCOUNT_ID).get_indicators()
        if not db_indicators:
            raise RuntimeError(f"No active KIIs found for account_id={ACCOUNT_ID}")
        logger.info("Loaded %s indicators from PostgreSQL for account_id=%s", len(db_indicators), ACCOUNT_ID)
    except Exception as exc:
        logger.error("Indicator DB fetch failed: %s", exc)
        raise RuntimeError(f"Unable to load active KIIs for account_id={ACCOUNT_ID}") from exc

    INDICATOR_LABELS.clear()
    INDICATOR_PROBLEM_KEYWORDS.clear()

    for ind in db_indicators:
        code = _normalize_indicator_code(ind.get("code"))
        if not code:
            continue

        name = str(ind.get("name") or code.replace("_", " ").title())
        INDICATOR_LABELS[code] = name
        INDICATOR_PROBLEM_KEYWORDS[code] = _build_problem_keywords(code, name)

    VALID_INDICATORS = set(INDICATOR_LABELS.keys())
    _INDICATOR_CONFIG_LOADED = True
    logger.info("Indicator configuration ready | indicators=%s", len(VALID_INDICATORS))


def infer_sales_phase(journey_day: int) -> str:
    return "acquisition" if journey_day <= 15 else "conversion"


def infer_experience_level(months_in_role: Optional[int]) -> str:
    if months_in_role is None:
        return "all"
    if months_in_role < 3:
        return "new_joiner"
    if months_in_role <= 12:
        return "experienced"
    return "senior"


def score_problem_match(weak_indicator: str, payload: dict) -> float:
    keywords = INDICATOR_PROBLEM_KEYWORDS.get(weak_indicator, [])
    haystack = " ".join(
        [
            str(payload.get("problem_solved", "")),
            str(payload.get("key_lesson", "")),
            str(payload.get("summary", "")),
        ]
    ).lower()
    if not keywords:
        return 0.0
    hits = sum(1 for kw in keywords if kw in haystack)
    return min(hits / max(len(keywords), 1), 1.0)


def score_intent_match(weak_indicator: str, payload: dict) -> float:
    haystack = " ".join(
        [
            str(payload.get("title", "")),
            str(payload.get("problem_solved", "")),
            str(payload.get("key_lesson", "")),
            str(payload.get("summary", "")),
        ]
    ).lower()

    keywords = INDICATOR_PROBLEM_KEYWORDS.get(weak_indicator, [])
    if not keywords:
        return 0.0

    hits = sum(1 for kw in keywords if len(kw) > 3 and kw in haystack)
    return min(hits * 0.08, 0.4)


def get_language_match(video_language: str, user_language: str) -> tuple[str, int, float]:
    video_lang = normalize_language(video_language)
    user_lang = normalize_language(user_language)

    if video_lang == user_lang:
        return "exact", 0, 0.30
    if video_lang == "english":
        return "english_fallback", 1, 0.18
    return "other_fallback", 2, 0.05


def _get_all_video_ids() -> set[int]:
    return get_all_video_ids()


def _find_unknown_watched_ids(watched_ids: list[int]) -> list[int]:
    if not watched_ids:
        return []

    valid_ids = _get_all_video_ids()
    return sorted([vid for vid in watched_ids if vid not in valid_ids])


def _extract_reference_id(notification: dict) -> str:
    deep_link = str(notification.get("deep_link", "")).strip()
    if deep_link:
        match = re.search(r"/watch/(\d+)", deep_link)
        if match:
            return match.group(1)
    return "0"


def _fetch_user_notification_params(user_id: int, weak_indicator_override: Optional[str] = None) -> dict:
    try:
        user_details = get_user_details(user_id)
        if not user_details:
            return {
                'error': f"User {user_id} not found in database",
                'user_id': user_id,
            }

        user_name = str(user_details.get("name", f"User {user_id}")).strip()

        if weak_indicator_override:
            weak_indicator = str(weak_indicator_override).strip().lower().replace(" ", "_")
            logger.info("Weak indicator override used | user_id=%s | indicator=%s", user_id, weak_indicator)
        else:
            try:
                from database.db_config import engine as db_engine
                weak_indicator = get_weak_indicator(db_engine, user_id)
                if not weak_indicator:
                    weak_indicator = "customer_generation"
                logger.info("Weak indicator fetched from DB | user_id=%s | indicator=%s", user_id, weak_indicator)
            except Exception as exc:
                logger.warning("Failed to fetch weak indicator from DB | user_id=%s | error=%s | using default", user_id, exc)
                weak_indicator = "customer_generation"

        months_in_role = None
        if user_details.get("profile_activation_date"):
            try:
                activation_date = user_details.get("profile_activation_date")
                if isinstance(activation_date, str):
                    activation_date = datetime.fromisoformat(activation_date)
                months_in_role = (datetime.now() - activation_date).days // 30
                if months_in_role < 0:
                    months_in_role = 0
            except Exception as exc:
                logger.warning("Failed to calculate months_in_role | user_id=%s | error=%s", user_id, exc)
                months_in_role = None

        return {
            'user_id': user_id,
            'user_name': user_name,
            'weak_indicator': weak_indicator,
            'months_in_role': months_in_role,
            'error': None,
        }

    except Exception as exc:
        logger.exception("Error fetching user notification params | user_id=%s", user_id)
        return {
            'error': f"Failed to fetch user parameters: {str(exc)}",
            'user_id': user_id,
        }


def _forward_to_remote_bulk_sender(user_id: int, notification: dict) -> dict:
    notification_type = str(notification.get("notification_type") or REMOTE_NOTIFICATION_TYPE).strip() or REMOTE_NOTIFICATION_TYPE
    deep_link = str(notification.get("deep_link", "")).strip()

    payload = [
        {
            "user_id": int(user_id),
            "title": str(notification.get("notification_title", "")).strip(),
            "description": str(notification.get("notification_body", "")).strip(),
            "notification_type": notification_type,
            "reference_id": _extract_reference_id(notification),
            "video_popup": "Y" if deep_link else "N",
        }
    ]

    try:
        with httpx.Client(timeout=REMOTE_NOTIFICATION_TIMEOUT_SECONDS) as client:
            response = client.post(REMOTE_NOTIFICATION_SEND_URL, json=payload)
    except httpx.TimeoutException as exc:
        logger.error("Remote notification sender timed out | url=%s | timeout=%s", REMOTE_NOTIFICATION_SEND_URL, REMOTE_NOTIFICATION_TIMEOUT_SECONDS)
        raise HTTPException(
            status_code=504,
            detail={
                "message": "Remote notification sender timed out",
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
                "timeout_seconds": REMOTE_NOTIFICATION_TIMEOUT_SECONDS,
            },
        ) from exc
    except httpx.HTTPError as exc:
        logger.error("Remote notification sender request failed | url=%s | error=%s", REMOTE_NOTIFICATION_SEND_URL, exc)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Remote notification sender request failed",
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
                "error": str(exc),
            },
        ) from exc

    try:
        response_body = response.json()
    except Exception:
        response_body = {"raw": response.text}

    if response.status_code >= 400:
        logger.error("Remote notification sender error | status=%s | user_id=%s | response=%s", response.status_code, user_id, response_body)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Remote notification sender returned an error",
                "remote_status_code": response.status_code,
                "remote_response": response_body,
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
            },
        )

    logger.info("Notification forwarded to remote sender | user_id=%s | status_code=%s", user_id, response.status_code)

    return {
        "status_code": response.status_code,
        "body": response_body,
        "payload": payload,
    }
def _is_generic_notification(text: str) -> bool:
    """Check if notification text is too generic"""
    generic_patterns = [
        r"\bwatch\b",
        r"\bcheck out\b",
        r"\bcheck it out\b",
        r"\bwatch this\b",
        r"\bhelp you improve\b",
        r"\blearn more\b",
    ]
    lowered = text.lower()
    return any(re.search(p, lowered) for p in generic_patterns)
def _generate_notification(req: RecommendRequest, weak: str, best: dict):
    """Simple notification without LangChain"""
    title = f"Quick tip from {best['creator']}"
    body = f"{req.user_name}, improve {weak.replace('_', ' ')} with this video"
    return title, body
def get_recommendation(req: RecommendRequest):
    _load_indicator_configuration()
    weak = req.weak_indicator
    query_text = f"how to improve {weak} for {req.role} in {req.region}"
    query_vector = model.encode(query_text).tolist()
    user_sales_phase = infer_sales_phase(req.journey_day)
    user_experience_level = infer_experience_level(req.months_in_role)

    results = query_points(
        query_vector=query_vector,
        limit=30,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="lead_indicators",
                    match=MatchValue(value=weak)
                )
            ]
        ),
    )

    if not results:
        results = query_points(
            query_vector=query_vector,
            limit=30,
        )
    scored = [] 
    for r in results:
        p = r.payload
        logger.info(f"DEBUG: Payload keys = {list(p.keys())}")
        logger.info(f"DEBUG: video_id = {p.get('video_id')}, id = {p.get('id')}")
            # ← SKIP WATCHED VIDEO RECORD!
        if 'watched_video_ids' in p and 'user_id' in p:
            logger.info(f"Skipping watched video record for user {p.get('user_id')}")
            continue
        # Get video metadata
        video_phase = str(p.get("sales_phase", "all")).lower()
        video_language = str(p.get("language", p.get("language_name", "en"))).lower()
        video_region = str(p.get("creator_region", "all")).lower()
        
        # NO HARD GATES - let scoring handle filtering
        # But we'll use these in scoring!
        
        base_score = r.score
        
        # ── SCORING ──────────────────────────────────────────
        indicators = p.get("lead_indicators", [])
        indicator_match = 1.0 if (not indicators or weak in indicators) else 0.0
        
        # Sales phase scoring (prefer matching phase)
        if video_phase == user_sales_phase:
            sales_phase_match = 1.0
        elif video_phase == "all":
            sales_phase_match = 0.5
        else:
            sales_phase_match = 0.1  # Still usable, just lower score
        
        # Language scoring (prefer user's language)
        video_lang_normalized = normalize_language(video_language)
        user_lang_normalized = normalize_language(req.user_language)
        if video_lang_normalized == user_lang_normalized:
            language_match = 1.0
        elif video_lang_normalized == "english":
            language_match = 0.7  # English is acceptable fallback
        else:
            language_match = 0.3  # Other languages still usable
        
        # Region scoring (prefer matching region)
        if video_region.lower() == "all" or req.region.lower() == "all":
            region_match = 0.5  # Generic region is acceptable
        elif video_region.lower() == req.region.lower():
            region_match = 1.0  # Exact match
        else:
            region_match = 0.2  # Different region, still usable
        
        # Rest of scoring...
        experience_match = 1.0 if str(p.get("experience_level", "all")).lower() == user_experience_level else 0.5
        problem_match = score_problem_match(weak, p)
        intent_match = score_intent_match(weak, p)
        recency_penalty = 0.15 if int(p.get("video_id", 0)) in req.watched_ids else 0.0
        language_match_type, language_rank, language_boost = get_language_match(
            p.get("language", p.get("language_name", "english")),
            req.user_language,
        )
        language_boost = language_boost * 0.1
        
        final_score = (
            (base_score * 0.20)
            + (indicator_match * 0.25)
            + (problem_match * 0.15)
            + (experience_match * 0.15)
            + (sales_phase_match * 0.15)  # ← SALES PHASE SCORING!
            + (language_match * 0.08)      # ← LANGUAGE SCORING!
            + (region_match * 0.02)        # ← REGION SCORING!
            + (intent_match * 0.15)
            + language_boost
            - recency_penalty
        )
        
        scored.append({
            'video_id':    p.get('video_id'),
            'title':       p.get('title'),
            'creator':     p.get('creator_name'),
            'creator_region': p.get('creator_region', 'all'),  # ← ADD THIS!
            'language':    p.get('language', 'en'),  # ← ADD THIS!
            'indicators':  p.get('lead_indicators'),
            'summary':     p.get('summary', ''),
            'key_lesson':  p.get('key_lesson', ''),
            'problem_solved': p.get('problem_solved', ''),
            'sales_phase': p.get('sales_phase', 'all'),
            'experience_level': p.get('experience_level', 'all'),
            'language_match_type': language_match_type,
            'final_score': round(final_score, 3),
        })
    scored.sort(key=lambda x: -x['final_score'])

    total_candidates = len(scored)
    phase_filtered = [
        v for v in scored
        if str(v['sales_phase']).lower() in {"all", user_sales_phase}
    ]
    if phase_filtered:
        scored = phase_filtered
        logger.info(
            "Sales-phase gate applied | phase=%s | candidates_before=%s | candidates_after=%s",
            user_sales_phase,
            total_candidates,
            len(phase_filtered),
        )
    else:
        logger.warning(
            "No videos found for sales_phase=%s — using full pool as fallback",
            user_sales_phase,
        )

    exp_before = len(scored)
    exp_filtered = [
        v for v in scored
        if str(v['experience_level']).lower() in {"all", user_experience_level}
    ]
    if exp_filtered:
        scored = exp_filtered
        logger.info(
            "Experience gate applied | level=%s | candidates_before=%s | candidates_after=%s",
            user_experience_level,
            exp_before,
            len(exp_filtered),
        )
    else:
        logger.warning(
            "No videos found for experience_level=%s — using full pool as fallback",
            user_experience_level,
        )

    if not scored:
        logger.warning("No valid videos available after filtering excluded content")
        raise HTTPException(status_code=404, detail="No video found for this indicator (all candidates excluded)")

    best = scored[0]
    # ← ADD THESE DEBUG LINES!
    logger.info(f"DEBUG: best dict keys = {list(best.keys())}")
    logger.info(f"DEBUG: best['video_id'] = {best.get('video_id')}")
    logger.info(f"DEBUG: best['title'] = {best.get('title')}")
    logger.info(f"DEBUG: best['creator'] = {best.get('creator')}")
    logger.info(f"DEBUG: FULL best = {best}")

    notif_title, notif_body = _generate_notification(req, weak, best)
    logger.info(
        "Recommendation selected | user_id=%s | weak=%s | video_id=%s | score=%s",
        req.user_id,
        weak,
        best["video_id"],
        best["final_score"],
    )

    return {
        'video_id':           best['video_id'],
        'title':              best['title'],
        'creator_name':       best['creator'],
        'creator_region':     best.get('creator_region', 'all'),  # ← ADD!
        'language':           best.get('language', 'en'),  # ← ADD!
        'summary':            best['summary'],
        'key_lesson':         best['key_lesson'],
        'problem_solved':     best['problem_solved'],
        'sales_phase':        best['sales_phase'],
        'experience_level':   best['experience_level'],
        'notification_title': notif_title,
        'notification_body':  notif_body,
        'score':              best['final_score'],
        'matched_indicator':  weak,
        'language_match_type': best['language_match_type'],
    }


# ════════════════════════════════════════════════════════════════════════════════
# MIDDLEWARE AND ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════════

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    logger.info("Incoming request | method=%s | path=%s", request.method, request.url.path)
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled API error")
        raise
    elapsed = round(time.time() - start, 3)
    logger.info(
        "Request complete | method=%s | path=%s | status=%s | seconds=%s",
        request.method,
        request.url.path,
        response.status_code,
        elapsed,
    )
    return response


@app.post("/recommend-video", response_model=RecommendResponse)
def recommend_video(req: RecommendRequest):
    unknown_ids = _find_unknown_watched_ids(req.watched_ids)
    if unknown_ids:
        raise HTTPException(
            status_code=422,
            detail=(
                f"watched_ids contain unknown video_id(s): {unknown_ids}. "
                "Use /videos to see valid video IDs."
            ),
        )

    result = get_recommendation(req)
    if not result:
        raise HTTPException(status_code=404, detail="No video found for this indicator")
    return result


@app.post("/notifications/admin/sync-indicators", response_model=IndicatorSyncResponse)
def sync_indicators(req: IndicatorSyncRequest):
    try:
        stats = sync_qdrant_payload_from_postgres(
            account_id=ACCOUNT_ID,
            dry_run=req.dry_run,
            clear_unmapped=req.clear_unmapped,
            limit=req.limit,
        )
        logger.info(
            "Indicator sync complete | dry_run=%s | scanned=%s | updated=%s",
            stats.get("dry_run"),
            stats.get("scanned_points"),
            stats.get("updated_points"),
        )
        return IndicatorSyncResponse(**stats)
    except Exception as exc:
        logger.exception("Payload sync failed")
        raise HTTPException(status_code=500, detail=f"Indicator sync failed: {exc}") from exc


@app.get("/videos")
def list_videos():
    results = scroll_points(limit=100)
    videos = []
    for point in results[0]:
        videos.append({
            'video_id': point.payload.get('video_id'),
            'title': point.payload.get('title'),
            'indicators': point.payload.get('lead_indicators'),
            'creator': point.payload.get('creator_name'),
        })
    return {"total": len(videos), "videos": videos}


@app.get("/videos/sync")
def sync_videos():
    results = scroll_points(limit=100)
    videos = [p.payload for p in results[0]]
    return {"total": len(videos), "videos": videos}


@app.get("/indicators")
def list_indicators():
    _load_indicator_configuration()
    return {"indicators": list(INDICATOR_LABELS.keys())}


@app.post("/notifications/send", response_model=SendNotificationResponse)
def send_notification_auto(req: SimpleNotificationRequest):
    """
    Simplified endpoint - Send notifications with ONLY user_id.
    
    Uses get_recommendation() for ALL 21 days!
    """
    try:
        _load_indicator_configuration()
        
        logger.info(
            "Auto-fetch notification request | user_id=%s | campaign_day=%s",
            req.user_id,
            req.campaign_day,
        )
        
        # ─────────────────────────────────────────────────────────
        # STEP 1: Auto-fetch all missing parameters
        # ─────────────────────────────────────────────────────────
        params = _fetch_user_notification_params(req.user_id, req.weak_indicator_override)
        
        if params.get("error"):
            logger.error("Failed to fetch user parameters | user_id=%s | error=%s", req.user_id, params['error'])
            raise HTTPException(status_code=404, detail=params['error'])
        
        user_id = params['user_id']
        user_name = params['user_name']
        print(f"user name: {user_name}")
        weak_indicator = params['weak_indicator']
        months_in_role = params['months_in_role']
        
        logger.info(
            "User parameters fetched | user_id=%s | name=%s | weak_indicator=%s",
            user_id,
            user_name,
            weak_indicator,
        )
        
        # ─────────────────────────────────────────────────────────
        # STEP 2: Get template for this day
        # ─────────────────────────────────────────────────────────
        template = NOTIFICATION_TEMPLATES.get(req.campaign_day)
        if not template:
            raise HTTPException(
                status_code=422,
                detail=f"No template for day {req.campaign_day}",
            )

        has_video = template.get("has_video", False)
        
        logger.info(
            "Day %s template found | has_video=%s | type=%s",
            req.campaign_day,
            has_video,
            template.get("type"),
        )
        
        # ─────────────────────────────────────────────────────────
        # STEP 3: If video day, get recommendation
        # ─────────────────────────────────────────────────────────
        video_recommendation = None

        if has_video:
            try:
                # Get watched videos from Qdrant
                watched_ids = get_user_watched_videos(user_id)
                logger.info(f"Day {req.campaign_day}: User {user_id} watched {len(watched_ids)} videos: {watched_ids}")
                
                rec_req = RecommendRequest(
                    user_id=user_id,
                    user_name=user_name,
                    role="RM",
                    region="all",
                    weak_indicator=weak_indicator,
                    user_language="en",
                    journey_day=req.campaign_day,
                    watched_ids=watched_ids,
                    months_in_role=months_in_role,
                )

                logger.info(f"Calling get_recommendation...")
                video_result = get_recommendation(rec_req)
                logger.info(f"video_result returned: {video_result}")
                
                if video_result:
                    logger.info(f"✓ video_result is NOT null")
                    video_id = video_result.get('video_id')
                    logger.info(f"video_id = {video_id}")
                    if video_id is None:
                        logger.warning(f"❌ video_id is NULL even though video_result exists!")
                        video_recommendation = None
                    else:
                    # SAVE WATCHED VIDEO
                        save_watched_video(user_id, int(video_id), req.campaign_day)
                    
                    # Get region and language from video
                        creator_region = video_result.get('creator_region', 'all')
                        video_language = video_result.get('language', 'en')
                        
                        video_recommendation = {
                            "video_title": video_result.get("title"),
                            "creator_name": video_result.get("creator_name"),
                            "deep_link": f"/watch/{video_id}",
                            "creator_region": creator_region,
                            "language": video_language,
                        }
                        logger.info(f"✓ Video recommendation: region={creator_region}, language={video_language}")
                else:
                    logger.warning(f"❌ video_result is NULL!")
                    video_recommendation = None
                    
            except Exception as e:
                logger.error(f"❌ Error in video section: {e}", exc_info=True)
                video_recommendation = None
        
        # ─────────────────────────────────────────────────────────
        # STEP 4: Build notification
        # ─────────────────────────────────────────────────────────
        context = {
            "user_name": user_name,
            "creator_name": video_recommendation.get("creator_name", "Expert") if video_recommendation else "Expert",
            "indicator": weak_indicator.replace("_", " ").title(),
            "region": video_recommendation.get("creator_region", "all") if video_recommendation else "all",
            "video_title": video_recommendation.get("video_title", "") if video_recommendation else "",
            "metric_value": "85",
            "team_member": "John",
        }

        notification_title = template["title"].format(**context)
        notification_body = template["body"].format(**context)
        
        notification_obj = {
            "campaign_day": req.campaign_day,
            "notification_title": notification_title,
            "notification_body": notification_body,
            "notification_type": template.get("type", "general"),
            "video_title": video_recommendation.get("video_title") if video_recommendation else None,
            "creator_name": video_recommendation.get("creator_name") if video_recommendation else None,
            "deep_link": video_recommendation.get("deep_link") if video_recommendation else None,
            "should_send": True,
        }

        logger.info(
            "✅ Notification built | day=%s | user=%s | type=%s | has_video=%s | title=%s",
            req.campaign_day,
            user_id,
            template.get("type"),
            has_video,
            notification_title[:60],
        )

        # ─────────────────────────────────────────────────────────
        # STEP 5: Forward to remote bulk sender
        # ─────────────────────────────────────────────────────────
        remote_result = _forward_to_remote_bulk_sender(
            user_id=user_id,
            notification=notification_obj,
        )

        # ─────────────────────────────────────────────────────────
        # STEP 6: Return response
        # ─────────────────────────────────────────────────────────
        response = SendNotificationResponse(
            success=True,
            user_id=user_id,
            notification=NotificationObject(**notification_obj),
            test_file_path=None,
            remote_send_status="sent",
            remote_send_response={
                "status_code": remote_result.get("status_code"),
                "response": remote_result.get("body"),
                "request_payload": remote_result.get("payload"),
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
            },
        )

        logger.info(
            "✅ 21-day notification sent successfully | day=%s | user=%s | type=%s | has_video=%s",
            req.campaign_day,
            user_id,
            template.get("type"),
            has_video,
        )

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("❌ Error in 21-day notification | user_id=%s | day=%s | error=%s", req.user_id, req.campaign_day, str(exc))
        raise HTTPException(status_code=500, detail=f"Internal error: {str(exc)}") from exc