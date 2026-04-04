import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import List, Optional
import httpx

from fastapi import FastAPI, HTTPException, Request
import requests
# from apscheduler.schedulers.background import BackgroundScheduler
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from whisper import model
from constants import ACCOUNT_ID, is_excluded_video, normalize_language
from notifications.models import NotificationRequest as CampaignNotificationRequest
from notifications.models import NotificationResponse as CampaignNotificationResponse
from notifications.models import BatchNotificationRequest as CampaignBatchNotificationRequest
from notifications.models import BatchNotificationResponse as CampaignBatchNotificationResponse
from notifications.service import NotificationService
from qdrant.indicator_sync import (
    sync_qdrant_payload_from_postgres,
)
from qdrant.query import get_all_video_ids, query_points, scroll_points
from sentence_transformers import SentenceTransformer
from qdrant_client.models import Filter, FieldCondition, MatchValue
from notifications.database_config import PostgresConfig
from database.db_config import SessionLocal
from notificationschema.resolver import NotificationResolver
from send_notification_to_user import get_user_details
from weak_indicator import get_weak_indicator

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

# Background scheduler for automated sync
# scheduler = BackgroundScheduler(daemon=True)

NOTIFICATION_API_URL = "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications"

def run_hourly_sync():
    """Background job to sync indicators + language from Postgres to Qdrant every hour."""
    try:
        logger.info("Starting automated hourly payload sync...")
        stats = sync_qdrant_payload_from_postgres(
            account_id=ACCOUNT_ID,
            dry_run=False,
            clear_unmapped=False,
            limit=None,
        )
        logger.info(
            "Automated sync complete | scanned=%s | updated=%s | unchanged=%s | skipped=%s",
            stats.get("scanned_points"),
            stats.get("updated_points"),
            stats.get("unchanged_points"),
            stats.get("skipped_unmapped"),
        )
    except Exception as exc:
        logger.exception("Automated sync failed: %s", exc)

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     """Manage app startup and shutdown."""
#     # Startup
#     logger.info("Starting FastAPI app with background indicator sync scheduler (hourly)")
#     scheduler.add_job(run_hourly_sync, "interval", hours=1, id="sync_indicators_hourly")
#     scheduler.start()
#     yield
#     # Shutdown
#     logger.info("Shutting down, stopping scheduler...")
#     scheduler.shutdown()

app = FastAPI(title="CLAN Video Recommendation API")
model = SentenceTransformer("all-MiniLM-L6-v2")
# notification_service = NotificationService()
notification_resolver = NotificationResolver()
REMOTE_NOTIFICATION_SEND_URL = os.getenv(
    "REMOTE_NOTIFICATION_SEND_URL",
    "https://clantesting.quantapeople.com/clantestapi/notifications/send_notifications",
).strip()
REMOTE_NOTIFICATION_TYPE = os.getenv("REMOTE_NOTIFICATION_TYPE", "video_recommendation").strip()
REMOTE_NOTIFICATION_TIMEOUT_SECONDS = float(
    os.getenv("REMOTE_NOTIFICATION_TIMEOUT_SECONDS", "10")
)

# ── INDICATOR CONFIGURATION (Dynamic) ─────────────────
INDICATOR_LABELS = {}
INDICATOR_PROBLEM_KEYWORDS = {}
VALID_INDICATORS = set()
_INDICATOR_CONFIG_LOADED = False


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

VALID_ROLES = {"RM", "BM", "SUPERVISOR"}
VALID_LANGUAGE_MATCH_TYPES = {"exact", "english_fallback", "other_fallback"}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# intent matching logic will now use the accumulated CUSTOMER_GENERATION_INTENT_KEYWORDS


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
    """Read all indexed video_id values from Qdrant payloads."""
    return get_all_video_ids()


def _find_unknown_watched_ids(watched_ids: list[int]) -> list[int]:
    if not watched_ids:
        return []

    valid_ids = _get_all_video_ids()
    return sorted([vid for vid in watched_ids if vid not in valid_ids])

# ── REQUEST MODEL ──────────────────────────────────────
class RecommendRequest(BaseModel):
    user_id:        int
    user_name:      str
    role:           str
    region:         str
    weak_indicator: str
    user_language:  str = "english"
    journey_day:    int
    watched_ids:    List[int] = []
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

    @field_validator("weak_indicator")
    @classmethod
    def validate_indicator(cls, v):
        _load_indicator_configuration()
        if not VALID_INDICATORS:
            raise ValueError(f"No active lead indicators available in DB for account_id={ACCOUNT_ID}")
        weak = str(v).strip().lower().replace(" ", "_")
        if weak not in VALID_INDICATORS:
            raise ValueError(f"weak_indicator must be one of: {sorted(VALID_INDICATORS)}")
        return weak

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

# ── RESPONSE MODEL ─────────────────────────────────────
class RecommendResponse(BaseModel):
    video_id:             str
    title:                str
    creator_name:         str
    summary:              str
    key_lesson:           str
    problem_solved:       str
    sales_phase:          str
    experience_level:     str
    notification_title:   str
    notification_body:    str
    score:                float
    matched_indicator:    str
    language_match_type:  str

    @field_validator("language_match_type")
    @classmethod
    def validate_language_match_type(cls, v):
        value = str(v).strip().lower()
        if value not in VALID_LANGUAGE_MATCH_TYPES:
            raise ValueError("language_match_type must be exact, english_fallback, or other_fallback")
        return value


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
    """Request model for sending Day 2 notifications to users."""
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
        if v < 1 or v > 31:
            raise ValueError("campaign_day must be between 1 and 31")
        return v


class SimpleNotificationRequest(BaseModel):
    """Simplified request model - only requires user_id, auto-fetches everything else."""
    user_id: int
    campaign_day: int = 2
    weak_indicator_override: Optional[str] = None  # Optional override for weak indicator
    
    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError("user_id must be positive")
        return v

    @field_validator("campaign_day")
    @classmethod
    def validate_campaign_day(cls, v):
        if v < 1 or v > 31:
            raise ValueError("campaign_day must be between 1 and 31")
        return v


class NotificationObject(BaseModel):
    """Notification object returned from resolver."""
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
    """Response model for send notifications endpoint."""
    success: bool
    user_id: int
    notification: NotificationObject
    test_file_path: Optional[str] = None
    error: Optional[str] = None
    remote_send_status: Optional[str] = None
    remote_send_response: Optional[dict] = None


notification_parser = PydanticOutputParser(pydantic_object=NotificationText)
notification_chain = None


def _build_notification_chain():
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY missing; notifications will use fallback template")
        return None

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        google_api_key=GEMINI_API_KEY,
        temperature=0.2,
    )

    prompt = PromptTemplate(
        template="""
You generate short, personalized push notifications for a sales training app.

User Name: {user_name}
Weak Indicator: {weak_indicator_readable}
Video Title: {video_title}
Creator: {creator_name}
Key Lesson: {key_lesson}
Problem Solved: {problem_solved}
Summary: {summary}

Rules:
- Keep title <= 12 words and <= 120 chars.
- Keep body <= 120 chars.
- Body must include user name exactly as given.
- Avoid generic lines like "watch this video".
- Mention a specific insight from key_lesson or problem_solved.

{format_instructions}

Return JSON only.
""",
        input_variables=[
            "user_name",
            "weak_indicator_readable",
            "video_title",
            "creator_name",
            "key_lesson",
            "problem_solved",
            "summary",
        ],
        partial_variables={"format_instructions": notification_parser.get_format_instructions()},
    )
    return prompt | llm | notification_parser


notification_chain = _build_notification_chain()


def _fallback_notification(user_name: str, weak: str, best: dict):
    _load_indicator_configuration()
    readable = INDICATOR_LABELS.get(weak, weak.replace("_", " "))
    title = f"{user_name}, improve {readable} today"
    insight = str(best.get("key_lesson", "")).strip()
    if insight:
        insight = insight.split(".")[0].strip()
    if not insight:
        insight = f"a practical method for {readable}"

    body = f"{user_name}, {best['creator']} shares {insight}"

    # Enforce length caps for downstream clients.
    if len(title) > 120:
        title = title[:117].rstrip() + "..."
    if len(body) > 120:
        body = body[:117].rstrip() + "..."
    return title, body


def _is_generic_notification(text: str) -> bool:
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
    if notification_chain is None:
        return _fallback_notification(req.user_name, weak, best)

    readable = INDICATOR_LABELS.get(weak, weak.replace("_", " "))
    key_lesson = str(best.get("key_lesson", "")).strip()
    problem_solved = str(best.get("problem_solved", "")).strip()

    for attempt in range(1, 4):
        try:
            out = notification_chain.invoke(
                {
                    "user_name": req.user_name,
                    "weak_indicator_readable": readable,
                    "video_title": best["title"],
                    "creator_name": best["creator"],
                    "key_lesson": key_lesson,
                    "problem_solved": problem_solved,
                    "summary": str(best.get("summary", ""))[:500],
                }
            )

            title = out.notification_title.strip()
            body = out.notification_body.strip()

            if req.user_name.lower() not in body.lower():
                raise ValueError("Body must include user name")
            if _is_generic_notification(body):
                raise ValueError("Body is too generic")

            insight_tokens = [tok for tok in re.findall(r"[a-zA-Z]+", (key_lesson + " " + problem_solved).lower()) if len(tok) > 4]
            if insight_tokens:
                insight_match = any(tok in body.lower() for tok in insight_tokens[:8])
                if not insight_match:
                    raise ValueError("Body must mention specific insight")

            logger.info("Notification generated via LangChain | attempt=%s", attempt)
            return title, body

        except Exception as exc:
            logger.warning("Notification generation failed | attempt=%s | error=%s", attempt, exc)

    logger.info("Notification fallback used after retries")
    return _fallback_notification(req.user_name, weak, best)


def _extract_reference_id(notification: dict) -> str:
    """Extract video reference ID required by downstream bulk sender."""
    deep_link = str(notification.get("deep_link", "")).strip()
    if deep_link:
        match = re.search(r"/watch/(\d+)", deep_link)
        if match:
            return match.group(1)
    return "0"


def _fetch_user_notification_params(user_id: int, weak_indicator_override: Optional[str] = None) -> dict:
    """
    Auto-fetch all required parameters for sending notification.
    
    Args:
        user_id: User ID to fetch data for
        weak_indicator_override: Optional override for weak indicator
    
    Returns:
        dict with: user_id, user_name, weak_indicator, months_in_role, error (if any)
    """
    try:
        # Fetch user details from PostgreSQL
        user_details = get_user_details(user_id)
        if not user_details:
            return {
                'error': f"User {user_id} not found in database",
                'user_id': user_id,
            }
        
        user_name = str(user_details.get("name", f"User {user_id}")).strip()
        
        # Get weak indicator: use override if provided, otherwise fetch from DB
        if weak_indicator_override:
            weak_indicator = str(weak_indicator_override).strip().lower().replace(" ", "_")
            logger.info("Weak indicator override used | user_id=%s | indicator=%s", user_id, weak_indicator)
        else:
            try:
                from database.db_config import engine as db_engine
                weak_indicator = get_weak_indicator(db_engine, user_id)
                if not weak_indicator:
                    weak_indicator = "customer_generation"  # Safe default
                logger.info("Weak indicator fetched from DB | user_id=%s | indicator=%s", user_id, weak_indicator)
            except Exception as exc:
                logger.warning("Failed to fetch weak indicator from DB | user_id=%s | error=%s | using default", user_id, exc)
                weak_indicator = "customer_generation"
        
        # Calculate months_in_role from user's profile_activation_date if available
        months_in_role = None
        if user_details.get("profile_activation_date"):
            try:
                from datetime import datetime as dt
                activation_date = user_details.get("profile_activation_date")
                if isinstance(activation_date, str):
                    activation_date = dt.fromisoformat(activation_date)
                months_in_role = (dt.now() - activation_date).days // 30
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
    """
    Forward built notification to deployed bulk sender endpoint.
    
    Args:
        user_id: User ID to send notification to
        notification: Notification object dict containing:
            - notification_title
            - notification_body
            - notification_type (optional)
            - deep_link (optional)
    
    Returns:
        dict with:
            - status_code: HTTP response status
            - body: Response body (JSON dict or raw text)
            - payload: The payload that was sent
    
    Raises:
        HTTPException: If request fails or returns error status
    """
    notification_type = str(notification.get("notification_type") or REMOTE_NOTIFICATION_TYPE).strip() or REMOTE_NOTIFICATION_TYPE
    deep_link = str(notification.get("deep_link", "")).strip()
    
    # Build payload in the format expected by remote endpoint
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

    # Try to parse response as JSON, fall back to raw text
    try:
        response_body = response.json()
    except Exception:
        response_body = {"raw": response.text}

    # Check for error status codes
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


# ── RECOMMENDATION LOGIC ───────────────────────────────
def get_recommendation(req: RecommendRequest):
    _load_indicator_configuration()
    weak = req.weak_indicator
    query_text = f"how to improve {weak} for {req.role} in {req.region}"
    query_vector = model.encode(query_text).tolist()
    user_sales_phase = infer_sales_phase(req.journey_day)
    user_experience_level = infer_experience_level(req.months_in_role)

    # Try exact indicator match first
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

    # Fallback to semantic search if no exact match
    if not results:
        results = query_points(
            query_vector=query_vector,
            limit=30,
        )

    if not results:
        return None

    # SCORE EACH CANDIDATE
    scored = []
    candidates_before_hard_gates = len(results)
    candidates_after_hard_gates = 0

    for r in results:
        p = r.payload
        video_phase = str(p.get("sales_phase", "all")).lower()
        video_exp = str(p.get("experience_level", "all")).lower()
        
        # Hard Gate: Language
        p_lang = p.get("language", p.get("language_name", "english"))
        video_lang = normalize_language(p_lang)
        if video_lang not in {req.user_language, "english"}:
            logger.debug("Dropping video %s due to language mismatch (video_lang=%s, user_lang=%s)", p.get('video_id'), video_lang, req.user_language)
            continue

        # Hard Gate: Sales Phase
        if video_phase not in {"all", user_sales_phase}:
            logger.debug("Dropping video %s due to sales phase mismatch (video_phase=%s, user_phase=%s)", p.get('video_id'), video_phase, user_sales_phase)
            continue  # Wrong phase, drop it immediately

        # Hard Gate: Experience Level
        if video_exp not in {"all", user_experience_level}:
            logger.debug("Dropping video %s due to experience level mismatch (video_exp=%s, user_exp=%s)", p.get('video_id'), video_exp, user_experience_level)
            continue  # Wrong experience level, drop it immediately
        
        # Skip excluded videos (intro, onboarding, etc)
        if is_excluded_video(p.get('title', '')):
            logger.info("Filtered out excluded video: %s", p.get('title', 'Unknown'))
            continue
        
        base_score = r.score

        indicators = p.get("lead_indicators", [])
        indicator_match = 1.0 if weak in indicators else 0.0

        video_phase = str(p.get("sales_phase", "all")).lower()
        if video_phase == user_sales_phase:
            sales_phase_match = 1.0   # exact match
        elif video_phase == "all":
            sales_phase_match = 0.5   # evergreen — relevant but not targeted
        else:
            sales_phase_match = 0.0   # wrong phase

        video_experience = str(p.get("experience_level", "all")).lower()
        if video_experience == user_experience_level:
            experience_match = 1.0    # exact match (e.g. both "experienced")
        elif video_experience == "all":
            experience_match = 0.5    # evergreen — acceptable but not tailored
        else:
            experience_match = 0.0    # wrong level (e.g. "new_joiner" for an experienced user)

        problem_match = score_problem_match(weak, p)
        intent_match = score_intent_match(weak, p)
        recency_penalty = 0.15 if int(p.get("video_id", 0)) in req.watched_ids else 0.0
        language_match_type, language_rank, language_boost = get_language_match(
            p.get("language", p.get("language_name", "english")),
            req.user_language,
        )
        
        # Scale language boost down drastically so it only serves as a tie-breaker
        # Max language boost used to be 0.30, now 0.03 (exact language gives +0.03)
        language_boost = language_boost * 0.1 

        final_score = (
            (base_score * 0.25)
            + (indicator_match * 0.25)
            + (problem_match * 0.18)
            + (experience_match * 0.35)  # Increased from 0.12 so Exact beats 'all' decisively
            + (sales_phase_match * 0.15) # Increased from 0.10
            + (intent_match * 0.22)
            + language_boost        # folds language preference into score only
            - recency_penalty
        )

        scored.append({
            'video_id':    p.get('video_id'),
            'title':       p.get('title'),
            'creator':     p.get('creator_name'),
            'indicators':  p.get('lead_indicators'),
            'summary':     p.get('summary', ''),
            'key_lesson':  p.get('key_lesson', ''),
            'problem_solved': p.get('problem_solved', ''),
            'sales_phase': p.get('sales_phase', 'all'),
            'experience_level': p.get('experience_level', 'all'),
            'language_match_type': language_match_type,
            'final_score': round(final_score, 3),
        })

    # Sort purely by final_score — language is already baked in via language_boost.
    # No hard primary key on language so a high-scoring video in a fallback language
    # can still beat a low-scoring video in the preferred language.
    scored.sort(key=lambda x: -x['final_score'])

    # ── SALES-PHASE GATE ──────────────────────────────────────────────────────
    # Hard preference: day<=15  → acquisition (lead/customer generation)
    #                  day>15   → conversion  (closing, loan disbursement)
    # First try to restrict to videos that match the user's phase OR are tagged
    # "all" (evergreen). Only fall back to the full pool if no phase-filtered
    # videos survive (e.g. data gap), so we always return something.
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

    # ── EXPERIENCE-LEVEL GATE ─────────────────────────────────────────────────
    # months_in_role < 3          → new_joiner   (basics, first discussions)
    # 3 <= months_in_role <= 12  → experienced   (field tactics, objections)
    # months_in_role > 12        → senior        (advanced, leadership)
    # Videos tagged "all" are always eligible.
    # Only fall back to the full pool if zero experience-filtered videos exist.
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


# ── GET RECOMMENDATION BY USER_ID ─────────────────────
# def get_user_recommendation(user_id: int, weak_indicator: Optional[str] = None) -> dict:
#     """
#     Get recommendation for a user using only user_id.
    
#     This function:
#     1. Fetches user details from PostgreSQL (name, region, role, language, months_in_role)
#     2. Fetches user's weakest KII if weak_indicator not provided
#     3. Calculates journey_day based on current date
#     4. Fetches user's watched videos (if applicable)
#     5. Constructs RecommendRequest
#     6. Executes get_recommendation() to find best video
#     7. Returns complete recommendation with video_id and metadata
    
#     Args:
#         user_id: User ID (int)
#         weak_indicator: Optional weak indicator override. If not provided, queries DB for weakest KII
    
#     Returns:
#         dict with video_id, title, creator_name, summary, key_lesson, problem_solved,
#         sales_phase, experience_level, notification_title, notification_body, score, etc.
    
#     Raises:
#         HTTPException: If user not found, no weak indicator available, or no video found
#     """
#     from datetime import datetime, timedelta
#     from weak_indicator import get_weak_indicator
    
#     logger.info("Getting recommendation for user_id=%s", user_id)
    
#     # Step 1: Fetch user details from PostgreSQL
#     user_details = get_user_details(user_id)
#     if not user_details:
#         logger.error("User not found | user_id=%s", user_id)
#         raise HTTPException(
#             status_code=404,
#             detail=f"User {user_id} not found in database"
#         )
    
#     user_name = str(user_details.get("name", f"User {user_id}")).strip()
#     user_region = str(user_details.get("region", "")).strip() or "unknown"
#     user_role = str(user_details.get("role", "RM")).strip().upper()
#     user_language = normalize_language(str(user_details.get("app_language_id", "en") or "en"))
#     user_created_at = user_details.get("created_at")
    
#     # Validate role
#     if user_role not in VALID_ROLES:
#         user_role = "RM"  # Default to RM if invalid
#         logger.warning("Invalid user role, defaulting to RM | user_id=%s | role=%s", user_id, user_role)
    
#     # Step 2: Get weak indicator (either provided or fetch from DB)
#     if weak_indicator:
#         weak = str(weak_indicator).strip().lower().replace(" ", "_")
#     else:
#         try:
#             from database.db_config import engine as db_engine
#             weak = get_weak_indicator(db_engine, user_id)
#         except Exception as exc:
#             logger.warning("Failed to fetch weak indicator from DB | user_id=%s | error=%s | using default", user_id, exc)
#             weak = "customer_generation"
    
#     # Validate weak_indicator
#     _load_indicator_configuration()
#     if weak not in VALID_INDICATORS:
#         logger.warning("Invalid weak_indicator %s, using customer_generation", weak)
#         weak = "customer_generation"
    
#     # Step 3: Calculate journey_day from created_at or use default
#     try:
#         if user_created_at:
#             journey_day = (datetime.now() - user_created_at).days + 1
#             if journey_day < 1:
#                 journey_day = 1
#             if journey_day > 31:
#                 journey_day = 31  # Cap at 31
#         else:
#             journey_day = 7  # Default to day 7
#     except Exception as exc:
#         logger.warning("Failed to calculate journey_day | user_id=%s | error=%s | using default", user_id, exc)
#         journey_day = 7
    
#     # Step 4: Calculate months_in_role from created_at
#     try:
#         if user_created_at:
#             months_in_role = (datetime.now() - user_created_at).days // 30
#             if months_in_role < 0:
#                 months_in_role = 0
#         else:
#             months_in_role = None
#     except Exception:
#         months_in_role = None
    
#     # Step 5: Construct RecommendRequest
#     req = RecommendRequest(
#         user_id=user_id,
#         user_name=user_name,
#         role=user_role,
#         region=user_region,
#         weak_indicator=weak,
#         user_language=user_language,
#         journey_day=journey_day,
#         watched_ids=[],  # Can be extended to fetch user's watch history
#         months_in_role=months_in_role,
#     )
    
#     logger.info(
#         "Recommendation request prepared | user_id=%s | name=%s | role=%s | region=%s | weak=%s | journey_day=%s | months_in_role=%s",
#         user_id,
#         user_name,
#         user_role,
#         user_region,
#         weak,
#         journey_day,
#         months_in_role,
#     )
    
#     # Step 6: Execute recommendation
#     try:
#         result = get_recommendation(req)
#         logger.info(
#             "Recommendation successful | user_id=%s | video_id=%s | score=%s",
#             user_id,
#             result.get("video_id"),
#             result.get("score"),
#         )
#         return result
#     except HTTPException as exc:
#         logger.error("Recommendation failed | user_id=%s | error=%s", user_id, exc.detail)
#         raise
#     except Exception as exc:
#         logger.exception("Unexpected error in recommendation | user_id=%s", user_id)
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to get recommendation for user {user_id}: {str(exc)}"
#         ) from exc


# ── ENDPOINTS ──────────────────────────────────────────

# @app.get("/")
# def root():
#     return {"status": "CLAN Recommendation API is running"}


# @app.get("/recommend-video-by-user-id/{user_id}", response_model=RecommendResponse)
# def get_video_recommendation_by_user_id(user_id: int, weak_indicator: Optional[str] = None):
#     """
#     Get video recommendation for a user using only user_id.
    
#     This endpoint:
#     1. Fetches user from PostgreSQL (name, region, role, language, join date)
#     2. Fetches user's weakest KII (key input indicator) if not provided
#     3. Calculates journey_day and months_in_role from user's created_at
#     4. Fetches all video recommendations from Qdrant
#     5. Scores and filters videos based on:
#        - Weak indicator match
#        - Sales phase (acquisition vs conversion based on journey day)
#        - Experience level (new_joiner vs experienced vs senior)
#        - Language match
#        - Semantic relevance
#     6. Returns the best matching video with full metadata
    
#     Args:
#         user_id: User ID (int) - Required path parameter
#         weak_indicator: Optional - Override user's weak indicator (e.g., "customer_generation")
    
#     Returns:
#         RecommendResponse with:
#             - video_id: Unique video identifier
#             - title: Video title
#             - creator_name: Creator's name
#             - summary: 3-4 sentence AI-generated summary
#             - key_lesson: One-sentence key takeaway
#             - problem_solved: What problem this video solves
#             - sales_phase: acquisition, development, conversion, or all
#             - experience_level: new_joiner, experienced, senior, or all
#             - notification_title: Personalized push notification title
#             - notification_body: Personalized push notification body
#             - score: Recommendation score (0.0-1.0)
#             - matched_indicator: The weak indicator used for matching
#             - language_match_type: exact, english_fallback, or other_fallback
    
#     Example:
#         GET /recommend-video-by-user-id/1020
#         GET /recommend-video-by-user-id/1020?weak_indicator=customer_generation
    
#     Raises:
#         404: User not found in database
#         422: Invalid weak_indicator or no valid videos found
#         500: Internal server error
#     """
#     try:
#         result = get_user_recommendation(user_id, weak_indicator)
#         return result
#     except HTTPException:
#         raise
#     except Exception as exc:
#         logger.exception("Unexpected error | user_id=%s", user_id)
#         raise HTTPException(status_code=500, detail=str(exc)) from exc


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


@app.post("/notifications/build", response_model=CampaignNotificationResponse)
def build_notification(req: CampaignNotificationRequest):
    res = notification_service.build_notification(req)
    if not res:
        raise HTTPException(status_code=422, detail="No notification generated for this payload (e.g. no rank improvement)")
    return res


@app.post("/notifications/build-batch", response_model=CampaignBatchNotificationResponse)
def build_notifications_batch(req: CampaignBatchNotificationRequest):
    return notification_service.build_notifications_batch(req.items)


@app.post("/notifications/admin/sync-indicators", response_model=IndicatorSyncResponse)
def sync_indicators(req: IndicatorSyncRequest):
    """
    Sync Qdrant payload from Postgres in one run:
    - lead_indicators from kii_content_relation
    - language/language_name from content.language_id
    """
    try:
        stats = sync_qdrant_payload_from_postgres(
            account_id=ACCOUNT_ID,
            dry_run=req.dry_run,
            clear_unmapped=req.clear_unmapped,
            limit=req.limit,
        )
        print("Sync stats:", stats)
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
            'video_id':    point.payload.get('video_id'),
            'title':       point.payload.get('title'),
            'indicators':  point.payload.get('lead_indicators'),
            'creator':     point.payload.get('creator_name'),
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


@app.post("/notifications/send-notifications", response_model=SendNotificationResponse)
def send_notifications(req: SendNotificationRequest):
    """
    Send Day 2 notifications to users.
    
    This endpoint:
    1. Fetches user details from PostgreSQL
    2. Recommends a video based on weak indicator
    3. Generates Day 2 specific copy
    4. Builds and returns notification object
    5. Saves to test log for manual testing
    
    Args:
        req: SendNotificationRequest containing:
            - user_id: User's ID (e.g., 953)
            - user_name: User's name (e.g., "Shashank")
            - weak_indicator: User's weakest KII (e.g., "customer_generation")
            - watched_video_ids: Videos already shown (optional)
            - months_in_role: Months in current role (optional)
            - campaign_day: Campaign day number (default: 2)
    
    Returns:
        SendNotificationResponse with notification object and test file path
    
    Raises:
        HTTPException: 400 for validation errors, 422 for processing errors
    """
    try:
        _load_indicator_configuration()
        
        # Validate weak_indicator exists
        if req.weak_indicator not in VALID_INDICATORS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"weak_indicator '{req.weak_indicator}' not found. "
                    f"Valid indicators: {sorted(VALID_INDICATORS)}"
                ),
            )
        
        # Call resolver
        result = notification_resolver.send_notifications(
            user_id=req.user_id,
            user_name=req.user_name,
            weak_indicator=req.weak_indicator,
            watched_video_ids=req.watched_video_ids or None,
            months_in_role=req.months_in_role,
            campaign_day=req.campaign_day,
        )
        
        # Check if resolver succeeded
        if not result.get("success"):
            logger.error(
                "Notification resolution failed | user_id=%s | error=%s",
                req.user_id,
                result.get("error", "Unknown error"),
            )
            raise HTTPException(
                status_code=422,
                detail=f"Failed to build notification for user {req.user_id}: {result.get('error', 'Unknown error')}",
            )
        
        # Format response
        notification_obj = result.get("notification", {})
        remote_result = _forward_to_remote_bulk_sender(
            user_id=req.user_id,
            notification=notification_obj,
        )

        response = SendNotificationResponse(
            success=True,
            user_id=result.get("user_id"),
            notification=NotificationObject(**notification_obj),
            test_file_path=result.get("test_file_path"),
            remote_send_status="sent",
            remote_send_response={
                "status_code": remote_result.get("status_code"),
                "response": remote_result.get("body"),
                "request_payload": remote_result.get("payload"),
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
            },
        )
        
        logger.info(
            "Notification sent | user_id=%s | campaign_day=%s | test_file=%s | remote_status=%s",
            req.user_id,
            req.campaign_day,
            result.get("test_file_path"),
            remote_result.get("status_code"),
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in send_notifications endpoint | user_id=%s", req.user_id)
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(exc)}",
        ) from exc


@app.post("/notifications/send", response_model=SendNotificationResponse)
def send_notification_auto(req: SimpleNotificationRequest):
    """
    Simplified endpoint - Send notifications with ONLY user_id.
    
    Auto-fetches and calculates all parameters:
    1. Fetches user name, region, role from PostgreSQL
    2. Auto-detects weak indicator (weakest KII from this week)
    3. Calculates months_in_role from user's profile_activation_date
    4. Builds and sends notification
    5. Forwards to remote bulk sender
    
    This is the SIMPLEST way to send notifications - just provide user_id!
    
    Args:
        req: SimpleNotificationRequest with:
            - user_id: User's ID (REQUIRED, e.g., 1020)
            - campaign_day: Campaign day number (optional, default: 2)
            - weak_indicator_override: Optional override for weak indicator
    
    Request Examples:
        POST /notifications/send
        {"user_id": 1020}
        
        POST /notifications/send
        {"user_id": 1020, "campaign_day": 1}
        
        POST /notifications/send
        {"user_id": 1020, "campaign_day": 1, "weak_indicator_override": "customer_met"}
    
    Returns:
        SendNotificationResponse with complete notification data
    
    Raises:
        HTTPException: 404 if user not found, 422 if notification build fails, 500 for errors
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
            raise HTTPException(
                status_code=404,
                detail=params['error'],
            )
        
        user_id = params['user_id']
        user_name = params['user_name']
        weak_indicator = params['weak_indicator']
        months_in_role = params['months_in_role']
        
        logger.info(
            "User parameters fetched | user_id=%s | name=%s | weak_indicator=%s | months_in_role=%s",
            user_id,
            user_name,
            weak_indicator,
            months_in_role,
        )
        
        # ─────────────────────────────────────────────────────────
        # STEP 2: Validate weak indicator
        # ─────────────────────────────────────────────────────────
        if weak_indicator not in VALID_INDICATORS:
            logger.warning(
                "Invalid weak_indicator | user_id=%s | indicator=%s | valid=%s",
                user_id,
                weak_indicator,
                sorted(list(VALID_INDICATORS))[:5],  # Show first 5 valid
            )
            raise HTTPException(
                status_code=422,
                detail=(
                    f"weak_indicator '{weak_indicator}' not valid for account. "
                    f"Valid indicators include: {sorted(list(VALID_INDICATORS))[:5]} (and {len(VALID_INDICATORS)-5} more)"
                ),
            )
        
        # ─────────────────────────────────────────────────────────
        # STEP 3: Call resolver with auto-fetched parameters
        # ─────────────────────────────────────────────────────────
        result = notification_resolver.send_notifications(
            user_id=user_id,
            user_name=user_name,
            weak_indicator=weak_indicator,
            watched_video_ids=None,  # Can be extended later
            months_in_role=months_in_role,
            campaign_day=req.campaign_day,
        )
        
        # ─────────────────────────────────────────────────────────
        # STEP 4: Check resolver success
        # ─────────────────────────────────────────────────────────
        if not result.get("success"):
            logger.error(
                "Notification resolution failed | user_id=%s | error=%s",
                user_id,
                result.get("error", "Unknown error"),
            )
            raise HTTPException(
                status_code=422,
                detail=f"Failed to build notification for user {user_id}: {result.get('error', 'Unknown error')}",
            )
        
        # ─────────────────────────────────────────────────────────
        # STEP 5: Forward to remote bulk sender
        # ─────────────────────────────────────────────────────────
        notification_obj = result.get("notification", {})
        remote_result = _forward_to_remote_bulk_sender(
            user_id=user_id,
            notification=notification_obj,
        )

        # ─────────────────────────────────────────────────────────
        # STEP 6: Build and return response
        # ─────────────────────────────────────────────────────────
        response = SendNotificationResponse(
            success=True,
            user_id=result.get("user_id"),
            notification=NotificationObject(**notification_obj),
            test_file_path=result.get("test_file_path"),
            remote_send_status="sent",
            remote_send_response={
                "status_code": remote_result.get("status_code"),
                "response": remote_result.get("body"),
                "request_payload": remote_result.get("payload"),
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
            },
        )
        
        logger.info(
            "Auto-fetch notification completed successfully | user_id=%s | campaign_day=%s | weak_indicator=%s",
            user_id,
            req.campaign_day,
            weak_indicator,
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in send_notification_auto endpoint | user_id=%s", req.user_id)
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(exc)}",
        ) from exc


def send_day_1_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 1 Title",
            "description": "Day 1 Description",
            "notification_type": "PUSH_VIDEO_FOR_USER",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 1 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 1 notification to user {user_id}: {e}")
        return False

def send_day_2_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 2 Title",
            "description": "Day 2 Description",
            "notification_type": "push_video_for_user",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 2 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 2 notification to user {user_id}: {e}")
        return False


def send_day_3_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 3 Title",
            "description": "Day 3 Description",
            "notification_type": "push_video_for_user",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 3 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 3 notification to user {user_id}: {e}")
        return False


def send_day_4_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 4 Title",
            "description": "Day 4 Description",
            "notification_type": "push_video_for_user",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 4 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 4 notification to user {user_id}: {e}")
        return False


def send_day_5_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 5 Title",
            "description": "Day 5 Description",
            "notification_type": "push_video_for_user",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 5 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 5 notification to user {user_id}: {e}")
        return False


def send_day_6_notification(user_id: int) -> bool:
    payload = [
        {
            "user_id": user_id,
            "title": "Day 6 Title",
            "description": "Day 6 Description",
            "notification_type": "push_video_for_user",
            "reference_id": 300,
            "video_popup": True,
            "image": None
        }
    ]
    try:
        response = requests.post(NOTIFICATION_API_URL, json=payload)
        response.raise_for_status()
        print(f"Day 6 notification sent to user {user_id}: {response.json()}")
        return True
    except Exception as e:
        print(f"Failed to send Day 6 notification to user {user_id}: {e}")
        return False


DAY_NOTIFICATION_FUNCTIONS = {
    1: send_day_1_notification,
    2: send_day_2_notification,
    3: send_day_3_notification,
    4: send_day_4_notification,
    5: send_day_5_notification,
    6: send_day_6_notification,
}


@app.post("/notifications/send-to-remote")
def send_notification_to_remote(day: int) -> bool:
    if day not in DAY_NOTIFICATION_FUNCTIONS:
        print(f"Invalid day: {day}. Must be between 1 and 7.")
        return False

    users = [1020,953]  # replace with actual user list

    for user in users:
        user_details = get_user_details(user)

        if user_details:
            print(f"Sending Day {day} notification to user {user}...")
            DAY_NOTIFICATION_FUNCTIONS[day](user_details["id"])  # call the correct day function
        else:
            print(f"No details found for user ID: {user}")

    return True

