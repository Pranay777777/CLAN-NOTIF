import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import List, Optional
import httpx

from fastapi import FastAPI, HTTPException, Request
from apscheduler.schedulers.background import BackgroundScheduler
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
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
scheduler = BackgroundScheduler(daemon=True)

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup and shutdown."""
    # Startup
    logger.info("Starting FastAPI app with background indicator sync scheduler (hourly)")
    scheduler.add_job(run_hourly_sync, "interval", hours=1, id="sync_indicators_hourly")
    scheduler.start()
    yield
    # Shutdown
    logger.info("Shutting down, stopping scheduler...")
    scheduler.shutdown()

app = FastAPI(title="CLAN Video Recommendation API", lifespan=lifespan)
model = SentenceTransformer("all-MiniLM-L6-v2")
notification_service = NotificationService()
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
    campaign_day: int = 2

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


def _forward_to_remote_bulk_sender(user_id: int, notification: dict) -> dict:
    """Forward built notification to deployed bulk sender endpoint."""
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
        raise HTTPException(
            status_code=504,
            detail={
                "message": "Remote notification sender timed out",
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
                "timeout_seconds": REMOTE_NOTIFICATION_TIMEOUT_SECONDS,
            },
        ) from exc
    except httpx.HTTPError as exc:
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
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Remote notification sender returned an error",
                "remote_status_code": response.status_code,
                "remote_response": response_body,
                "remote_url": REMOTE_NOTIFICATION_SEND_URL,
            },
        )

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


# ── ENDPOINTS ──────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "CLAN Recommendation API is running"}


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