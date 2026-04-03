import argparse
import json
import logging
import os
import re
import time

import pandas as pd
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from constants import normalize_language

from extract_final import extract_audio_transcript, extract_screen_text
from generate_metadata import build_chain, build_fallback, generate_metadata_for_video
from qdrant.store import upsert_video

os.makedirs("logs", exist_ok=True)

# ── SETTINGS ──────────────────────────────────────────
EXTRACTION_FILE = "./video_content_final.xlsx"  # still used until Phase 2 replaces it
# ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/clan_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("pipeline")
# Load once at module level — shared across all pipeline runs
_EMBED_MODEL: SentenceTransformer | None = None

# Load .env once for CLI runs (python pipeline.py ...)
load_dotenv()

from sqlalchemy import create_engine, text

def get_db_engine():
    # Use DATABASE_URL from environment/.env
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise ValueError("DATABASE_URL not set in .env")
    return create_engine(url)

def fetch_catalog_from_db() -> pd.DataFrame:
    """
    Replaces reading video_catalog.xlsx.
    Pulls content + creator info directly from DB.
    """
    engine = get_db_engine()
    query = text("""
        SELECT
            c.id          AS video_id,
            c.title       AS Title,
            c.description,
            c.language_id,
            c.thumbnail_url,
            c.length_mins AS duration,
            c.code        AS video_code,
            e.user_id     AS creator_user_id,
            u.name        AS creator_name,
            u.zone        AS creator_region
        FROM content c
        LEFT JOIN expert_user e ON e.account_id = c.created_by
        LEFT JOIN "user" u ON u.id = e.user_id
        WHERE c.status = 1
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df.fillna("")

def get_embed_model() -> SentenceTransformer:
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        model_name = os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")
        logger.info("Loading SentenceTransformer model: %s", model_name)
        _EMBED_MODEL = SentenceTransformer(model_name)
    return _EMBED_MODEL


def normalize_title(value: str) -> str:
    text = str(value).lower().strip()
    text = text.replace("_", " ")
    text = re.sub(r"\b720p\b|\bh264\b", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_indicators(row: dict):
    raw_ai = str(row.get("ai_lead_indicators", "")).strip()
    if raw_ai:
        try:
            parsed = json.loads(raw_ai)
            if isinstance(parsed, list) and parsed:
                return [str(x).strip().lower().replace(" ", "_") for x in parsed]
        except json.JSONDecodeError:
            pass

    raw_fallback = str(row.get("lead_indicator", ""))
    return [i.strip().lower().replace(" ", "_") for i in raw_fallback.split(",") if i.strip()]


def find_catalog_row(video_path: str, df_catalog: pd.DataFrame):
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    key = normalize_title(base_name)

    candidates = []
    for _, row in df_catalog.iterrows():
        title = str(row.get("Title", ""))
        tkey = normalize_title(title)
        if key == tkey or key in tkey or tkey in key:
            candidates.append(row)

    if not candidates:
        return None

    # Prefer exact normalized match if possible.
    for row in candidates:
        if normalize_title(row.get("Title", "")) == key:
            return row

    return candidates[0]



def get_existing_content(video_path: str):
    if not os.path.exists(EXTRACTION_FILE):
        return None, None

    df = pd.read_excel(EXTRACTION_FILE).fillna("")
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    key = normalize_title(base_name)

    for _, row in df.iterrows():
        video_title = str(row.get("Video Title", ""))
        if normalize_title(video_title) == key:
            return str(row.get("Screen Text (OCR)", "")), str(row.get("Audio Transcript (Whisper)", ""))

    return None, None


def build_embedding_text(row: dict, indicators):
    summary = str(row.get("ai_summary", "")) or str(row.get("description", ""))
    key_lesson = str(row.get("ai_key_lesson", ""))
    problem_solved = str(row.get("ai_problem_solved", ""))
    sales_phase = str(row.get("ai_sales_phase", "all")) or "all"
    experience_level = str(row.get("ai_experience_level", "all")) or "all"
    target_audience = str(row.get("ai_target_audience", "")) or str(row.get("target_audience", "all"))
    difficulty = str(row.get("ai_difficulty", "")) or str(row.get("difficulty", "beginner"))
    title = str(row.get("Title", "") or row.get("title", ""))

    return f"""
    Title: {title}
    Problem Solved: {problem_solved}
    Key Lesson: {key_lesson}
    Lead Indicators: {' '.join(indicators)}
    Summary: {summary}
    Sales Phase: {sales_phase}
    Experience Level: {experience_level}
    Description: {row.get('description', '')}
    Creator Role: {row.get('creator_role', '')}
    Difficulty: {difficulty}
    Target Audience: {target_audience}
    """.strip()


def build_payload(row: dict, indicators):
    summary = str(row.get("ai_summary", "")) or str(row.get("description", ""))
    key_lesson = str(row.get("ai_key_lesson", ""))
    problem_solved = str(row.get("ai_problem_solved", ""))
    sales_phase = str(row.get("ai_sales_phase", "all")) or "all"
    experience_level = str(row.get("ai_experience_level", "all")) or "all"
    target_audience = str(row.get("ai_target_audience", "")) or str(row.get("target_audience", "all"))
    difficulty = str(row.get("ai_difficulty", "")) or str(row.get("difficulty", "beginner"))
    persona_type = str(row.get("ai_persona_type", "")) or "all"
    user_context = str(row.get("ai_user_context", ""))
    background_situation = str(row.get("ai_background_situation", ""))
    emotional_tone = str(row.get("ai_emotional_tone", "")) or "supportive"
    language_name = str(row.get("language_name", "") or row.get("language", "") or "english")
    language = normalize_language(language_name)
    language_id = str(row.get("language_id", ""))

    return {
        "video_id": str(row.get("video_id", "")),
        "title": str(row.get("Title", "")),
        "creator_name": str(row.get("creator_name", "")),
        "creator_role": str(row.get("creator_role", "")),
        "creator_region": str(row.get("creator_region", "")),
        "lead_indicators": indicators,
        "target_audience": target_audience,
        "difficulty": difficulty,
        "summary": summary,
        "key_lesson": key_lesson,
        "problem_solved": problem_solved,
        "sales_phase": sales_phase,
        "experience_level": experience_level,
        "language": language,
        "language_name": language_name,
        "language_id": language_id,
        "persona_type": persona_type,
        "user_context": user_context,
        "background_situation": background_situation,
        "emotional_tone": emotional_tone,
        "ai_generated": bool(row.get("ai_generated", False)),
        "thumbnail_url": str(row.get("thumbnail_url", "")),
        "description": str(row.get("description", "")),
    }


def _has_meaningful_text(value: str) -> bool:
    text_value = str(value or "").strip()
    if not text_value:
        return False
    if text_value.lower() == "no screen text found":
        return False
    return True


def run_pipeline(video_path: str, reuse_existing_content: bool, dry_run: bool):
    start = time.time()
    logger.info("=" * 60)
    logger.info("PIPELINE START | video=%s", video_path)

    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video not found: {video_path}")

    logger.info("Step 1/5: Load catalog from DB")
    df_catalog = fetch_catalog_from_db()

    catalog_row = find_catalog_row(video_path, df_catalog)
    if catalog_row is None:
        raise ValueError(
            f"No matching content row found in DB for video: {video_path}. "
            "Check that the content title in DB matches the video filename."
        )

    row = catalog_row.to_dict()
    video_title = row.get("Title", "")
    logger.info("Matched catalog title: %s", video_title)

    logger.info("Step 2/5: Extract content")
    screen_text, transcript = (None, None)
    if reuse_existing_content:
        screen_text, transcript = get_existing_content(video_path)

    if not screen_text:
        screen_text = extract_screen_text(video_path)
    if not transcript:
        transcript = extract_audio_transcript(video_path)

    row["screen_text"] = screen_text
    row["transcript"] = transcript

    if not _has_meaningful_text(screen_text):
        raise RuntimeError(
            "OCR extraction is compulsory and returned empty text. "
            "Verify UNSTRUCTURED_OCR_URL and OCR service health."
        )
    if not _has_meaningful_text(transcript):
        raise RuntimeError(
            "Audio transcript extraction is compulsory and returned empty text. "
            "Verify source audio quality and Whisper processing."
        )

    logger.info(
        "Extraction complete | screen_chars=%d | transcript_chars=%d",
        len(str(screen_text)),
        len(str(transcript)),
    )

    logger.info("Step 3/5: Generate metadata")
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        logger.warning("GEMINI_API_KEY missing, using fallback metadata")
        metadata = None
    else:
        chain = build_chain(api_key)
        metadata = generate_metadata_for_video(chain, row)

    if metadata is None:
        fallback = build_fallback(row)
        row["ai_summary"] = fallback["summary"]
        row["ai_lead_indicators"] = json.dumps(fallback["lead_indicators"])
        row["ai_target_audience"] = fallback["target_audience"]
        row["ai_difficulty"] = fallback["difficulty"]
        row["ai_key_lesson"] = fallback["key_lesson"]
        row["ai_sales_phase"] = fallback["sales_phase"]
        row["ai_experience_level"] = fallback["experience_level"]
        row["ai_problem_solved"] = fallback["problem_solved"]
        row["ai_persona_type"] = fallback["persona_type"]
        row["ai_user_context"] = fallback["user_context"]
        row["ai_background_situation"] = fallback["background_situation"]
        row["ai_emotional_tone"] = fallback["emotional_tone"]
        row["ai_generated"] = False
    else:
        row["ai_summary"] = metadata.summary
        row["ai_lead_indicators"] = json.dumps(metadata.lead_indicators)
        row["ai_target_audience"] = metadata.target_audience
        row["ai_difficulty"] = metadata.difficulty
        row["ai_key_lesson"] = metadata.key_lesson
        row["ai_sales_phase"] = metadata.sales_phase
        row["ai_experience_level"] = metadata.experience_level
        row["ai_problem_solved"] = metadata.problem_solved
        row["ai_persona_type"] = metadata.persona_type
        row["ai_user_context"] = metadata.user_context
        row["ai_background_situation"] = metadata.background_situation
        row["ai_emotional_tone"] = metadata.emotional_tone
        row["ai_generated"] = True

    logger.info("Metadata complete | ai_generated=%s", row["ai_generated"])

    logger.info("Step 4/5: Build embedding + payload")
    indicators = parse_indicators(row)
    embedding_text = build_embedding_text(row, indicators)
    payload = build_payload(row, indicators)

    if dry_run:
        logger.info("Step 5/5: DRY RUN mode, skipping Qdrant upsert")
        logger.info("Sample payload title=%s | indicators=%s", payload.get("title"), payload.get("lead_indicators"))
    else:
        logger.info("Step 5/5: Upsert to Qdrant")
        model = get_embed_model()
        vector = model.encode(embedding_text).tolist()
        upsert_video(
            video_id=int(row.get("video_id")),
            vector=vector,
            payload=payload,
        )
        logger.info("Qdrant upsert complete | video_id=%s", row.get("video_id"))

    elapsed = round(time.time() - start, 2)
    logger.info("PIPELINE COMPLETE | seconds=%s", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-video orchestrator pipeline")
    parser.add_argument("--video", required=True, help="Path to video file")
    parser.add_argument(
        "--reuse-existing-content",
        action="store_true",
        help="Reuse content from video_content_final.xlsx if available",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run without Qdrant upsert")
    args = parser.parse_args()

    run_pipeline(
        video_path=args.video,
        reuse_existing_content=args.reuse_existing_content,
        dry_run=args.dry_run,
    )
