import json
import pandas as pd
from sentence_transformers import SentenceTransformer
from constants import is_excluded_video, normalize_language

from qdrant.client import get_collection_name
from qdrant.store import reload_all_videos

# ── SETTINGS ──────────────────────────────────────────
CATALOG_FILE = "./video_catalog_enriched.xlsx"
COLLECTION = get_collection_name()
# ──────────────────────────────────────────────────────

print("Loading model...")
model  = SentenceTransformer('all-MiniLM-L6-v2')

# ── Load catalog ──
print("Reading video catalog...")
df = pd.read_excel(CATALOG_FILE)
df = df.fillna("")  # replace empty cells with empty string


def parse_indicators(row):
    """Prefer AI-generated indicators, fallback to catalog lead_indicator."""
    raw_ai = str(row.get("ai_lead_indicators", "")).strip()
    if raw_ai:
        try:
            parsed = json.loads(raw_ai)
            if isinstance(parsed, list):
                cleaned = [str(i).strip().lower().replace(" ", "_") for i in parsed if str(i).strip()]
                if cleaned:
                    return cleaned
        except json.JSONDecodeError:
            pass

    raw_fallback = str(row.get("lead_indicator", ""))
    return [i.strip().lower().replace(" ", "_") for i in raw_fallback.split(",") if i.strip()]

print(f"Preparing collection '{COLLECTION}' on Qdrant server...")

# ── Generate embeddings and store ──
print("\nGenerating embeddings for training videos (excluding intros)...")
points = []
excluded_count = 0

for _, row in df.iterrows():
    title = str(row.get('Title', ''))
    
    # Skip intro/onboarding videos
    if is_excluded_video(title):
        print(f"  ⊘ Skipped: {title}")
        excluded_count += 1
        continue
    
    indicators = parse_indicators(row)

    summary = str(row.get("ai_summary", "")) or str(row.get("description", ""))
    key_lesson = str(row.get("ai_key_lesson", ""))
    problem_solved = str(row.get("ai_problem_solved", ""))
    sales_phase = str(row.get("ai_sales_phase", "all")) or "all"
    experience_level = str(row.get("ai_experience_level", "all")) or "all"
    target_audience = str(row.get("ai_target_audience", "")) or str(row.get("target_audience", "all"))
    difficulty = str(row.get("ai_difficulty", "")) or str(row.get("difficulty", "beginner"))
    language_name = str(row.get("language_name", "") or row.get("language", "") or "english")
    language = normalize_language(language_name)
    language_id = str(row.get("language_id", ""))

    text_to_embed = f"""
    Title: {row['Title']}
    Problem Solved: {problem_solved}
    Key Lesson: {key_lesson}
    Lead Indicators: {' '.join(indicators)}
    Summary: {summary}
    Sales Phase: {sales_phase}
    Experience Level: {experience_level}
    Description: {row['description']}
    Creator Role: {row['creator_role']}
    Difficulty: {difficulty}
    Target Audience: {target_audience}
    """.strip()

    # Generate vector
    vector = model.encode(text_to_embed).tolist()

    # Build metadata payload
    payload = {
        "video_id":        str(row['video_id']),
        "title":           str(row['Title']),
        "creator_name":    str(row['creator_name']),
        "creator_role":    str(row['creator_role']),
        "creator_region":  str(row['creator_region']),
        "lead_indicators": indicators,
        "target_audience": target_audience,
        "difficulty":      difficulty,
        "language":        language,
        "language_name":   language_name,
        "language_id":     language_id,
        "summary":         summary,
        "key_lesson":      key_lesson,
        "problem_solved":  problem_solved,
        "sales_phase":     sales_phase,
        "experience_level": experience_level,
        "ai_generated":    bool(row.get("ai_generated", False)),
        "thumbnail_url":   str(row['thumbnail_url']),
        "description":     str(row['description']),
    }

    points.append(
        {
            "id": int(row['video_id']),
            "vector": vector,
            "payload": payload,
        }
    )

    print(f"  ✓ {row['Title']}")

# ── Upload all points to Qdrant ──
reload_all_videos(points)

print(f"\n✓ Loaded {len(points)} training videos to Qdrant (excluded {excluded_count} intro videos)")
print("✓ Ready for recommendations")