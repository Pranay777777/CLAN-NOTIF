from sentence_transformers import SentenceTransformer
from qdrant_client.models import Filter, FieldCondition, MatchValue

from qdrant.query import query_points
from constants import is_excluded_video

# ── SETTINGS ──────────────────────────────────────────
INDICATOR_LABELS = {
    "customer_generation":                    "customer generation",
    "customer_interested":                    "converting interested customers",
    "customer_document_uploaded":             "document collection",
    "customer_loan_approved":                 "loan approval rate",
    "customer_loan_disbursed":                "loan disbursals",
    "product_trainings_attended":             "product knowledge",
    "marketing_activities_conducted":         "marketing activities",
    "daily_huddle_meeting_attended":          "daily huddle attendance",
    "potential_channel_partners_identified":  "channel partner sourcing",
    "channel_partners_empanelled":            "channel partner empanelment",
}
# ──────────────────────────────────────────────────────

model  = SentenceTransformer('all-MiniLM-L6-v2')

INDICATOR_PROBLEM_KEYWORDS = {
    "customer_generation": ["lead", "prospect", "source", "market", "customer generation"],
    "customer_interested": ["interest", "convince", "objection", "follow up"],
    "customer_document_uploaded": ["document", "kyc", "paperwork", "upload"],
    "customer_loan_approved": ["approval", "reject", "underwriting"],
    "customer_loan_disbursed": ["disbursal", "processing", "sanction"],
    "product_trainings_attended": ["training", "product", "knowledge"],
    "marketing_activities_conducted": ["marketing", "activity", "campaign"],
    "daily_huddle_meeting_attended": ["huddle", "meeting", "daily plan"],
    "potential_channel_partners_identified": ["channel partner", "partner", "builder"],
    "channel_partners_empanelled": ["empanel", "onboard", "channel partner"],
}


def infer_sales_phase(journey_day):
    return "acquisition" if journey_day <= 15 else "conversion"


def infer_experience_level(months_in_role):
    if months_in_role is None:
        return "all"
    if months_in_role < 3:
        return "new_joiner"
    if months_in_role <= 12:
        return "experienced"
    return "senior"


def score_problem_match(weak_indicator, payload):
    keywords = INDICATOR_PROBLEM_KEYWORDS.get(weak_indicator, [])
    haystack = " ".join([
        str(payload.get("problem_solved", "")),
        str(payload.get("key_lesson", "")),
        str(payload.get("summary", "")),
    ]).lower()
    if not keywords:
        return 0.0
    hits = sum(1 for kw in keywords if kw in haystack)
    return min(hits / max(len(keywords), 1), 1.0)

def recommend_video(user_name, weak_indicator, user_role, user_region, journey_day=10, months_in_role=None, watched_ids=[]):
    print(f"\n{'='*50}")
    print(f"User:             {user_name}")
    print(f"Weak Indicator:   {weak_indicator}")
    print(f"Role:             {user_role}")
    print(f"Region:           {user_region}")
    print(f"Journey Day:      {journey_day}")
    print(f"Months in Role:   {months_in_role}")
    print(f"Already watched:  {watched_ids}")
    print(f"{'='*50}")

    # Build search query from user's weakness
    query_text = f"how to improve {weak_indicator} for {user_role} in {user_region}"
    query_vector = model.encode(query_text).tolist()
    user_sales_phase = infer_sales_phase(journey_day)
    user_experience_level = infer_experience_level(months_in_role)

    # Search Qdrant — filter by lead_indicator
    # Clean the weak indicator to match stored format
    weak_indicator_clean = weak_indicator.strip().lower().replace(' ', '_')

    results = query_points(
        query_vector=query_vector,
        limit=5,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="lead_indicators",
                    match=MatchValue(value=weak_indicator_clean)
                )
            ]
        ),
    )

    if not results:
        # Fallback — no filter, just semantic search
        print("No exact match found — using semantic fallback")
        results = query_points(
            query_vector=query_vector,
            limit=5,
        )

    # Score and rank results
    scored = []
    for r in results:
        p = r.payload

        # Never recommend intro/onboarding content.
        if is_excluded_video(p.get('title', '')):
            continue

        base_score = r.score

        indicators = p.get("lead_indicators", [])
        indicator_match = 1.0 if weak_indicator_clean in indicators else 0.0

        video_phase = str(p.get("sales_phase", "all")).lower()
        sales_phase_match = 1.0 if video_phase in {"all", user_sales_phase} else 0.0

        video_experience = str(p.get("experience_level", "all")).lower()
        experience_match = 1.0 if video_experience in {"all", user_experience_level} else 0.0

        problem_match = score_problem_match(weak_indicator_clean, p)
        recency_penalty = 0.15 if int(p.get('video_id', 0)) in watched_ids else 0.0

        final_score = (
            (base_score * 0.30)
            + (indicator_match * 0.25)
            + (problem_match * 0.20)
            + (experience_match * 0.15)
            + (sales_phase_match * 0.10)
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
            'final_score': round(final_score, 3),
            'base_score':  round(base_score, 3),
        })

    # Sort by final score
    scored.sort(key=lambda x: x['final_score'], reverse=True)
    if not scored:
        raise RuntimeError("No eligible videos available after exclusion filters")
    best = scored[0]

    print(f"\n✓ RECOMMENDED VIDEO:")
    print(f"  Title:       {best['title']}")
    print(f"  Creator:     {best['creator']}")
    print(f"  Indicators:  {best['indicators']}")
    print(f"  Sales Phase: {best['sales_phase']}")
    print(f"  Experience:  {best['experience_level']}")
    print(f"  Key Lesson:  {best['key_lesson']}")
    print(f"  Score:       {best['final_score']} (base: {best['base_score']})")

    # Generate notification text
    readable = INDICATOR_LABELS.get(weak_indicator_clean, weak_indicator.replace('_', ' '))
    notification = f"{user_name}, {best['creator']} shares a practical tip to improve your {readable}."
    print(f"\n📱 NOTIFICATION:")
    print(f"  {notification}")

    return best, notification

# ── TEST WITH REAL INDICATOR NAMES ────────────────────
if __name__ == "__main__":

    # Test 1 — RM behind on customer generation
    recommend_video(
        user_name      = "Aarav",
        weak_indicator = "customer_generation",
        user_role      = "RM",
        user_region    = "Hyderabad",
        journey_day    = 8,
        months_in_role = 2,
        watched_ids    = []
    )

    # Test 2 — RM behind on customer document uploaded
    recommend_video(
        user_name      = "Priya",
        weak_indicator = "customer_document_uploaded",
        user_role      = "RM",
        user_region    = "Mumbai",
        journey_day    = 20,
        months_in_role = 6,
        watched_ids    = [1]
    )

    # Test 3 — RM behind on marketing activities conducted
    recommend_video(
        user_name      = "Ravi",
        weak_indicator = "marketing_activities_conducted",
        user_role      = "RM",
        user_region    = "Pune",
        journey_day    = 12,
        months_in_role = 14,
        watched_ids    = []
    )