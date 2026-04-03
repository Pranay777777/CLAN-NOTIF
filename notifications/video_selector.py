import hashlib
import logging
from typing import Optional

from constants import is_excluded_video, normalize_language
from qdrant.query import scroll_points


logger = logging.getLogger(__name__)


class VideoSelector:
    """Select campaign videos from Qdrant collection using region/language constraints."""

    def __init__(self, catalog_path: str = None):
        # catalog_path kept for backwards compatibility but not used
        self._videos: Optional[list[dict]] = None

    def refresh(self):
        """Clear cached videos to force re-fetch from Qdrant."""
        self._videos = None

    def _load_from_qdrant(self) -> list[dict]:
        """Load all videos from Qdrant and convert to dict format."""
        if self._videos is None:
            videos = []
            offset = None

            while True:
                points, next_offset = scroll_points(
                    limit=200,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )

                for point in points:
                    payload = point.payload or {}
                    title = str(payload.get("title", "")).strip()
                    
                    # Filter out excluded videos
                    if is_excluded_video(title):
                        continue

                    video_dict = {
                        "video_id": payload.get("video_id"),
                        "Title": title,
                        "creator": str(payload.get("creator_name", payload.get("creator", ""))).strip(),
                        "lead_indicator": " ".join(payload.get("lead_indicators", [])),
                        "summary": str(payload.get("summary", "")).strip(),
                        "key_lesson": str(payload.get("key_lesson", "")).strip(),
                        "problem_solved": str(payload.get("problem_solved", "")).strip(),
                        # Keep these fields aligned with NotificationService expectations.
                        "creator_region": str(payload.get("creator_region", "")).strip(),
                        "creator_role": str(payload.get("creator_role", "")).strip(),
                        "creator_name": str(payload.get("creator_name", payload.get("creator", ""))).strip(),
                        "language_name": str(
                            payload.get("language_name", payload.get("language", payload.get("lang", "")))
                        ).strip(),
                    }
                    videos.append(video_dict)

                if next_offset is None:
                    break
                offset = next_offset

            initial_count = len(videos)
            # Filter out excluded videos (redundant but keeps same logic)
            videos = [v for v in videos if not is_excluded_video(v.get("Title", ""))]
            excluded_count = initial_count - len(videos)
            if excluded_count > 0:
                logger.info(f"Excluded {excluded_count} intro/onboarding videos from notification selection")

            self._videos = videos
            logger.info(f"Loaded {len(self._videos)} videos from Qdrant")

        return self._videos


    @staticmethod
    def _norm(v: str) -> str:
        return str(v).strip().lower()

    def _match_language(self, videos: list[dict], language: str) -> list[dict]:
        """Filter videos by language."""
        lang = normalize_language(language)
        if lang in {"", "all", "any", "english"}:
            return videos

        # If catalog has language metadata, enforce strict same-language filtering.
        has_language_metadata = any(str(v.get("language_name", "")).strip() for v in videos)
        if has_language_metadata:
            scoped = [
                v for v in videos
                if normalize_language(str(v.get("language_name", ""))) == lang
            ]
            return scoped

        # No language metadata available in Qdrant payload yet.
        # Fallback keeps system functional while preserving deterministic selection.
        logger.warning("Language filter requested (%s) but no language metadata present in Qdrant payload", lang)
        return videos

    def _pick_deterministic(self, videos: list[dict], seed_key: str) -> Optional[dict]:
        """Pick one video deterministically using seed key."""
        if not videos:
            return None
        sorted_videos = sorted(videos, key=lambda v: (v.get("video_id", 0), v.get("Title", "")))
        seed = int(hashlib.md5(seed_key.encode("utf-8")).hexdigest(), 16)
        return sorted_videos[seed % len(sorted_videos)]

    def select_for_day1(self, user_region: str, language: str, cohort_key: str) -> Optional[dict]:
        """Pick a welcome-day "star" video with region/language preference.

        Heuristic:
        1) Prefer known performer roles (RM/BM/Supervisor) and named creators.
        2) Deterministically pick one for the cohort/day key.
        
        Note: Region/language filtering disabled until Qdrant payload includes these fields.
        """
        videos = self._load_from_qdrant()
        scoped = self._match_language(videos, language)

        if not scoped:
            scoped = videos

        # Prefer videos with identified creators
        named = [v for v in scoped if v.get("creator", "").strip() != ""]
        if named:
            scoped = named

        return self._pick_deterministic(scoped, seed_key=cohort_key)

    def select_for_campaign(
        self,
        campaign_day: int,
        user_region: str,
        language: str,
        cohort_key: str,
        user_id: int,
    ) -> Optional[dict]:
        """Select video for campaign based on day, region, language."""
        videos = self._load_from_qdrant()
        scoped = self._match_language(videos, language)

        if not scoped:
            scoped = videos

        # Day 4: prefer RM creators (but not available in Qdrant payload yet)
        # For now, just use all videos
        if campaign_day in (2, 4):
            chosen = self._pick_deterministic(scoped, seed_key=cohort_key)
        else:
            chosen = self._pick_deterministic(scoped, seed_key=f"day12:user:{user_id}")

        return chosen

    def select_for_day2(
        self,
        user_branch: str,
        language: str,
        weak_indicator: str,
        cohort_key: str,
        user_id: int,
    ) -> Optional[dict]:
        """Select a Day 2 video preferring creator branch + same language.

        Fallback order:
        1) Same creator branch + same language
        2) Same language + weak indicator match
        3) Same language only
        4) Weak indicator match across all videos
        5) Deterministic fallback across all eligible videos
        """
        videos = self._load_from_qdrant()
        branch = self._norm(user_branch)
        weak = self._norm(weak_indicator).replace(" ", "_")

        same_language = self._match_language(videos, language)
        if not same_language:
            same_language = videos

        branch_scoped = [
            v for v in same_language
            if self._norm(v.get("creator_region", "")) == branch
        ]
        if branch_scoped:
            return self._pick_deterministic(branch_scoped, seed_key=cohort_key)

        if weak:
            weak_scoped = [
                v for v in same_language
                if weak in self._norm(v.get("lead_indicator", "")).replace(" ", "_")
            ]
            if weak_scoped:
                return self._pick_deterministic(weak_scoped, seed_key=f"day2:weak:{weak}:{cohort_key}")

        if same_language:
            return self._pick_deterministic(same_language, seed_key=f"day2:lang:{cohort_key}:{user_id}")

        if weak:
            weak_scoped = [
                v for v in videos
                if weak in self._norm(v.get("lead_indicator", "")).replace(" ", "_")
            ]
            if weak_scoped:
                return self._pick_deterministic(weak_scoped, seed_key=f"day2:weak-all:{weak}:{cohort_key}")

        return self._pick_deterministic(videos, seed_key=f"day2:fallback:{cohort_key}:{user_id}")

    def select_for_day10(
        self, weak_indicator: str, language: str
    ) -> Optional[dict]:
        """Select video matching weak indicator for learning."""
        videos = self._load_from_qdrant()
        scoped = self._match_language(videos, language)

        if not scoped:
            scoped = videos

        # Filter by weak_indicator if provided
        if weak_indicator:
            indicator_scoped = [
                v
                for v in scoped
                if weak_indicator and weak_indicator.lower() in v.get("lead_indicator", "").lower()
            ]
            if indicator_scoped:
                scoped = indicator_scoped

        return self._pick_deterministic(scoped, seed_key=f"day10:{weak_indicator}")

    @staticmethod
    def _is_rm_role(role: str) -> bool:
        text = str(role or "").strip().lower()
        return "relationship manager" in text or text == "rm" or " rm " in f" {text} "

    @staticmethod
    def _is_closing_video(video: dict) -> bool:
        haystack = " ".join(
            [
                str(video.get("Title", "")),
                str(video.get("summary", "")),
                str(video.get("key_lesson", "")),
                str(video.get("problem_solved", "")),
            ]
        ).lower()
        keywords = ["close", "closing", "leads faster", "convert", "conversion", "approved"]
        return any(k in haystack for k in keywords)

    def select_for_day3(
        self,
        user_branch: str,
        language: str,
        cohort_key: str,
        user_id: int,
    ) -> Optional[dict]:
        """Day 3: prefer branch RM video, else best lead-closing video."""
        videos = self._load_from_qdrant()
        branch = self._norm(user_branch)

        same_language = self._match_language(videos, language)
        if not same_language:
            same_language = videos

        branch_rm = [
            v for v in same_language
            if self._norm(v.get("creator_region", "")) == branch and self._is_rm_role(v.get("creator_role", ""))
        ]
        if branch_rm:
            return self._pick_deterministic(branch_rm, seed_key=f"day3:branch-rm:{cohort_key}")

        closing_scoped = [v for v in same_language if self._is_closing_video(v)]
        if closing_scoped:
            return self._pick_deterministic(closing_scoped, seed_key=f"day3:closing:{cohort_key}")

        return self._pick_deterministic(same_language, seed_key=f"day3:fallback:{cohort_key}:{user_id}")
