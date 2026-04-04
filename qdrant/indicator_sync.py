from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import text

from constants import ACCOUNT_ID, normalize_language
from database.db_config import engine as db_engine
from qdrant.client import get_collection_name, get_qdrant_client


def _to_indicator_code(name: str) -> str:
    return str(name or "").strip().lower().replace(" ", "_")


def _normalize_indicator_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []

    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = _to_indicator_code(str(value))
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _fetch_content_indicator_map(account_id: int) -> dict[int, list[str]]:
    query = text(
        """
        SELECT
            r.content_id,
            km.kii_name
        FROM public.kii_content_relation r
        JOIN public.kii_master km
          ON km.id = r.kii_id
        WHERE r.status = 1
          AND km.status = 1
          AND km.account_id = :account_id
        ORDER BY r.content_id, r.id
        """
    )

    mapping: dict[int, list[str]] = defaultdict(list)
    with db_engine.connect() as conn:
        rows = conn.execute(query, {"account_id": account_id}).mappings().all()

    for row in rows:
        try:
            content_id = int(row.get("content_id"))
        except (TypeError, ValueError):
            continue

        code = _to_indicator_code(str(row.get("kii_name", "")))
        if not code:
            continue

        if code not in mapping[content_id]:
            mapping[content_id].append(code)

    print(dict(mapping))        

    return dict(mapping)


def sync_lead_indicators_from_postgres(
    account_id: int = ACCOUNT_ID,
    dry_run: bool = True,
    clear_unmapped: bool = False,
    limit: int | None = None,
) -> dict:
    """
    Sync only lead_indicators in Qdrant payload from Postgres kii_content_relation.

    This intentionally updates a minimal payload surface to avoid storing
    unnecessary duplicated state in Qdrant.
    """
    client = get_qdrant_client()
    collection = get_collection_name()

    db_mapping = _fetch_content_indicator_map(account_id=account_id)

    scanned_points = 0
    matched_points = 0
    updated_points = 0
    unchanged_points = 0
    skipped_unmapped = 0

    samples: list[dict[str, Any]] = []

    offset = None
    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=200,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for point in points:
            scanned_points += 1
            payload = point.payload or {}

            raw_video_id = payload.get("video_id", point.id)
            try:
                content_id = int(raw_video_id)
            except (TypeError, ValueError):
                continue

            desired = db_mapping.get(content_id)
            if desired is None:
                if not clear_unmapped:
                    skipped_unmapped += 1
                    continue
                desired = []

            matched_points += 1

            current = _normalize_indicator_list(payload.get("lead_indicators"))
            desired = _normalize_indicator_list(desired)

            if current == desired:
                unchanged_points += 1
                continue

            if len(samples) < 10:
                samples.append(
                    {
                        "point_id": point.id,
                        "video_id": content_id,
                        "before": current,
                        "after": desired,
                    }
                )

            if not dry_run:
                client.set_payload(
                    collection_name=collection,
                    payload={"lead_indicators": desired},
                    points=[point.id],
                )

            updated_points += 1

            if limit is not None and updated_points >= limit:
                return {
                    "dry_run": dry_run,
                    "account_id": account_id,
                    "collection": collection,
                    "scanned_points": scanned_points,
                    "matched_points": matched_points,
                    "updated_points": updated_points,
                    "unchanged_points": unchanged_points,
                    "skipped_unmapped": skipped_unmapped,
                    "db_mapped_content_ids": len(db_mapping),
                    "limit_reached": True,
                    "samples": samples,
                }

        if next_offset is None:
            break
        offset = next_offset

    return {
        "dry_run": dry_run,
        "account_id": account_id,
        "collection": collection,
        "scanned_points": scanned_points,
        "matched_points": matched_points,
        "updated_points": updated_points,
        "unchanged_points": unchanged_points,
        "skipped_unmapped": skipped_unmapped,
        "db_mapped_content_ids": len(db_mapping),
        "limit_reached": False,
        "samples": samples,
    }


def _fetch_content_language_map(account_id: int) -> dict[int, str]:
    query = text(
        """
        SELECT
            c.id AS content_id,
            COALESCE(ml.language_code, '') AS language_code
        FROM public.content c
        LEFT JOIN public.md_app_languages ml
          ON ml.id = c.language_id
        WHERE c.status = 1
        """
    )

    mapping: dict[int, str] = {}
    with db_engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    for row in rows:
        try:
            content_id = int(row.get("content_id"))
        except (TypeError, ValueError):
            continue

        language_code = str(row.get("language_code", "")).strip().lower()
        if not language_code:
            language_code = "en"
        mapping[content_id] = language_code

    return mapping


def sync_video_languages_from_postgres(
    account_id: int = ACCOUNT_ID,
    dry_run: bool = True,
    limit: int | None = None,
) -> dict:
    """Sync language/language_name payload fields in Qdrant from Postgres content.language_id."""
    client = get_qdrant_client()
    collection = get_collection_name()

    db_mapping = _fetch_content_language_map(account_id=account_id)

    scanned_points = 0
    matched_points = 0
    updated_points = 0
    unchanged_points = 0
    skipped_unmapped = 0

    samples: list[dict[str, Any]] = []

    offset = None
    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=200,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for point in points:
            scanned_points += 1
            payload = point.payload or {}

            raw_video_id = payload.get("video_id", point.id)
            try:
                content_id = int(raw_video_id)
            except (TypeError, ValueError):
                continue

            language_code = db_mapping.get(content_id)
            if language_code is None:
                skipped_unmapped += 1
                continue

            matched_points += 1

            desired_language = normalize_language(language_code)
            desired_language_name = desired_language
            current_language = normalize_language(str(payload.get("language", "")).strip())
            current_language_name = normalize_language(str(payload.get("language_name", "")).strip())

            if current_language == desired_language and current_language_name == desired_language_name:
                unchanged_points += 1
                continue

            if len(samples) < 10:
                samples.append(
                    {
                        "point_id": point.id,
                        "video_id": content_id,
                        "before": {
                            "language": payload.get("language"),
                            "language_name": payload.get("language_name"),
                        },
                        "after": {
                            "language": desired_language,
                            "language_name": desired_language_name,
                        },
                    }
                )

            if not dry_run:
                client.set_payload(
                    collection_name=collection,
                    payload={
                        "language": desired_language,
                        "language_name": desired_language_name,
                    },
                    points=[point.id],
                )

            updated_points += 1

            if limit is not None and updated_points >= limit:
                return {
                    "dry_run": dry_run,
                    "account_id": account_id,
                    "collection": collection,
                    "scanned_points": scanned_points,
                    "matched_points": matched_points,
                    "updated_points": updated_points,
                    "unchanged_points": unchanged_points,
                    "skipped_unmapped": skipped_unmapped,
                    "db_mapped_content_ids": len(db_mapping),
                    "limit_reached": True,
                    "samples": samples,
                }

        if next_offset is None:
            break
        offset = next_offset

    return {
        "dry_run": dry_run,
        "account_id": account_id,
        "collection": collection,
        "scanned_points": scanned_points,
        "matched_points": matched_points,
        "updated_points": updated_points,
        "unchanged_points": unchanged_points,
        "skipped_unmapped": skipped_unmapped,
        "db_mapped_content_ids": len(db_mapping),
        "limit_reached": False,
        "samples": samples,
    }


def sync_qdrant_payload_from_postgres(
    account_id: int = ACCOUNT_ID,
    dry_run: bool = True,
    clear_unmapped: bool = False,
    limit: int | None = None,
) -> dict:
    """Sync indicators + language in one pass via existing admin flow.

    Returns legacy top-level keys for backward compatibility and includes
    per-section details under `indicator_sync` and `language_sync`.
    """
    indicator_stats = sync_lead_indicators_from_postgres(
        account_id=account_id,
        dry_run=dry_run,
        clear_unmapped=clear_unmapped,
        limit=limit,
    )
    language_stats = sync_video_languages_from_postgres(
        account_id=account_id,
        dry_run=dry_run,
        limit=limit,
    )

    return {
        "dry_run": dry_run,
        "account_id": account_id,
        "collection": indicator_stats.get("collection", language_stats.get("collection", "clan_videos")),
        "scanned_points": max(
            int(indicator_stats.get("scanned_points", 0)),
            int(language_stats.get("scanned_points", 0)),
        ),
        "matched_points": int(indicator_stats.get("matched_points", 0)) + int(language_stats.get("matched_points", 0)),
        "updated_points": int(indicator_stats.get("updated_points", 0)) + int(language_stats.get("updated_points", 0)),
        "unchanged_points": int(indicator_stats.get("unchanged_points", 0)) + int(language_stats.get("unchanged_points", 0)),
        "skipped_unmapped": int(indicator_stats.get("skipped_unmapped", 0)) + int(language_stats.get("skipped_unmapped", 0)),
        "db_mapped_content_ids": int(language_stats.get("db_mapped_content_ids", 0)),
        "limit_reached": bool(indicator_stats.get("limit_reached") or language_stats.get("limit_reached")),
        "samples": (indicator_stats.get("samples", [])[:5] + language_stats.get("samples", [])[:5])[:10],
        "indicator_sync": indicator_stats,
        "language_sync": language_stats,
    }
