"""
Script to fetch content data (languages, content_id, video names) from Postgres
and store/update in Qdrant collection.
"""

import logging
import os
from typing import Any, Optional
from sqlalchemy import text
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from qdrant_client.models import PointStruct

from constants import ACCOUNT_ID
from database.db_config import engine as db_engine
from qdrant.client import get_qdrant_client, get_collection_name

load_dotenv()
os.makedirs("logs", exist_ok=True)

# ── LOGGING ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/store_content_to_qdrant.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('store_content_to_qdrant')


def fetch_content_data_from_postgres(account_id: int = ACCOUNT_ID) -> dict[int, dict[str, Any]]:
    """
    Fetch content data from Postgres WHERE creator's account_id matches:
    content_id, title, language_code.
    
    Args:
        account_id: Filter by creator's account_id (default: from ACCOUNT_ID)
    
    Returns:
        {content_id: {"title": str, "language_code": str}}
    """
    query = text(
        """
        SELECT
            c.id AS content_id,
            c.title AS content_title,
            COALESCE(ml.language_code, 'en') AS language_code
        FROM public.content c
        INNER JOIN public.expert_user eu
            ON eu.user_id = c.created_by
        LEFT JOIN public.md_app_languages ml
            ON ml.id = c.language_id
        WHERE c.status = 1 
          AND eu.status = 1
          AND eu.account_id = :account_id
        ORDER BY c.id
        """
    )

    content_data: dict[int, dict[str, Any]] = {}
    
    try:
        with db_engine.connect() as conn:
            rows = conn.execute(query, {"account_id": account_id}).mappings().all()
        
        logger.info(f"Fetched {len(rows)} content records from Postgres for account_id={account_id}")
        
        for row in rows:
            try:
                content_id = int(row.get("content_id"))
                title = str(row.get("content_title", "")).strip()
                language_code = str(row.get("language_code", "en")).strip().lower()
                
                content_data[content_id] = {
                    "title": title,
                    "language_code": language_code,
                }
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping row due to error: {e}")
                continue
        
        return content_data
    
    except Exception as e:
        logger.error(f"Failed to fetch content data from Postgres: {e}")
        raise


def store_content_in_qdrant(
    content_data: dict[int, dict[str, Any]],
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Store/update content data in Qdrant with embeddings.
    
    Args:
        content_data: {content_id: {"title": ..., "language_code": ...}}
        dry_run: If True, don't actually write to Qdrant
    
    Returns:
        Summary stats dict
    """
    client = get_qdrant_client()
    collection = get_collection_name()
    
    # Load embedding model
    logger.info("Loading SentenceTransformer model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    stored_count = 0
    updated_count = 0
    failed_count = 0
    samples: list[dict[str, Any]] = []
    points_to_upsert: list[PointStruct] = []
    
    logger.info(f"{'='*60}")
    logger.info(f"Starting Qdrant storage (dry_run={dry_run})")
    logger.info(f"{'='*60}")
    
    # Get all existing points in Qdrant first
    logger.info("Scanning existing Qdrant points...")
    existing_point_ids: dict[int, int] = {}  # {content_id: point_id}
    
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
            payload = point.payload or {}
            try:
                video_id = int(payload.get("video_id", point.id))
                existing_point_ids[video_id] = point.id
            except (TypeError, ValueError):
                continue
        
        if next_offset is None:
            break
        offset = next_offset
    
    logger.info(f"Found {len(existing_point_ids)} existing points in Qdrant")
    
    # Process each content record
    logger.info(f"Generating embeddings for {len(content_data)} records...")
    for content_id, data in content_data.items():
        try:
            title = data.get("title", "")
            language_code = data.get("language_code", "en")
            
            # Generate embedding from title + language
            embedding_text = f"{title} {language_code}".strip()
            vector = model.encode(embedding_text).tolist()
            
            payload = {
                "video_id": content_id,
                "title": title,
                "language": language_code,
            }
            
            # Check if this is an update or insert
            if content_id in existing_point_ids:
                # Update existing point
                point_id = existing_point_ids[content_id]
                
                if not dry_run:
                    client.set_payload(
                        collection_name=collection,
                        payload=payload,
                        points=[point_id],
                    )
                
                updated_count += 1
                logger.info(f"[UPDATE] content_id={content_id}, title={title[:30]}...")
                
                if len(samples) < 5:
                    samples.append({
                        "content_id": content_id,
                        "title": title,
                        "language": language_code,
                        "action": "updated",
                    })
            else:
                # Insert new point with vector
                point = PointStruct(
                    id=int(content_id),
                    vector=vector,
                    payload=payload,
                )
                points_to_upsert.append(point)
                stored_count += 1
                logger.info(f"[INSERT] content_id={content_id}, title={title[:30]}...")
                
                if len(samples) < 5:
                    samples.append({
                        "content_id": content_id,
                        "title": title,
                        "language": language_code,
                        "action": "inserted",
                    })
                
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to process content_id={content_id}: {e}")
            continue
    
    # Batch upsert all new points
    if points_to_upsert:
        logger.info(f"Upserting {len(points_to_upsert)} new points to Qdrant...")
        if not dry_run:
            client.upsert(
                collection_name=collection,
                points=points_to_upsert,
            )
        logger.info(f"✓ Successfully upserted {len(points_to_upsert)} points")
    
    result = {
        "dry_run": dry_run,
        "collection": collection,
        "total_content_from_postgres": len(content_data),
        "inserted_count": stored_count,
        "updated_count": updated_count,
        "failed_count": failed_count,
        "existing_qdrant_points": len(existing_point_ids),
        "samples": samples,
    }
    
    logger.info(f"{'='*60}")
    logger.info(f"SUMMARY: Inserted={stored_count}, Updated={updated_count}, Failed={failed_count}")
    logger.info(f"Total content in Postgres: {len(content_data)}")
    logger.info(f"Existing Qdrant points: {len(existing_point_ids)}")
    logger.info(f"{'='*60}")
    
    return result


def main(dry_run: bool = True):
    """
    Main function to orchestrate the flow:
    1. Fetch content data from Postgres
    2. Store/update in Qdrant
    """
    logger.info("Starting content data sync: Postgres → Qdrant")
    
    try:
        # Step 1: Fetch from Postgres
        logger.info("Step 1: Fetching content data from Postgres...")
        content_data = fetch_content_data_from_postgres(account_id=ACCOUNT_ID)
        logger.info(f"✓ Fetched {len(content_data)} records")
        
        # Step 2: Store in Qdrant
        logger.info("Step 2: Storing content data in Qdrant...")
        result = store_content_in_qdrant(
            content_data=content_data,
            dry_run=dry_run,
        )
        logger.info("✓ Storage complete")
        
        # Print results
        print("\n" + "="*60)
        print("FINAL RESULTS")
        print("="*60)
        for key, value in result.items():
            if key != "samples":
                print(f"{key:.<40} {value}")
        
        if result["samples"]:
            print("\nSample updates:")
            for sample in result["samples"]:
                print(f"  - {sample}")
        
        return result
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch content data from Postgres and store in Qdrant"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry run mode (default: True)"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write to Qdrant (disables dry-run)"
    )
    
    args = parser.parse_args()
    
    dry_run = not args.execute
    
    if dry_run:
        logger.info("DRY RUN MODE: No data will be written to Qdrant")
    else:
        logger.warning("EXECUTE MODE: Data WILL be written to Qdrant!")
    
    main(dry_run=dry_run)