"""
Video Metadata Storage to Qdrant
Stores video metadata directly to Qdrant collections (clan_videos and branch_mappings)
with proper formatting for both collections
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import os

logger = logging.getLogger('video_metadata_storage')

# ── QDRANT CONFIGURATION ──────────────────────────────
QDRANT_URL = os.getenv("QDRANT_URL", "http://172.20.3.65:6333")
QDRANT_MODE = os.getenv("QDRANT_MODE", "remote").lower()

# Collections
CLAN_VIDEOS_COLLECTION = "clan_videos"
BRANCH_MAPPINGS_COLLECTION = "branch_mappings"

# Embedding model
EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
EMBEDDING_DIM = 384

# ──────────────────────────────────────────────────────

class VideoMetadataQdrantStorage:
    """
    Stores video metadata to Qdrant in both collections
    """
    
    def __init__(self):
        """Initialize Qdrant client"""
        try:
            if QDRANT_MODE == "remote":
                self.client = QdrantClient(url=QDRANT_URL)
            else:
                self.client = QdrantClient(path="./qdrant_storage")
            
            logger.info(f"✓ Qdrant client initialized ({QDRANT_MODE} mode)")
            self._verify_collections()
        except Exception as e:
            logger.error(f"✗ Failed to initialize Qdrant: {e}")
            raise
    
    def _verify_collections(self):
        """Verify required collections exist"""
        try:
            collections = [c.name for c in self.client.get_collections().collections]
            
            if CLAN_VIDEOS_COLLECTION not in collections:
                logger.warning(f"! Collection '{CLAN_VIDEOS_COLLECTION}' not found")
            else:
                logger.info(f"✓ Collection '{CLAN_VIDEOS_COLLECTION}' exists")
            
            if BRANCH_MAPPINGS_COLLECTION not in collections:
                logger.warning(f"! Collection '{BRANCH_MAPPINGS_COLLECTION}' not found")
            else:
                logger.info(f"✓ Collection '{BRANCH_MAPPINGS_COLLECTION}' exists")
                
        except Exception as e:
            logger.error(f"✗ Error verifying collections: {e}")
    
    def _generate_embedding(self, text: str) -> list:
        """Generate embedding using SentenceTransformer"""
        try:
            embedding = EMBEDDING_MODEL.encode(text, convert_to_tensor=False)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"✗ Error generating embedding: {e}")
            return [0.0] * EMBEDDING_DIM
    
    def store_to_clan_videos(
        self,
        video_id: Optional[int] = None,
        title: str = "",
        transcript: str = "",
        screen_text: str = "",
        duration: str = "0:00",
        creator_name: str = "Unknown",
        language: str = "en"
    ) -> bool:
        """
        Store video metadata to clan_videos collection
        
        Format:
        {
            "id": <unique_id>,
            "vector": <384-dim embedding>,
            "payload": {
                "video_id": <int>,
                "title": <str>,
                "transcript": <str>,
                "screen_text": <str>,
                "duration": <str>,
                "creator_name": <str>,
                "language": <str>,
                "created_at": <timestamp>,
                "metadata_source": "video_upload"
            }
        }
        """
        try:
            # Generate unique ID
            point_id = abs(hash(f"{title}_{datetime.now().timestamp()}")) % (2**31)
            
            # Create search text from title + transcript for embedding
            search_text = f"{title} {transcript}".strip()[:500]  # Limit to 500 chars
            embedding = self._generate_embedding(search_text if search_text else title)
            
            payload = {
                "video_id": video_id or point_id,
                "title": title,
                "transcript": transcript,
                "screen_text": screen_text,
                "duration": duration,
                "creator_name": creator_name,
                "language": language,
                "created_at": datetime.now().isoformat(),
                "metadata_source": "video_upload"
            }
            
            point = PointStruct(
                id=point_id,
                vector=embedding,
                payload=payload
            )
            
            self.client.upsert(
                collection_name=CLAN_VIDEOS_COLLECTION,
                points=[point]
            )
            
            logger.info(f"✓ Stored to clan_videos | ID: {point_id} | Title: {title[:50]}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error storing to clan_videos: {e}")
            return False
    
    def store_to_branch_mappings(
        self,
        video_id: int,
        title: str = "",
        branch: str = "Global",
        region: str = "APTL 3"
    ) -> bool:
        """
        Store video metadata to branch_mappings collection
        
        Format:
        {
            "id": <unique_id>,
            "vector": <384-dim embedding>,
            "payload": {
                "type": "video",
                "key": <video_id>,
                "branch": <branch_name>,
                "region": <region>,
                "title": <str>,
                "created_at": <timestamp>,
                "metadata_source": "video_upload"
            }
        }
        """
        try:
            # Generate unique ID
            point_id = abs(hash(f"branch_{video_id}_{datetime.now().timestamp()}")) % (2**31)
            
            # Create search text for embedding
            search_text = f"{branch} {region} {title}".strip()[:500]
            embedding = self._generate_embedding(search_text if search_text else branch)
            
            payload = {
                "type": "video",
                "key": video_id,
                "branch": branch,
                "region": region,
                "title": title,
                "created_at": datetime.now().isoformat(),
                "metadata_source": "video_upload"
            }
            
            point = PointStruct(
                id=point_id,
                vector=embedding,
                payload=payload
            )
            
            self.client.upsert(
                collection_name=BRANCH_MAPPINGS_COLLECTION,
                points=[point]
            )
            
            logger.info(f"✓ Stored to branch_mappings | ID: {point_id} | Video: {video_id} | Branch: {branch}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error storing to branch_mappings: {e}")
            return False
    
    def store_video_metadata(
        self,
        video_id: int,
        title: str,
        transcript: str,
        screen_text: str,
        duration: str = "0:00",
        creator_name: str = "Unknown",
        language: str = "en",
        branch: str = "Global",
        region: str = "APTL 3"
    ) -> Dict[str, bool]:
        """
        Store video to both collections
        
        Returns:
            dict with status for each collection
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"STORING VIDEO TO QDRANT")
        logger.info(f"{'='*80}")
        logger.info(f"Video ID: {video_id}")
        logger.info(f"Title: {title}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Transcript length: {len(transcript)} chars")
        logger.info(f"Screen text length: {len(screen_text)} chars")
        logger.info(f"Creator: {creator_name}")
        logger.info(f"Language: {language}")
        logger.info(f"Branch: {branch}")
        logger.info(f"Region: {region}\n")
        
        results = {}
        
        # Store to clan_videos
        logger.info("📤 Storing to clan_videos collection...")
        results['clan_videos'] = self.store_to_clan_videos(
            video_id=video_id,
            title=title,
            transcript=transcript,
            screen_text=screen_text,
            duration=duration,
            creator_name=creator_name,
            language=language
        )
        
        # Store to branch_mappings
        logger.info("📤 Storing to branch_mappings collection...")
        results['branch_mappings'] = self.store_to_branch_mappings(
            video_id=video_id,
            title=title,
            branch=branch,
            region=region
        )
        
        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("STORAGE SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"✓ clan_videos: {'SUCCESS' if results['clan_videos'] else 'FAILED'}")
        logger.info(f"✓ branch_mappings: {'SUCCESS' if results['branch_mappings'] else 'FAILED'}")
        logger.info(f"{'='*80}\n")
        
        return results


def store_video_to_qdrant(metadata: Dict[str, Any]) -> Dict[str, bool]:
    """
    Convenience function to store video metadata to Qdrant
    
    Expected metadata dict:
    {
        "video_id": int,
        "video_name": str,
        "title": str,
        "transcript": str,
        "screen_text": str,
        "duration": str,
        "creator_name": str (optional),
        "language": str (optional),
        "branch": str (optional),
        "region": str (optional)
    }
    """
    try:
        storage = VideoMetadataQdrantStorage()
        
        results = storage.store_video_metadata(
            video_id=metadata.get('video_id', abs(hash(metadata.get('video_name', ''))) % (2**31)),
            title=metadata.get('title', metadata.get('video_name', 'Unknown')),
            transcript=metadata.get('transcript', ''),
            screen_text=metadata.get('screen_text', ''),
            duration=metadata.get('duration', '0:00'),
            creator_name=metadata.get('creator_name', 'Unknown'),
            language=metadata.get('language', 'en'),
            branch=metadata.get('branch', 'Global'),
            region=metadata.get('region', 'APTL 3')
        )
        
        return results
        
    except Exception as e:
        logger.error(f"✗ Fatal error storing video: {e}")
        return {'clan_videos': False, 'branch_mappings': False}


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[
            logging.FileHandler('logs/video_metadata_storage.log'),
            logging.StreamHandler()
        ]
    )
    
    # Test with sample video
    sample_metadata = {
        "video_id": 9999,
        "video_name": "test_video",
        "title": "How to Close More Deals",
        "transcript": "In this video, we'll learn the proven techniques to close more deals. First, understand the customer's need. Build rapport and trust. Present solutions confidently.",
        "screen_text": "Step 1: Understand customer needs\nStep 2: Build rapport\nStep 3: Present solutions\nStep 4: Close the deal",
        "duration": "12:34",
        "creator_name": "Sales Expert",
        "language": "en",
        "branch": "Test Branch",
        "region": "APTL 3"
    }
    
    results = store_video_to_qdrant(sample_metadata)
    print("\nTest Results:", results)
