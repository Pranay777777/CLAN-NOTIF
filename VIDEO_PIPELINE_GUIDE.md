"""
COMPLETE GUIDE: Video Metadata to Qdrant Storage Pipeline
==========================================================

This system automatically extracts video metadata and stores it to Qdrant
without any Excel dependencies. When a new video is uploaded to ./videos,
it is automatically extracted and stored to both collections.

COMPONENTS:
-----------
1. extract_final.py             - Video extraction (OCR + Whisper)
2. video_metadata_qdrant_storage.py - Qdrant storage handler
3. video_upload_watcher.py      - File system watcher for auto-processing
4. verify_qdrant_storage.py     - Verification script

WORKFLOW:
---------
Video Upload Flow:

    ./videos folder
         |
         v
    video_upload_watcher.py (monitors folder)
         |
         v
    Detects new .mp4/.mov/.avi file
         |
         v
    Waits for upload to complete (file size stable)
         |
         v
    Extracts metadata:
         - Screen text (OCR using Unstructured AI)
         - Audio transcript (Whisper or Sarvam)
         - Duration
         |
         v
    Stores to TWO collections simultaneously:
         |
         +---> clan_videos collection
         |     (video_id, title, transcript, screen_text, duration, creator, language)
         |
         +---> branch_mappings collection
               (type: "video", key: video_id, branch, region)
         |
         v
    Tracks processing in logs/video_processing_history.json
    Logs all operations to logs/video_watcher.log


USAGE:
------

1. MONITOR & AUTO-PROCESS (RECOMMENDED):
   python video_upload_watcher.py --watch
   
   This starts a background watcher that:
   - Monitors ./videos folder
   - Automatically processes new videos
   - Stores to Qdrant in both collections
   - Tracks processing history

2. PROCESS EXISTING VIDEOS:
   python video_upload_watcher.py --process-existing
   
   Processes any unprocessed videos already in ./videos folder

3. VIEW PROCESSING HISTORY:
   python video_upload_watcher.py --history
   
   Shows all processed videos with status and errors

4. DIRECT STORAGE (without watcher):
   
   from video_metadata_qdrant_storage import store_video_to_qdrant
   
   metadata = {
       "video_id": 123,
       "video_name": "my_video",
       "title": "How to Close More Deals",
       "transcript": "<full transcript>",
       "screen_text": "<extracted text>",
       "duration": "12:34",
       "creator_name": "John Doe",
       "language": "en",
       "branch": "Mumbai",
       "region": "APTL 3"
   }
   
   results = store_video_to_qdrant(metadata)
   print(results)  # {'clan_videos': True, 'branch_mappings': True}


COLLECTION FORMATS:
-------------------

clan_videos Collection:
{
    "id": <unique_id>,
    "vector": <384-dimensional embedding>,
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

branch_mappings Collection:
{
    "id": <unique_id>,
    "vector": <384-dimensional embedding>,
    "payload": {
        "type": "video",
        "key": <video_id>,
        "branch": <str>,
        "region": <str>,
        "title": <str>,
        "created_at": <timestamp>,
        "metadata_source": "video_upload"
    }
}


CURRENT STATUS:
---------------
✓ clan_videos: 22 points (embeddings: 384-dim)
✓ branch_mappings: 43 points (embeddings: 384-dim)
✓ Qdrant Server: http://172.20.3.65:6333 (remote mode)
✓ Embedding Model: sentence-transformers all-MiniLM-L6-v2
✓ No Excel file dependencies


NEXT STEPS:
-----------
1. Upload a new video to ./videos folder
2. Run: python video_upload_watcher.py --watch
3. Monitor logs/video_watcher.log for processing
4. Verify data with: python verify_qdrant_storage.py


DEPENDENCIES INSTALLED:
-----------------------
- sentence-transformers (embeddings)
- qdrant-client (Qdrant storage)
- watchdog (file system monitoring)
- opencv-python (video extraction)
- unstructured (OCR)
- openai-whisper (audio extraction)


TROUBLESHOOTING:
----------------

Issue: "watchdog not installed"
Fix: pip install watchdog

Issue: "Unicode errors in logs"
Fix: Internal formatting issue, doesn't affect functionality
      Set environment variable: PYTHONIOENCODING=utf-8

Issue: "Video not detected"
Fix: Ensure file is .mp4/.mov/.avi/.mkv/.m4v
     Minimum file size: 1 MB
     Check logs/video_watcher.log for details

Issue: "Qdrant connection failed"
Fix: Verify Qdrant server: telnet 172.20.3.65 6333
     Or check: curl http://172.20.3.65:6333/health


MONITORING:
-----------

Real-time logs:
  tail -f logs/video_watcher.log

Processing history:
  cat logs/video_processing_history.json

Qdrant collection stats:
  python verify_qdrant_storage.py
"""

print(__doc__)

if __name__ == "__main__":
    print("\n" + "="*80)
    print("VIDEO METADATA STORAGE PIPELINE - DOCUMENTATION")
    print("="*80)
