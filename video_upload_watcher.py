"""
Video Upload Watcher
Monitors ./videos folder and auto-processes new videos:
1. Extracts metadata (OCR + Whisper)
2. Stores to both Qdrant collections (clan_videos + branch_mappings)
3. Tracks processing history
"""

import os
import logging
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Import extraction and storage functions
from extract_final import extract_screen_text, extract_audio_transcript, get_video_duration
from video_metadata_qdrant_storage import store_video_to_qdrant

# ── LOGGING ───────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/video_watcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('video_watcher')

# ── CONFIGURATION ──────────────────────────────────────
VIDEO_FOLDER = "./videos"
PROCESSING_HISTORY_FILE = "logs/video_processing_history.json"
VIDEO_EXTENSIONS = ('.mp4', '.mov', '.avi', '.mkv', '.m4v')
MIN_FILE_SIZE = 1024 * 1024  # 1 MB minimum to avoid partial uploads

# ──────────────────────────────────────────────────────

class ProcessingHistory:
    """Track video processing history"""
    
    def __init__(self, history_file: str = PROCESSING_HISTORY_FILE):
        self.history_file = history_file
        self.data = self._load()
    
    def _load(self) -> Dict:
        """Load history from file"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load history: {e}")
        return {"processed": {}}
    
    def _save(self):
        """Save history to file"""
        try:
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
            with open(self.history_file, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save history: {e}")
    
    def is_processed(self, video_file: str) -> bool:
        """Check if video was already processed"""
        return video_file in self.data.get("processed", {})
    
    def mark_processing(self, video_file: str, status: str = "in_progress"):
        """Mark video as being processed"""
        if "processed" not in self.data:
            self.data["processed"] = {}
        
        self.data["processed"][video_file] = {
            "status": status,
            "started_at": datetime.now().isoformat(),
            "completed_at": None
        }
        self._save()
    
    def mark_complete(self, video_file: str, status: str = "success", errors: List[str] = None):
        """Mark video as processed"""
        if "processed" not in self.data:
            self.data["processed"] = {}
        
        entry = self.data["processed"].get(video_file, {})
        entry.update({
            "status": status,
            "completed_at": datetime.now().isoformat(),
            "errors": errors or []
        })
        self.data["processed"][video_file] = entry
        self._save()


class VideoUploadHandler(FileSystemEventHandler):
    """Handle video file uploads"""
    
    def __init__(self, history: ProcessingHistory):
        self.history = history
        self.processing = set()  # Track currently processing files
    
    def on_created(self, event):
        """Handle new file creation"""
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)
        
        # Check if it's a video file
        if not file_name.lower().endswith(VIDEO_EXTENSIONS):
            return
        
        logger.info(f"🎬 New file detected: {file_name}")
        
        # Wait for file to be fully uploaded (size stable)
        if self._wait_for_file_stability(file_path):
            self._process_video(file_path)
    
    def _wait_for_file_stability(self, file_path: str, timeout: int = 30) -> bool:
        """Wait for file size to stabilize (upload complete)"""
        file_name = os.path.basename(file_path)
        
        try:
            prev_size = 0
            stable_count = 0
            
            for _ in range(timeout):
                if not os.path.exists(file_path):
                    return False
                
                current_size = os.path.getsize(file_path)
                
                if current_size < MIN_FILE_SIZE:
                    logger.info(f"⏳ {file_name}: Waiting for upload ({current_size} bytes)...")
                    time.sleep(2)
                    continue
                
                if current_size == prev_size:
                    stable_count += 1
                    if stable_count >= 3:  # Stable for 6 seconds
                        logger.info(f"✓ {file_name}: Upload complete ({current_size} bytes)")
                        return True
                else:
                    stable_count = 0
                    prev_size = current_size
                
                time.sleep(2)
            
            logger.warning(f"⚠️ {file_name}: Timeout waiting for upload stability")
            return False
            
        except Exception as e:
            logger.error(f"✗ Error checking file stability: {e}")
            return False
    
    def _process_video(self, file_path: str):
        """Extract and store video metadata"""
        file_name = os.path.basename(file_path)
        video_name = os.path.splitext(file_name)[0]
        
        # Prevent duplicate processing
        if self.history.is_processed(file_name):
            logger.info(f"⊘ {file_name}: Already processed, skipping")
            return
        
        if file_name in self.processing:
            logger.warning(f"⊘ {file_name}: Already processing, skipping duplicate")
            return
        
        self.processing.add(file_name)
        self.history.mark_processing(file_name, "in_progress")
        
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"PROCESSING VIDEO: {file_name}")
            logger.info(f"{'='*80}\n")
            
            # Extract metadata
            logger.info("📋 Step 1: Extracting screen text (OCR)...")
            screen_text = extract_screen_text(file_path)
            logger.info(f"✓ OCR complete: {len(screen_text)} chars extracted")
            
            logger.info("📋 Step 2: Extracting audio transcript...")
            transcript = extract_audio_transcript(file_path)
            logger.info(f"✓ Transcript complete: {len(transcript)} chars extracted")
            
            logger.info("📋 Step 3: Getting video duration...")
            duration = get_video_duration(file_path)
            logger.info(f"✓ Duration: {duration}")
            
            # Prepare metadata
            video_id = abs(hash(file_name)) % (2**31)
            metadata = {
                "video_id": video_id,
                "video_name": video_name,
                "title": video_name.replace('_', ' ').title(),
                "transcript": transcript,
                "screen_text": screen_text,
                "duration": duration,
                "creator_name": "Unknown",
                "language": "en",
                "branch": "Global",
                "region": "APTL 3"
            }
            
            # Store to Qdrant
            logger.info("📋 Step 4: Storing to Qdrant collections...")
            results = store_video_to_qdrant(metadata)
            
            # Mark as complete
            errors = []
            if not results.get('clan_videos'):
                errors.append("Failed to store in clan_videos")
            if not results.get('branch_mappings'):
                errors.append("Failed to store in branch_mappings")
            
            status = "success" if not errors else "partial_success"
            self.history.mark_complete(file_name, status, errors)
            
            logger.info(f"\n{'='*80}")
            logger.info("PROCESSING COMPLETE ✓")
            logger.info(f"{'='*80}")
            logger.info(f"✓ clan_videos: {'SUCCESS' if results['clan_videos'] else 'FAILED'}")
            logger.info(f"✓ branch_mappings: {'SUCCESS' if results['branch_mappings'] else 'FAILED'}")
            logger.info(f"{'='*80}\n")
            
            if status == "success":
                logger.info(f"🎉 Video '{file_name}' successfully processed and stored!")
            else:
                logger.warning(f"⚠️  Video '{file_name}' processed with errors: {errors}")
            
        except Exception as e:
            logger.error(f"\n✗ ERROR processing video: {e}")
            logger.exception(e)
            self.history.mark_complete(file_name, "failed", [str(e)])
        
        finally:
            self.processing.discard(file_name)


class VideoWatcher:
    """Watch for video uploads and process them"""
    
    def __init__(self, video_folder: str = VIDEO_FOLDER):
        self.video_folder = video_folder
        self.history = ProcessingHistory()
        self.observer = None
    
    def start(self):
        """Start watching for video uploads"""
        os.makedirs(self.video_folder, exist_ok=True)
        
        logger.info("\n" + "="*80)
        logger.info("VIDEO UPLOAD WATCHER STARTED")
        logger.info("="*80)
        logger.info(f"Watching folder: {self.video_folder}")
        logger.info(f"Video extensions: {', '.join(VIDEO_EXTENSIONS)}")
        logger.info(f"Processing history: {PROCESSING_HISTORY_FILE}")
        logger.info("="*80 + "\n")
        
        handler = VideoUploadHandler(self.history)
        self.observer = Observer()
        self.observer.schedule(handler, self.video_folder, recursive=False)
        self.observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop watching"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
        logger.info("\n🛑 Video upload watcher stopped")
    
    def process_existing_videos(self):
        """Process any existing videos in the folder"""
        logger.info("\n📂 Scanning for existing videos...")
        
        video_files = [
            f for f in os.listdir(self.video_folder)
            if f.lower().endswith(VIDEO_EXTENSIONS)
        ]
        
        if not video_files:
            logger.info("✓ No videos found")
            return
        
        logger.info(f"✓ Found {len(video_files)} video(s)")
        
        handler = VideoUploadHandler(self.history)
        for video_file in video_files:
            if not self.history.is_processed(video_file):
                file_path = os.path.join(self.video_folder, video_file)
                logger.info(f"→ Processing existing video: {video_file}")
                handler._process_video(file_path)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Video Upload Watcher")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Start watching for new videos (runs continuously)"
    )
    parser.add_argument(
        "--process-existing",
        action="store_true",
        help="Process existing videos in folder"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Show processing history"
    )
    
    args = parser.parse_args()
    
    watcher = VideoWatcher()
    
    if args.history:
        print("\n" + "="*80)
        print("VIDEO PROCESSING HISTORY")
        print("="*80)
        for video_file, entry in watcher.history.data.get("processed", {}).items():
            print(f"\n📹 {video_file}")
            print(f"   Status: {entry.get('status')}")
            print(f"   Started: {entry.get('started_at')}")
            print(f"   Completed: {entry.get('completed_at')}")
            if entry.get('errors'):
                print(f"   Errors: {entry.get('errors')}")
        print("="*80 + "\n")
        return
    
    if args.process_existing:
        watcher.process_existing_videos()
        return
    
    if args.watch:
        watcher.start()
    else:
        # Default: process existing then watch
        watcher.process_existing_videos()
        watcher.start()


if __name__ == "__main__":
    main()
