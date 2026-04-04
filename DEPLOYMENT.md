# CLAN Video Recommendation System - Deployment & Operations Manual

**System Status**: Production Ready (Step 10 Complete)  
**Last Updated**: March 22, 2026  
**Database**: 21 videos with AI-enriched metadata  
**API Status**: Running on localhost:8000

---

## Table of Contents
1. [Quick Start](#quick-start)
2. [System Architecture](#system-architecture)
3. [Component Details](#component-details)
4. [Operational Procedures](#operational-procedures)
5. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
6. [Advanced Usage](#advanced-usage)

---

## Quick Start

### Prerequisites
- Python 3.12.10
- Windows PowerShell 5.1+
- 500MB disk space for Qdrant vector database
- Environment variables: `GEMINI_API_KEY`, `SARVAM_API_KEY`

### Setup (First Time)
```powershell
cd C:\Users\aim4g\Desktop\TMI\video...ex

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Verify environment variables are set
Write-Host $env:GEMINI_API_KEY
Write-Host $env:SARVAM_API_KEY
```

### Start Full System (Single Command)
```powershell
# Terminal 1: Start API server
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe ^
  -m uvicorn api:app --host localhost --port 8000

# Terminal 2: Start file watcher
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe ^
  .\watcher.py --watch-dir ./new_videos --poll-seconds 1 --settle-seconds 2 --reuse-existing-content
```

### Test System Health
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe .\quick_api_test.py
```

Expected output:
```
Total videos in database: 21
Status: 200 OK
All 21 videos loaded with enriched fields: YES
API VERIFICATION: PASS - All endpoints working
```

---

## System Architecture

### Core Pipeline (5 Steps)

```
Input Video
    ↓
[Step 1] Load Catalog + Match to existing record
    ↓
[Step 2] Extract Content (OCR, Whisper, reuse existing)
    ↓
[Step 3] Generate AI Metadata (Gemini 2.0-flash)
    ↓
[Step 4] Build Embedding + Payload (SentenceTransformers)
    ↓
[Step 5] Upsert to Qdrant Vector Database
    ↓
Output: Searchable, Personalized Recommendations
```

### Component Overview

| Component | Technology | Purpose | Status |
|-----------|-----------|---------|--------|
| **API Server** | FastAPI + Uvicorn | REST endpoints for video search & recommendations | Running |
| **Vector Database** | Qdrant (local) | 21 videos with 384-dim embeddings | Ready |
| **File Watcher** | watchdog | Auto-process incoming videos | Implemented |
| **Pipeline Orchestrator** | Python script | 5-step video processing | Implemented |
| **Metadata Generator** | Gemini 2.0-flash + LangChain | AI-enriched fields (summary, indicators, etc.) | Implemented |
| **Transcription** | Sarvam API + Whisper | Extract Telugu audio + English transcription | Implemented |
| **Embedding** | SentenceTransformers (all-MiniLM-L6-v2) | Generate vector representations | Ready |

---

## Component Details

### 1. FastAPI Server (`api.py`)

**Purpose**: Serve video recommendations and catalog data

**Endpoints**:
- `GET /` — Health check
- `GET /videos/sync` — Return all 21 videos with full metadata
- `POST /recommend-video` — Get personalized recommendation

**Start Command**:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  -m uvicorn api:app --host localhost --port 8000
```

**Log File**: `logs/clan_api.log`

**Example Request**:
```bash
curl -X POST http://localhost:8000/recommend-video \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 123,
    "user_name": "Aarav_Singh",
    "role": "RM",
    "region": "North",
    "journey_day": 8,
    "weak_indicator": "customer_generation",
    "watched_ids": [1, 2],
    "months_in_role": 2
  }'
```

**Response** (200 OK):
```json
{
  "video_id": "299",
  "title": "How did you overcome initial challenges...",
  "creator_name": "Priya Sharma",
  "summary": "...",
  "key_lesson": "...",
  "problem_solved": "customer_generation",
  "sales_phase": "acquisition",
  "experience_level": "new_joiner",
  "score": 0.689,
  "matched_indicator": "customer_generation"
}
```

**Request Validation**:
- `role` ∈ {RM, BM, SUPERVISOR}
- `journey_day` ∈ [1, 31]
- `weak_indicator` ∈ valid indicator list
- `user_name` non-empty
- `watched_ids` must be integers

---

### 2. File Watcher (`watcher.py`)

**Purpose**: Monitor `./new_videos` folder and auto-process uploads

**Command**:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\watcher.py --watch-dir ./new_videos --poll-seconds 1 --settle-seconds 2
```

**Flags**:
- `--watch-dir` — Folder to monitor (e.g., `./new_videos`)
- `--poll-seconds` — Check interval (default: 2)
- `--settle-seconds` — Wait time for upload completion (default: 3)
- `--once` — Process one file and exit (for testing)
- `--dry-run` — Skip Qdrant write (for validation)

**Log File**: `logs/clan_watcher.log`

**Normal Flow** (Dry-Run Test):
```powershell
# Terminal 1: Start watcher in test mode
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\watcher.py --watch-dir ./new_videos --poll-seconds 1 --settle-seconds 2 --dry-run --once

# Terminal 2: Drop a video
Copy-Item "./videos/Charan_Teja_-_An_Intro_720P.mp4" "./new_videos/"

# Output in Terminal 1:
# New file detected: Charan_Teja_-_An_Intro_720P.mp4
# Upload stable for 2s: Charan_Teja_-_An_Intro_720P.mp4
# Triggering pipeline...
# Pipeline complete in 28.52s
# DRY RUN mode, skipping Qdrant upsert
```

**Production Flow** (Actual Upsert):
```powershell
# Remove --dry-run to write to Qdrant
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\watcher.py --watch-dir ./new_videos --poll-seconds 1 --settle-seconds 2
```

---

### 3. Pipeline Orchestrator (`pipeline.py`)

**Purpose**: Single-video end-to-end processing (catalog match → extract → metadata → embed → upsert)

**Command**:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\pipeline.py --video "./videos/[NAME].mp4" --dry-run
```

**Flags**:
- `--video` — Path to video file (required)
- `--dry-run` — Skip Qdrant write (optional)

**Output**:
```
Step 1/5: Load catalog
Matched catalog title: Abhishek Sahu - An Intro

Step 2/5: Extract content
Extraction complete | screen_chars=424 | transcript_chars=810

Step 3/5: Generate metadata
Metadata complete | ai_generated=True
Lead indicators: loaded dynamically from PostgreSQL (account_id=14, status=1)
Sales phase: acquisition | Experience level: new_joiner

Step 4/5: Build embedding + payload
Payload fields: video_id, title, summary, key_lesson, problem_solved, sales_phase, experience_level

Step 5/5: [DRY RUN] Skipping Qdrant upsert
```

**Log File**: `logs/clan_pipeline.log`

---

### 4. Metadata Generation (`generate_metadata.py`)

**Purpose**: Call Gemini 2.0-flash to generate AI-enriched metadata

**Standalone Usage**:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\generate_metadata.py `
  --input ./video_catalog_with_content.xlsx `
  --output ./video_catalog_enriched.xlsx `
  --limit 5
```

**Flags**:
- `--output` — Output file to save enriched metadata
- `--limit` — Process only N videos (default: all)
- `--api-key` — Override GEMINI_API_KEY (optional)

**Output File Columns**:
- `ai_summary` — 300-600 char description
- `ai_lead_indicators` — JSON array of business indicators
- `ai_target_audience` — Audience segment
- `ai_difficulty` — Beginner/Intermediate/Advanced
- `ai_key_lesson` — Main takeaway
- `ai_sales_phase` — Pipeline stage (awareness/consideration/acquisition/retention)
- `ai_experience_level` — User level (new_joiner/experienced)
- `ai_problem_solved` — Core problem addressed
- `ai_generated` — Boolean (true if AI-generated, false if fallback)

Indicator Source of Truth
- Active indicator list is loaded from PostgreSQL using SQLAlchemy.
- Query basis: `public.kii_master` filtered by `account_id=14 AND status=1`.
- API and notifications should use `GET /indicators` for current valid indicator codes.

**Log File**: `logs/clan_metadata.log`

---

### 5. Vector Database (`qdrant_storage/`)

**Purpose**: Store embeddings and metadata for fast semantic search

**Current State**:
- 21 points (videos)
- 384-dimensional vectors
- COSINE similarity metric
- Enriched payloads (15 fields per point)

**Schema** (example):
```json
{
  "id": "288",
  "vector": [0.123, -0.045, ...],
  "payload": {
    "video_id": "288",
    "title": "Abhishek Sahu - An Intro",
    "creator_name": "Abhishek Sahu",
    "summary": "...",
    "key_lesson": "...",
    "problem_solved": "customer_generation",
    "sales_phase": "acquisition",
    "experience_level": "new_joiner",
    "lead_indicators": ["dynamic_from_db_indicator_codes"],
    ...
  }
}
```

**Access**:
```powershell
# Direct access (local file storage)
# Qdrant locks the database - only one client at a time
# API and watcher cannot run simultaneously

from qdrant_client import QdrantClient
client = QdrantClient(path='./qdrant_storage')
info = client.get_collection('clan_videos')
print(f'Points: {info.points_count}')
```

---

## Operational Procedures

### Daily/Weekly Tasks

#### 1. Check System Status
```powershell
# Test API health
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe ./quick_api_test.py
```

Expected result: `API VERIFICATION: PASS`

#### 2. Monitor Logs
```powershell
# Check recent API activity
Get-Content ./logs/clan_api.log -Tail 20

# Check watcher activity
Get-Content ./logs/clan_watcher.log -Tail 20

# Check metadata generation
Get-Content ./logs/clan_metadata.log -Tail 20
```

#### 3. Verify Database Integrity
```powershell
# After processing new video, verify Qdrant is updated
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe -c "
from qdrant_client import QdrantClient
client = QdrantClient(path='./qdrant_storage')
info = client.get_collection('clan_videos')
print(f'Total videos: {info.points_count}')
"
```

### Processing New Videos

#### Scenario 1: Single Video (Manual)
```powershell
# Validate video first (dry-run)
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\pipeline.py --video "./videos/NewVideo.mp4" --reuse-existing-content --dry-run

# If validated, run actual upsert
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\pipeline.py --video "./videos/NewVideo.mp4" --reuse-existing-content
```

#### Scenario 2: Batch Videos (Via Watcher)
```powershell
# Terminal 1: Start watcher
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\watcher.py --watch-dir ./new_videos --poll-seconds 1 --settle-seconds 2 --reuse-existing-content

# Terminal 2: Copy videos to ./new_videos (one at a time or in batch)
Copy-Item "./videos/Video1.mp4" "./new_videos/"
Copy-Item "./videos/Video2.mp4" "./new_videos/"

# Watcher will process each sequentially
# Check logs to monitor progress: Get-Content ./logs/clan_watcher.log -Tail 5 -Wait
```

#### Scenario 3: Reprocess All Videos
Use the DB-driven pipeline flow for reprocessing and then run payload sync:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\pipeline.py --video "./videos/[NAME].mp4"

Invoke-RestMethod -Uri "http://127.0.0.1:8000/notifications/admin/sync-indicators" -Method POST -Body '{"dry_run":false,"clear_unmapped":false}' -ContentType "application/json"
```

---

## Monitoring & Troubleshooting

### API Server Issues

**Issue**: Port 8000 already in use
```powershell
# Find and kill process
Get-Process python | Where-Object { $_.CommandLine -like '*uvicorn*' } | Stop-Process -Force
# Wait 5 seconds, retry
```

**Issue**: Qdrant database locked
```
RuntimeError: Storage folder ./qdrant_storage is already accessed by another instance
```
**Solution**: Stop API before running watcher, or vice versa
```powershell
# Only one client can access local Qdrant at a time
# Kill the blocking process: Get-Process python | Stop-Process -Force
```

**Issue**: API returns 422 on recommendation request
```
Validation Error: role not in enum
```
**Solution**: Check request JSON matches schema:
```json
{
  "user_id": 123,
  "user_name": "name",
  "role": "RM",           // Must be RM, BM, or SUPERVISOR
  "region": "North",
  "journey_day": 15,      // Must be 1-31
  "weak_indicator": "customer_generation",
  "watched_ids": [1, 2],
  "months_in_role": 6
}
```

### Watcher Issues

**Issue**: Watcher detects file but doesn't process
```
[ERROR] Triggering pipeline for: Video.mp4
[ERROR] Pipeline failed...
```
**Solution**: Check API is not holding Qdrant lock
```powershell
# Watcher needs exclusive Qdrant access
# Kill API: Get-Process python | Where-Object { $_.CommandLine -like '*api*' } | Stop-Process
```

**Issue**: File detected but not settling
```
Checking upload completion: Video.mp4
[not advancing]
```
**Solution**: Upload file is still being written. Wait or increase `--settle-seconds`:
```powershell
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\watcher.py --watch-dir ./new_videos --settle-seconds 5
```

### Database Issues

**Issue**: Qdrant corrupted or in bad state
```powershell
# Backup and delete
Rename-Item ./qdrant_storage ./qdrant_storage.bak
# Re-run pipeline for source videos and then sync payload fields
Invoke-RestMethod -Uri "http://127.0.0.1:8000/notifications/admin/sync-indicators" -Method POST -Body '{"dry_run":false,"clear_unmapped":false}' -ContentType "application/json"
```

**Issue**: Video not appearing in /videos/sync
```powershell
# Re-run pipeline for the specific video, then sync payload fields
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\pipeline.py --video "./videos/[NAME].mp4"

Invoke-RestMethod -Uri "http://127.0.0.1:8000/notifications/admin/sync-indicators" -Method POST -Body '{"dry_run":false,"clear_unmapped":false}' -ContentType "application/json"
```

### Performance Tuning

**Slow Recommendations**: Increase vector pool
- Currently embedding 21 videos (fast)
- At 100+ videos, consider Qdrant server mode

**Slow Metadata Generation**: Batch generate
```powershell
# Process in batches (GPU recommended)
c:/Users/aim4g/Desktop/TMI/video...ex/.venv/Scripts/python.exe `
  .\generate_metadata.py --limit 5
# Then repeat with next batch
```

---

## Advanced Usage

### Custom Scoring Formula (api.py)
Current weights:
```
score = (
  (base_score * 0.30) +
  (indicator_match * 0.25) +
  (problem_match * 0.20) +
  (experience_match * 0.15) +
  (sales_phase_match * 0.10) -
  recency_penalty
)
```

Edit `api.py` lines ~180-200 to adjust weights.

### Environment Variables
```powershell
# Set in .env or system environment
GEMINI_API_KEY=sk-...
SARVAM_API_KEY=...
QDRANT_PATH=./qdrant_storage
```

### Backup & Restore

**Backup Database**:
```powershell
Copy-Item -Recurse ./qdrant_storage ./qdrant_storage_backup_$(Get-Date -Format 'YYYYMMDD')
```

**Restore Database**:
```powershell
Remove-Item -Recurse ./qdrant_storage
Copy-Item -Recurse ./qdrant_storage_backup_20260322 ./qdrant_storage
# Restart API
```

---

## Testing Checklist

- [ ] API starts without errors: `python -m uvicorn api:app --host localhost --port 8000`
- [ ] GET / returns status 200
- [ ] GET /videos/sync returns 21 videos
- [ ] POST /recommend-video returns valid recommendation
- [ ] GET /indicators returns active DB indicators for account 14
- [ ] Invalid request returns 422 validation error
- [ ] POST /notifications/build returns `should_send=true` for a valid payload
- [ ] Watcher detects new files in ./new_videos
- [ ] Watcher processes file in <40 seconds
- [ ] Pipeline dry-run passes all 5 steps
- [ ] Pipeline (no --dry-run) writes to Qdrant
- [ ] New video appears in /videos/sync
- [ ] Recommendation scores include new video

---

## Support Contacts & Resources

- **Gemini API**: https://ai.google.dev/
- **Sarvam API**: Contact account manager
- **Qdrant Documentation**: https://qdrant.tech/documentation/
- **FastAPI Docs**: http://localhost:8000/docs (when API running)

Notification Validation via FastAPI Docs
1. Open Swagger UI at `http://localhost:8000/docs`.
2. Run `GET /indicators` and copy a valid indicator code.
3. Run `POST /notifications/build` with a supported journey day (2, 4, 5, 6, 10, 11, 12, 16).
4. Set `weak_indicator` to a code from step 2 and provide matching metrics for that day.
5. Validate response quality:
  - `should_send` is `true`
  - `notification_title` is non-empty
  - `notification_body` is non-empty
- **SentenceTransformers**: https://www.sbert.net/

---

**System Status**: ✅ Production Ready  
**Last Verified**: March 22, 2026  
**Videos in Database**: 21  
**API Health**: All endpoints validated
