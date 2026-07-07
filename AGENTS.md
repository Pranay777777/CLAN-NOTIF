# AGENTS.md

## Cursor Cloud specific instructions

### Product Overview
CLAN Video Recommendation API — a FastAPI service for personalized sales training video recommendations with notification campaigns. Uses PostgreSQL (relational data), Qdrant (vector search), and Google Gemini (AI copy generation).

### Running the API Server
```
.venv/bin/python -m uvicorn api:app --host 127.0.0.1 --port 8000
```
The SentenceTransformer model (`all-MiniLM-L6-v2`) is downloaded on first startup (~80 MB), which adds ~10-15 seconds to the initial boot.

### Required Services
| Service | Notes |
|---------|-------|
| **PostgreSQL** | Must be running. Start with `sudo pg_ctlcluster 16 main start`. DB: `clan_development`, user: `clan_dev`, password: `devpassword`. |
| **Qdrant** | Uses local file-based mode (`./qdrant_storage`). No separate server needed. Collection `clan_videos` must exist (run `setup_collection.py` once if missing). |

### Environment Variables
Copy `.env.example` to `.env`. For local dev, `GEMINI_API_KEY` can be set to `placeholder` — the API falls back to template-based notifications without it. PostgreSQL connection vars (`PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`, `DATABASE_URL`) are required.

### Gotchas
- **Qdrant local lock**: Only one process can access `./qdrant_storage` at a time. If you need to run `setup_collection.py` or other Qdrant scripts, stop the API server first.
- **`database/db_config.py` crashes on import** if `DATABASE_URL` is not set — many modules import it at the top level, so ensure `.env` is present before running anything.
- **`from whisper import model`** in `api.py` is shadowed by the `SentenceTransformer` assignment on line 87 (pre-existing F811 lint warning; do not "fix" this).

### Running Tests
```
.venv/bin/python -m pytest tests/test_video_selector_language.py tests/test_day1_day7_engine.py tests/test_resolver_send_notifications_days.py tests/test_day3_selector_and_day5_copy.py tests/test_day2_selector_branch_language.py -v
```
Some test files under `tests/` (e.g., `test_campaign_notifications_batch_api.py`) call `raise SystemExit(1)` at module level and will crash pytest collection. Run only the specific test files listed above.

### Linting
No formal linter is configured. Use `ruff check --select=E,F --ignore=F401,E501` for basic checks.

### Key Endpoints (for smoke-testing)
- `GET /docs` — Swagger UI
- `GET /indicators` — Lists active KII codes (reads from PostgreSQL)
- `GET /videos` — Lists indexed videos from Qdrant
- `POST /recommend-video` — Core recommendation endpoint (requires Qdrant data)
