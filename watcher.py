import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

# ── SETTINGS ──────────────────────────────────────────
WATCH_DIR = "./new_videos"
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".m4v"}
STATE_FILE = "./watcher_state.json"
# ──────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/clan_watcher.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("watcher")


def is_video_file(file_path: str) -> bool:
    return os.path.splitext(file_path)[1].lower() in VIDEO_EXTENSIONS


def list_video_files(folder: str):
    files = []
    if not os.path.isdir(folder):
        return files
    for name in os.listdir(folder):
        full = os.path.join(folder, name)
        if os.path.isfile(full) and is_video_file(full):
            files.append(full)
    return sorted(files)


def wait_for_upload_complete(file_path: str, settle_seconds: int):
    """Wait until file size remains stable for settle_seconds."""
    logger.info("Checking upload completion: %s", os.path.basename(file_path))

    last_size = -1
    stable_since = None

    while True:
        if not os.path.exists(file_path):
            logger.warning("File disappeared during upload wait: %s", file_path)
            return False

        size = os.path.getsize(file_path)
        now = time.time()

        if size == last_size:
            if stable_since is None:
                stable_since = now
            elapsed_stable = now - stable_since
            if elapsed_stable >= settle_seconds:
                logger.info("Upload stable for %ss: %s", settle_seconds, os.path.basename(file_path))
                return True
        else:
            stable_since = None
            last_size = size

        time.sleep(1)


def _file_fingerprint(file_path: str) -> str:
    """Create a stable identity for a file version."""
    stat = os.stat(file_path)
    return f"{stat.st_size}:{int(stat.st_mtime_ns)}"


def load_processed_state(state_file: str) -> dict[str, str]:
    if not os.path.exists(state_file):
        return {}

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            # Keep only string:string pairs to avoid corrupt schema drift.
            return {
                str(k): str(v)
                for k, v in payload.items()
                if isinstance(k, str) and isinstance(v, str)
            }
    except Exception as exc:
        logger.warning("Failed to load state file %s: %s", state_file, exc)

    return {}


def save_processed_state(state_file: str, processed: dict[str, str]):
    os.makedirs(os.path.dirname(os.path.abspath(state_file)), exist_ok=True)
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(processed, f, indent=2, sort_keys=True)
    os.replace(tmp, state_file)


def run_pipeline(video_path: str, dry_run: bool, reuse_existing_content: bool) -> bool:
    cmd = [sys.executable, "pipeline.py", "--video", video_path]
    if dry_run:
        cmd.append("--dry-run")
    if reuse_existing_content:
        cmd.append("--reuse-existing-content")

    logger.info("Triggering pipeline for: %s", os.path.basename(video_path))
    logger.info("Pipeline command: %s", " ".join(cmd))
    start = time.time()

    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - start, 2)

    if proc.returncode == 0:
        logger.info("Pipeline complete for %s in %ss", os.path.basename(video_path), elapsed)
        if proc.stdout.strip():
            logger.info("Pipeline stdout tail:\n%s", "\n".join(proc.stdout.strip().splitlines()[-10:]))
        return True
    else:
        logger.error("Pipeline failed for %s in %ss", os.path.basename(video_path), elapsed)
        if proc.stdout.strip():
            logger.error("Pipeline stdout tail:\n%s", "\n".join(proc.stdout.strip().splitlines()[-20:]))
        if proc.stderr.strip():
            logger.error("Pipeline stderr tail:\n%s", "\n".join(proc.stderr.strip().splitlines()[-20:]))
        return False


def watch_folder(
    folder: str,
    poll_seconds: int,
    settle_seconds: int,
    dry_run: bool,
    reuse_existing_content: bool,
    once: bool,
    state_file: str,
):
    os.makedirs(folder, exist_ok=True)

    processed = load_processed_state(state_file)
    logger.info("Watching folder: %s", os.path.abspath(folder))
    logger.info("Ready. Drop videos to auto-process.")
    logger.info("Settings | poll_seconds=%s | settle_seconds=%s | dry_run=%s | reuse_existing_content=%s",
                poll_seconds, settle_seconds, dry_run, reuse_existing_content)
    logger.info("Loaded %s processed file entries from %s", len(processed), state_file)

    while True:
        files = list_video_files(folder)

        for file_path in files:
            abs_path = os.path.abspath(file_path)
            current_fingerprint = _file_fingerprint(file_path)
            if processed.get(abs_path) == current_fingerprint:
                continue

            logger.info("New file detected: %s", os.path.basename(file_path))

            ok = wait_for_upload_complete(file_path, settle_seconds=settle_seconds)
            if not ok:
                continue

            # Recompute after upload settles to avoid stale fingerprint.
            current_fingerprint = _file_fingerprint(file_path)

            succeeded = run_pipeline(file_path, dry_run=dry_run, reuse_existing_content=reuse_existing_content)
            if succeeded:
                processed[abs_path] = current_fingerprint
                save_processed_state(state_file, processed)
            else:
                logger.warning("Skipping state save for failed file: %s", os.path.basename(file_path))

            if once:
                logger.info("Once mode enabled. Exiting watcher after first processed file.")
                return

        time.sleep(poll_seconds)


def main():
    parser = argparse.ArgumentParser(description="Watch folder and trigger pipeline.py for new videos")
    parser.add_argument("--watch-dir", default=WATCH_DIR, help="Folder to watch for new videos")
    parser.add_argument("--poll-seconds", type=int, default=2, help="Polling interval in seconds")
    parser.add_argument("--settle-seconds", type=int, default=30, help="File size stability duration before processing")
    parser.add_argument("--dry-run", action="store_true", help="Pass --dry-run to pipeline")
    parser.add_argument("--reuse-existing-content", action="store_true", help="Pass --reuse-existing-content to pipeline")
    parser.add_argument("--once", action="store_true", help="Exit after first processed file")
    parser.add_argument("--state-file", default=STATE_FILE, help="JSON file to persist processed file state")
    args = parser.parse_args()

    logger.info("Watcher started at %s", datetime.now().isoformat(timespec="seconds"))
    watch_folder(
        folder=args.watch_dir,
        poll_seconds=args.poll_seconds,
        settle_seconds=args.settle_seconds,
        dry_run=args.dry_run,
        reuse_existing_content=args.reuse_existing_content,
        once=args.once,
        state_file=args.state_file,
    )


if __name__ == "__main__":
    main()
