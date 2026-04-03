import logging
import os
import json
import requests

logger = logging.getLogger("sender")

_LOG_FILE = "notification_logs/sent_log.json"


def _load_log() -> dict:
    os.makedirs("notification_logs", exist_ok=True)
    if not os.path.exists(_LOG_FILE):
        return {}
    with open(_LOG_FILE) as f:
        return json.load(f)


def _save_log(log: dict):
    with open(_LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)


def already_sent(user_id: int, content_id: str) -> bool:
    log = _load_log()
    return str(content_id) in log.get(str(user_id), [])


def mark_sent(user_id: int, content_id: str):
    log = _load_log()
    key = str(user_id)
    if key not in log:
        log[key] = []
    if str(content_id) not in log[key]:
        log[key].append(str(content_id))
    _save_log(log)


def send_notification(
    fcm_token:  str,
    title:      str,
    body:       str,
    content_id: str,
    dry_run:    bool = True
) -> bool:

    if dry_run:
        logger.info(
            "[DRY RUN] token=%s... | title=%s | content_id=%s",
            fcm_token[:20], title, content_id
        )
        return True

    server_key = os.getenv("FCM_SERVER_KEY", "").strip()
    if not server_key:
        logger.error("FCM_SERVER_KEY not set")
        return False

    try:
        resp = requests.post(
            "https://fcm.googleapis.com/fcm/send",
            headers={
                "Authorization": f"key={server_key}",
                "Content-Type":  "application/json",
            },
            json={
                "to": fcm_token,
                "notification": {"title": title, "body": body},
                "data": {
                    "content_id": str(content_id),
                    "type":       "video_recommendation",
                },
            },
            timeout=10,
        )
        if resp.status_code == 200 and resp.json().get("success") == 1:
            return True
        logger.warning("FCM response: %s", resp.text)
        return False
    except Exception as e:
        logger.error("FCM send error: %s", e)
        return False