import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from qdrant_client import QdrantClient

load_dotenv()


def _get_qdrant_url() -> str:
    url = os.getenv("QDRANT_URL", "").strip()
    if url:
        return url

    host = os.getenv("QDRANT_HOST", "127.0.0.1").strip()
    port = os.getenv("QDRANT_PORT", "6333").strip()
    return f"http://{host}:{port}"


def _get_qdrant_path() -> str:
    # Use local embedded Qdrant storage by default.
    default_path = Path(__file__).resolve().parent.parent / "qdrant_storage"
    return os.getenv("QDRANT_PATH", str(default_path)).strip() or str(default_path)


def get_collection_name() -> str:
    return os.getenv("QDRANT_COLLECTION", "clan_videos").strip() or "clan_videos"


@lru_cache(maxsize=1)
def get_qdrant_client() -> QdrantClient:
    mode = os.getenv("QDRANT_MODE", "local").strip().lower()
    timeout = float(os.getenv("QDRANT_TIMEOUT", "30"))

    if mode == "remote":
        url = _get_qdrant_url()
        api_key = os.getenv("QDRANT_API_KEY", "").strip()
        kwargs: dict = {"url": url, "timeout": timeout}
        if api_key:
            kwargs["api_key"] = api_key
        return QdrantClient(**kwargs)

    # Default: local embedded storage
    path = _get_qdrant_path()
    return QdrantClient(path=path, timeout=timeout)
