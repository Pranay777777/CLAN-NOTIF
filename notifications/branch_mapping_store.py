import hashlib
from pathlib import Path

import pandas as pd
from qdrant_client.models import Distance, PointStruct, VectorParams

from qdrant.client import get_qdrant_client


BRANCH_MAPPING_COLLECTION = "branch_mappings"


def _to_norm(value) -> str:
    return str(value or "").strip().lower()


def _point_id(key: str) -> int:
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def ensure_branch_mapping_collection() -> None:
    client = get_qdrant_client()
    existing = {c.name for c in client.get_collections().collections}
    if BRANCH_MAPPING_COLLECTION in existing:
        return

    client.create_collection(
        collection_name=BRANCH_MAPPING_COLLECTION,
        vectors_config=VectorParams(size=1, distance=Distance.COSINE),
    )


def upsert_branch_mappings_from_excel(excel_path: str) -> dict:
    if not Path(excel_path).exists():
        raise FileNotFoundError(f"Branch mapping file not found: {excel_path}")

    df = pd.read_excel(excel_path)
    points: list[PointStruct] = []

    for _, row in df.iterrows():
        branch = str(row.get("Branch", "")).strip()
        content_id = str(row.get("Content_id", "")).strip()
        code = str(row.get("code", "")).strip()

        if not branch:
            continue

        if content_id:
            key = f"id:{_to_norm(content_id)}"
            points.append(
                PointStruct(
                    id=_point_id(key),
                    vector=[0.0],
                    payload={
                        "type": "content_id",
                        "key": _to_norm(content_id),
                        "branch": branch,
                        "source": excel_path,
                    },
                )
            )

        if code:
            key = f"code:{_to_norm(code)}"
            points.append(
                PointStruct(
                    id=_point_id(key),
                    vector=[0.0],
                    payload={
                        "type": "code",
                        "key": _to_norm(code),
                        "branch": branch,
                        "source": excel_path,
                    },
                )
            )

    ensure_branch_mapping_collection()
    if points:
        get_qdrant_client().upsert(
            collection_name=BRANCH_MAPPING_COLLECTION,
            points=points,
        )

    return {
        "rows": int(len(df)),
        "stored_points": len(points),
        "collection": BRANCH_MAPPING_COLLECTION,
    }


def load_branch_maps_from_qdrant() -> tuple[dict, dict, int]:
    ensure_branch_mapping_collection()

    client = get_qdrant_client()
    branch_map_id: dict[str, str] = {}
    branch_map_code: dict[str, str] = {}

    offset = None
    total_points = 0

    while True:
        points, next_offset = client.scroll(
            collection_name=BRANCH_MAPPING_COLLECTION,
            limit=500,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for point in points:
            payload = point.payload or {}
            key_type = _to_norm(payload.get("type"))
            key_value = _to_norm(payload.get("key"))
            branch = str(payload.get("branch", "")).strip()

            if not key_value or not branch:
                continue

            if key_type == "content_id":
                branch_map_id[key_value] = branch
            elif key_type == "code":
                branch_map_code[key_value] = branch

            total_points += 1

        if next_offset is None:
            break
        offset = next_offset

    return branch_map_id, branch_map_code, total_points
