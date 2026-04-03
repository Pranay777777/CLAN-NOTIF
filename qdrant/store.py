from qdrant_client.models import Distance, PointIdsList, PointStruct, VectorParams

from .client import get_collection_name, get_qdrant_client


def ensure_collection(vector_size: int = 384):
    client = get_qdrant_client()
    collection = get_collection_name()
    existing = [c.name for c in client.get_collections().collections]
    if collection not in existing:
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )


def recreate_collection(vector_size: int = 384):
    client = get_qdrant_client()
    collection = get_collection_name()
    existing = [c.name for c in client.get_collections().collections]
    if collection in existing:
        client.delete_collection(collection)

    client.create_collection(
        collection_name=collection,
        vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
    )


def upsert_video(video_id: int, vector: list[float], payload: dict):
    ensure_collection(vector_size=len(vector))
    point = PointStruct(id=int(video_id), vector=vector, payload=payload)
    get_qdrant_client().upsert(
        collection_name=get_collection_name(),
        points=[point],
    )


def upsert_videos(points: list[dict]):
    if not points:
        return

    vector_size = len(points[0]["vector"])
    ensure_collection(vector_size=vector_size)

    batch = [
        PointStruct(
            id=int(item["id"]),
            vector=item["vector"],
            payload=item["payload"],
        )
        for item in points
    ]
    get_qdrant_client().upsert(
        collection_name=get_collection_name(),
        points=batch,
    )


def delete_video(video_id: int):
    get_qdrant_client().delete(
        collection_name=get_collection_name(),
        points_selector=PointIdsList(points=[int(video_id)]),
    )


def reload_all_videos(points: list[dict]):
    if points:
        recreate_collection(vector_size=len(points[0]["vector"]))
    else:
        recreate_collection(vector_size=384)
    upsert_videos(points)
