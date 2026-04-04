from qdrant_client.models import Filter

from .client import get_collection_name, get_qdrant_client


def query_points(query_vector, limit: int = 30, query_filter: Filter | None = None):
    client = get_qdrant_client()
    return client.query_points(
        collection_name=get_collection_name(),
        query=query_vector,
        limit=limit,
        query_filter=query_filter,
    ).points


def scroll_points(limit: int = 100, offset=None, with_payload: bool = True, with_vectors: bool = False):
    client = get_qdrant_client()
    print("client", client)
    return client.scroll(
        collection_name=get_collection_name(),
        limit=limit,
        offset=offset,
        with_payload=with_payload,
        with_vectors=with_vectors,
    )


def get_all_video_ids() -> set[int]:
    ids: set[int] = set()
    offset = None

    while True:
        points, next_offset = scroll_points(
            limit=200,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )

        for point in points:
            raw_id = point.payload.get("video_id")
            try:
                ids.add(int(raw_id))
            except (TypeError, ValueError):
                continue

        if next_offset is None:
            break
        offset = next_offset

    return ids
