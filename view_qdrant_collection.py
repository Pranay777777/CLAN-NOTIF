"""
Script to view all content in a Qdrant collection
"""

from qdrant.client import get_qdrant_client, get_collection_name


def view_collection():
    """View all points in the Qdrant collection"""
    client = get_qdrant_client()
    collection = get_collection_name()
    
    print(f"\n{'='*80}")
    print(f"VIEWING COLLECTION: {collection}")
    print(f"{'='*80}\n")
    
    offset = None
    total_points = 0
    
    while True:
        # Scroll through all points
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=100,
            offset=offset,
            with_payload=True,
            with_vectors=False,  # Don't fetch vectors (too much data)
        )
        
        for point in points:
            total_points += 1
            payload = point.payload or {}
            print(f"ID: {point.id}")
            print(f"  Video ID: {payload.get('video_id')}")
            print(f"  Title: {payload.get('title')}")
            print(f"  Language: {payload.get('language')}")
            print()
        
        if next_offset is None:
            break
        offset = next_offset
    
    print(f"{'='*80}")
    print(f"TOTAL POINTS: {total_points}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    view_collection()
