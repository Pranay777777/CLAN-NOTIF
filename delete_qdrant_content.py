"""
Script to delete content from Qdrant collection
"""

from qdrant.client import get_qdrant_client, get_collection_name


def delete_all_points():
    """Delete ALL points from collection"""
    client = get_qdrant_client()
    collection = get_collection_name()
    
    print(f"\n{'='*60}")
    print(f"DELETING ALL POINTS from {collection}")
    print(f"{'='*60}")
    
    # Get current count
    collection_info = client.get_collection(collection)
    current_count = collection_info.points_count
    print(f"Current points: {current_count}")
    
    # Delete using filter (delete all)
    client.delete(
        collection_name=collection,
        points_selector=client.models.PointIdsList(
            ids=[]  # Empty list deletes all when used with rest API
        )
    )
    
    print("Option 1: Delete using batch delete (by IDs)")
    print("Code:")
    print("""
    from qdrant.client.models import PointIdsList
    
    client.delete(
        collection_name="branch_mappings",
        points_selector=PointIdsList(ids=[288, 289, 290])  # Delete specific IDs
    )
    """)
    
    print("\nOption 2: Delete using Qdrant API directly")
    print("Code:")
    print("""
    import requests
    
    requests.post(
        "http://172.20.3.65:6333/collections/branch_mappings/points/delete",
        json={"points": [288, 289, 290]}
    )
    """)
    
    print("\nOption 3: Delete entire collection")
    print("Code:")
    print("""
    client.delete_collection("branch_mappings")
    """)
    
    print("\nOption 4: Recreate collection (empties it)")
    print("Code:")
    print("""
    from qdrant.client.models import Distance, VectorParams
    
    # Delete old
    client.delete_collection("branch_mappings")
    
    # Create new empty collection
    client.recreate_collection(
        collection_name="branch_mappings",
        vectors_config=VectorParams(size=384, distance=Distance.COSINE),
    )
    """)


def delete_specific_points(point_ids: list):
    """Delete specific points by ID"""
    client = get_qdrant_client()
    collection = get_collection_name()
    
    print(f"\nDeleting {len(point_ids)} specific points: {point_ids}")
    
    client.delete(
        collection_name=collection,
        points_selector=client.models.PointIdsList(ids=point_ids)
    )
    
    print("✓ Deleted successfully")
    
    # Show remaining count
    collection_info = client.get_collection(collection)
    print(f"Remaining points: {collection_info.points_count}")


def delete_collection():
    """Delete entire collection"""
    client = get_qdrant_client()
    collection = get_collection_name()
    
    print(f"\n{'='*60}")
    print(f"DELETING COLLECTION: {collection}")
    print(f"{'='*60}")
    
    client.delete_collection(collection)
    print("✓ Collection deleted")


if __name__ == "__main__":
    import sys
    
    print("\n" + "="*60)
    print("QDRANT DELETE OPTIONS")
    print("="*60)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--delete-all":
            print("\n⚠️  WARNING: This will delete ALL points!")
            confirm = input("Type 'yes' to confirm: ")
            if confirm.lower() == "yes":
                # Get all point IDs first
                client = get_qdrant_client()
                collection = get_collection_name()
                offset = None
                all_ids = []
                
                while True:
                    points, next_offset = client.scroll(
                        collection_name=collection,
                        limit=100,
                        offset=offset,
                        with_payload=False,
                        with_vectors=False,
                    )
                    all_ids.extend([p.id for p in points])
                    if next_offset is None:
                        break
                    offset = next_offset
                
                if all_ids:
                    delete_specific_points(all_ids)
                print("✓ All points deleted")
        
        elif sys.argv[1] == "--delete-collection":
            print("\n⚠️  WARNING: This will delete the entire collection!")
            confirm = input("Type 'yes' to confirm: ")
            if confirm.lower() == "yes":
                delete_collection()
        
        else:
            # Delete specific IDs
            ids = [int(x) for x in sys.argv[1:]]
            delete_specific_points(ids)
    else:
        delete_all_points()
