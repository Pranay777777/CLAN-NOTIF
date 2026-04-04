"""
Remove 'source' field from all points in Qdrant collections
"""

import os
os.environ["QDRANT_MODE"] = "remote"

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct


def remove_source_from_all_points():
    """Remove 'source' field from payload of all points"""
    
    client = QdrantClient(url="http://172.20.3.65:6333")
    
    print("\n" + "="*70)
    print("REMOVING 'source' FIELD FROM ALL POINTS")
    print("="*70)
    
    # Get all collections
    collections = client.get_collections()
    
    total_updated = 0
    
    for col_info in collections.collections:
        collection_name = col_info.name
        print(f"\n📦 Processing collection: {collection_name}")
        
        # Scroll through all points
        offset = None
        points_with_source = []
        updated_points = []
        
        while True:
            points, next_offset = client.scroll(
                collection_name=collection_name,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            
            for point in points:
                payload = point.payload or {}
                
                # Check if point has 'source' field
                if "source" in payload:
                    points_with_source.append(point.id)
                    
                    # Create updated payload without 'source'
                    updated_payload = {k: v for k, v in payload.items() if k != "source"}
                    
                    # Create updated point
                    updated_point = PointStruct(
                        id=point.id,
                        vector=point.vector,
                        payload=updated_payload,
                    )
                    updated_points.append(updated_point)
            
            if next_offset is None:
                break
            offset = next_offset
        
        # Upsert updated points
        if updated_points:
            print(f"   Found {len(points_with_source)} points with 'source' field")
            print(f"   Removing 'source' from {len(updated_points)} points...")
            
            client.upsert(
                collection_name=collection_name,
                points=updated_points,
            )
            
            print(f"   ✓ Updated {len(updated_points)} points")
            total_updated += len(updated_points)
        else:
            print(f"   No points with 'source' field found")
    
    print(f"\n{'='*70}")
    print(f"✅ TOTAL POINTS UPDATED: {total_updated}")
    print(f"{'='*70}\n")
    
    return total_updated


def verify_source_removed():
    """Verify 'source' field is gone"""
    print("\n🔍 VERIFICATION: Checking for any remaining 'source' fields...")
    
    client = QdrantClient(url="http://172.20.3.65:6333")
    collections = client.get_collections()
    
    found_source = False
    
    for col_info in collections.collections:
        collection_name = col_info.name
        
        offset = None
        while True:
            points, next_offset = client.scroll(
                collection_name=collection_name,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            
            for point in points:
                payload = point.payload or {}
                if "source" in payload:
                    print(f"   ✗ Found 'source' in {collection_name} point {point.id}")
                    found_source = True
            
            if next_offset is None:
                break
            offset = next_offset
    
    if not found_source:
        print("   ✓ No 'source' fields found - Clean!")
    
    return not found_source


def show_sample_points():
    """Show sample points to verify structure"""
    print("\n📋 SAMPLE POINTS (after cleanup):")
    
    client = QdrantClient(url="http://172.20.3.65:6333")
    collections = client.get_collections()
    
    for col_info in collections.collections:
        collection_name = col_info.name
        
        points, _ = client.scroll(
            collection_name=collection_name,
            limit=2,
            offset=None,
            with_payload=True,
            with_vectors=False,
        )
        
        print(f"\n   Collection: {collection_name}")
        for i, point in enumerate(points, 1):
            print(f"     Point {i} (ID: {point.id}):")
            payload = point.payload or {}
            for key, value in payload.items():
                value_str = str(value)[:50]
                print(f"       • {key}: {value_str}")


if __name__ == "__main__":
    import sys
    
    print("\n⚠️  This will remove 'source' field from ALL points in all collections")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--execute":
        confirm = "yes"
    else:
        confirm = input("\nType 'yes' to proceed: ").strip()
    
    if confirm.lower() == "yes":
        # Execute removal
        total = remove_source_from_all_points()
        
        # Verify
        cleaned = verify_source_removed()
        
        if cleaned:
            print("\n✅ SUCCESS! 'source' field has been removed from all points")
            
            # Show samples
            show_sample_points()
        else:
            print("\n⚠️  Some points still have 'source' field")
    else:
        print("Cancelled")
