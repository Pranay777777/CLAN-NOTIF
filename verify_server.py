"""
Quick server verification
"""
import os
os.environ["QDRANT_MODE"] = "remote"

from qdrant.client import QdrantClient

client = QdrantClient(url="http://172.20.3.65:6333")

collections = client.get_collections()

print("\n" + "="*60)
print("QDRANT SERVER - FINAL STATE")
print("="*60)

total_points = 0

for col_info in collections.collections:
    # Get collection object to check points
    col_obj = client.get_collection(col_info.name)
    
    # Try different ways to get point count
    try:
        points_count = col_obj.points_count
    except:
        try:
            points_count = col_obj.get("points_count", 0)
        except:
            # Count manually
            points, _ = client.scroll(col_info.name, limit=1000)
            points_count = len(points)
    
    print(f"\n📦 Collection: {col_info.name}")
    print(f"   Points: {points_count}")
    total_points += points_count
    
    # Show first 5 points
    if points_count > 0:
        offset = None
        count = 0
        print(f"   Samples:")
        
        while count < 5:
            pts, next_offset = client.scroll(
                col_info.name,
                limit=5,
                offset=offset,
                with_payload=True,
                with_vectors=False
            )
            
            if not pts:
                break
            
            for p in pts:
                if count >= 5:
                    break
                payload = p.payload or {}
                title = payload.get("title", f"ID:{p.id}")[:50]
                print(f"     • {title}")
                count += 1
            
            if next_offset is None:
                break
            offset = next_offset

print(f"\n{'='*60}")
print(f"✅ TOTAL POINTS ON SERVER: {total_points}")
print(f"{'='*60}\n")
