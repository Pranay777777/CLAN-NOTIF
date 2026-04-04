"""
Verify video metadata is stored in Qdrant collections
"""

from qdrant_client import QdrantClient

QDRANT_URL = "http://172.20.3.65:6333"
client = QdrantClient(url=QDRANT_URL)

print("\n" + "="*80)
print("QDRANT COLLECTION VERIFICATION")
print("="*80)

# Check clan_videos collection
print("\n📊 clan_videos Collection Status:")
try:
    info = client.get_collection("clan_videos")
    points_count = info.points_count
    print(f"✓ Points count: {points_count}")
    print(f"✓ Vector size: {info.config.params.vectors.size}")
    
    # Get last 5 points
    print(f"\n✓ Last 5 points:")
    points = client.scroll("clan_videos", limit=5, with_payload=True, with_vectors=False)[0]
    for i, point in enumerate(points, 1):
        payload = point.payload
        print(f"  {i}. ID: {point.id}")
        print(f"     Title: {payload.get('title', 'N/A')[:50]}")
        print(f"     Video ID: {payload.get('video_id')}")
        print(f"     Created: {payload.get('created_at', 'N/A')[:10]}")
except Exception as e:
    print(f"✗ Error: {e}")

# Check branch_mappings collection
print("\n📊 branch_mappings Collection Status:")
try:
    info = client.get_collection("branch_mappings")
    points_count = info.points_count
    print(f"✓ Points count: {points_count}")
    print(f"✓ Vector size: {info.config.params.vectors.size}")
    
    # Get last 5 points
    print(f"\n✓ Last 5 points:")
    points = client.scroll("branch_mappings", limit=5, with_payload=True, with_vectors=False)[0]
    for i, point in enumerate(points, 1):
        payload = point.payload
        print(f"  {i}. ID: {point.id}")
        print(f"     Type: {payload.get('type')}")
        print(f"     Key: {payload.get('key')}")
        print(f"     Branch: {payload.get('branch')}")
        print(f"     Region: {payload.get('region')}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "="*80 + "\n")
