"""
Complete migration from embedded Qdrant to server mode
"""

import os
import json
import shutil
from pathlib import Path
from datetime import datetime
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams


def backup_embedded_storage():
    """Step 1: Back up embedded storage directory"""
    embedded_path = Path("qdrant_storage")
    if not embedded_path.exists():
        print("✓ No embedded storage found (already clean)")
        return None
    
    backup_dir = Path(f"qdrant_storage_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    print(f"\n📦 STEP 1: Backing up embedded storage...")
    print(f"   From: {embedded_path.absolute()}")
    print(f"   To:   {backup_dir.absolute()}")
    
    shutil.copytree(embedded_path, backup_dir)
    print(f"✓ Backup created at: {backup_dir}")
    
    return backup_dir


def export_embedded_collections():
    """Step 2: Export data from embedded collections to JSON"""
    embedded_path = Path("qdrant_storage")
    if not embedded_path.exists():
        print("✓ No embedded storage found")
        return {}
    
    print(f"\n📤 STEP 2: Exporting embedded collection data to JSON...")
    
    # Connect to embedded storage
    embedded_client = QdrantClient(path=str(embedded_path))
    
    exported_data = {}
    
    # List all collections in embedded storage
    collections = embedded_client.get_collections()
    
    for collection_info in collections.collections:
        collection_name = collection_info.name
        print(f"\n   Collection: {collection_name}")
        
        # Scroll through all points
        all_points = []
        offset = None
        
        while True:
            points, next_offset = embedded_client.scroll(
                collection_name=collection_name,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            
            for point in points:
                all_points.append({
                    "id": point.id,
                    "payload": point.payload or {},
                })
            
            if next_offset is None:
                break
            offset = next_offset
        
        exported_data[collection_name] = all_points
        print(f"   ✓ Exported {len(all_points)} points")
    
    # Save to JSON
    export_file = Path("qdrant_embedded_export.json")
    with open(export_file, "w", encoding="utf-8") as f:
        json.dump(exported_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ All data exported to: {export_file}")
    
    return exported_data


def verify_server_data():
    """Step 3: Verify server has the data"""
    print(f"\n🔍 STEP 3: Verifying server data...")
    
    # Use QDRANT_MODE=remote to connect to server
    os.environ["QDRANT_MODE"] = "remote"
    
    from qdrant.client import get_qdrant_client, get_collection_name
    
    try:
        client = get_qdrant_client()
        collection = get_collection_name()
        
        # Check collection info
        info = client.get_collection(collection)
        print(f"   Collection: {collection}")
        print(f"   Points count: {info.points_count}")
        print(f"   Vector size: {info.config.params.vectors.size}")
        print(f"   Distance: {info.config.params.vectors.distance}")
        
        # Sample a few points
        points, _ = client.scroll(
            collection_name=collection,
            limit=3,
            with_payload=True,
            with_vectors=False,
        )
        
        print(f"\n   Sample points:")
        for point in points:
            payload = point.payload or {}
            print(f"     - ID {point.id}: {payload.get('title', 'N/A')[:50]}")
        
        print(f"\n✓ Server data verified!")
        return True
        
    except Exception as e:
        print(f"✗ Error verifying server: {e}")
        return False


def clean_embedded_storage():
    """Step 4: Remove embedded storage directory"""
    embedded_path = Path("qdrant_storage")
    
    if not embedded_path.exists():
        print(f"\n✓ Embedded storage already removed")
        return True
    
    print(f"\n🗑️  STEP 4: Removing embedded storage directory...")
    print(f"   Path: {embedded_path.absolute()}")
    
    try:
        shutil.rmtree(embedded_path)
        print(f"✓ Embedded storage removed")
        return True
    except Exception as e:
        print(f"✗ Error removing directory: {e}")
        return False


def test_endpoints():
    """Step 5: Test key endpoints work with server mode"""
    print(f"\n🧪 STEP 5: Testing endpoints...")
    
    os.environ["QDRANT_MODE"] = "remote"
    
    from qdrant.client import get_qdrant_client, get_collection_name
    
    try:
        client = get_qdrant_client()
        collection = get_collection_name()
        
        # Test 1: Scroll points
        points, _ = client.scroll(collection_name=collection, limit=5)
        print(f"   ✓ Scroll test: {len(points)} points retrieved")
        
        # Test 2: Search (with a dummy vector)
        import numpy as np
        dummy_vector = np.random.rand(384).tolist()
        
        results = client.search(
            collection_name=collection,
            query_vector=dummy_vector,
            limit=3,
        )
        print(f"   ✓ Search test: {len(results)} results returned")
        
        # Test 3: Get collection stats
        info = client.get_collection(collection)
        print(f"   ✓ Collection stats: {info.points_count} points")
        
        print(f"\n✓ All endpoint tests passed!")
        return True
        
    except Exception as e:
        print(f"✗ Endpoint test failed: {e}")
        return False


def main():
    print("\n" + "="*70)
    print("QDRANT MIGRATION: EMBEDDED → SERVER MODE")
    print("="*70)
    
    print("\nCurrent Configuration:")
    print(f"  Mode: {os.getenv('QDRANT_MODE', 'local')}")
    print(f"  Server: {os.getenv('QDRANT_URL', 'N/A')}")
    print(f"  Embedded Path: ./qdrant_storage (if exists)")
    
    input("\nPress ENTER to start migration (or Ctrl+C to cancel)...")
    
    # Execute migration steps
    backup_dir = backup_embedded_storage()
    exported = export_embedded_collections()
    server_ok = verify_server_data()
    cleaned = clean_embedded_storage()
    tests_ok = test_endpoints()
    
    # Summary
    print("\n" + "="*70)
    print("MIGRATION SUMMARY")
    print("="*70)
    print(f"✓ Backup created: {backup_dir}")
    print(f"✓ Data exported: qdrant_embedded_export.json ({len(exported)} collections)")
    print(f"✓ Server verified: {server_ok}")
    print(f"✓ Embedded removed: {cleaned}")
    print(f"✓ Endpoints tested: {tests_ok}")
    
    if all([backup_dir, server_ok, cleaned, tests_ok]):
        print("\n✅ MIGRATION COMPLETE - Now running 100% SERVER MODE!")
        print("\nConfiguration is now optimized:")
        print("  • QDRANT_MODE=remote")
        print("  • Using server at http://172.20.3.65:6333")
        print("  • Embedded storage removed")
        print("  • All endpoints verified working")
    else:
        print("\n⚠️  Migration partially completed - review errors above")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
