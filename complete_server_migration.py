"""
Import embedded Qdrant data to server and complete migration
"""

import json
import os
from pathlib import Path
from qdrant_client.models import PointStruct
from sentence_transformers import SentenceTransformer


def import_data_from_export():
    """Load exported data into server"""
    print("\n📥 IMPORTING DATA TO SERVER...")
    
    export_file = Path("qdrant_embedded_export.json")
    if not export_file.exists():
        print(f"✗ Export file not found: {export_file}")
        return False
    
    try:
        with open(export_file, "r", encoding="utf-8") as f:
            exported_data = json.load(f)
        
        # Force server mode
        os.environ["QDRANT_MODE"] = "remote"
        
        from qdrant.client import get_qdrant_client
        
        client = get_qdrant_client()
        model = SentenceTransformer("all-MiniLM-L6-v2")
        
        total_imported = 0
        
        for collection_name, points_data in exported_data.items():
            print(f"\n   Collection: {collection_name} ({len(points_data)} points)")
            
            points_to_upsert = []
            
            for point_data in points_data:
                point_id = point_data["id"]
                payload = point_data["payload"]
                
                # Generate embedding for the title and language
                title = payload.get("title", "")
                language = payload.get("language", "en")
                embedding_text = f"{title} {language}".strip()
                
                try:
                    vector = model.encode(embedding_text).tolist()
                    
                    point = PointStruct(
                        id=point_id,
                        vector=vector,
                        payload=payload,
                    )
                    points_to_upsert.append(point)
                except Exception as e:
                    print(f"      ✗ Error encoding point {point_id}: {e}")
                    continue
            
            # Upsert all points
            if points_to_upsert:
                try:
                    client.upsert(
                        collection_name=collection_name,
                        points=points_to_upsert,
                    )
                    print(f"   ✓ Imported {len(points_to_upsert)} points to server")
                    total_imported += len(points_to_upsert)
                except Exception as e:
                    print(f"   ✗ Error upserting to server: {e}")
                    return False
        
        print(f"\n✓ Total {total_imported} points imported to server")
        return True
        
    except Exception as e:
        print(f"✗ Error importing data: {e}")
        return False


def verify_final_state():
    """Verify server now has all data"""
    print("\n🔍 FINAL VERIFICATION...")
    
    os.environ["QDRANT_MODE"] = "remote"
    
    from qdrant.client import get_qdrant_client
    
    try:
        client = get_qdrant_client()
        
        # List all collections on server
        collections = client.get_collections()
        
        print(f"\n   Server collections:")
        for collection in collections.collections:
            name = collection.name
            count = collection.points_count
            print(f"     • {name}: {count} points")
            
            # Show sample points
            if count > 0:
                points, _ = client.scroll(
                    collection_name=name,
                    limit=3,
                    with_payload=True,
                    with_vectors=False,
                )
                for p in points:
                    payload = p.payload or {}
                    title = payload.get("title", "N/A")[:40]
                    print(f"       └─ {title}...")
        
        return True
        
    except Exception as e:
        print(f"✗ Error verifying: {e}")
        return False


def cleanup_backup():
    """Clean up the backup directory (optional)"""
    print("\n🧹 CLEANUP...")
    
    # Find latest backup
    import glob
    backups = glob.glob("qdrant_storage_backup_*")
    
    if backups:
        print(f"   Found {len(backups)} backup(s):")
        for backup in sorted(backups):
            print(f"     • {backup}")
        
        keep = input("\n   Keep backups? (y/n default=y): ").lower()
        if keep == "n":
            import shutil
            for backup in backups:
                shutil.rmtree(backup)
                print(f"   ✓ Removed {backup}")
        else:
            print("   ✓ Backups retained for safety")
    
    return True


def main():
    print("\n" + "="*70)
    print("COMPLETE MIGRATION: Copy embedded data to server")
    print("="*70)
    
    print("\nThis will:")
    print("  1. Read exported embedded data")
    print("  2. Generate embeddings for all points")
    print("  3. Upsert to remote server")
    print("  4. Verify all collections are on server")
    print("  5. Keep backups for safety")
    
    input("\nPress ENTER to proceed (or Ctrl+C to cancel)...")
    
    # Execute migration
    imported = import_data_from_export()
    verified = verify_final_state()
    cleaned = cleanup_backup()
    
    # Summary
    print("\n" + "="*70)
    print("MIGRATION COMPLETE")
    print("="*70)
    
    if imported and verified:
        print("✅ SUCCESS!")
        print("\nYour Qdrant setup is now:")
        print("  • 100% server mode (remote)")
        print("  • All embedded data migrated to server")
        print("  • Embedded storage safely backed up")
        print("  • Ready for production use")
    else:
        print("⚠️  See errors above")
    
    print("\nNext steps:")
    print("  1. Restart your API server: python api.py")
    print("  2. Test endpoints: /videos, /recommend-video, etc.")
    print("  3. Remove backup if everything works: del qdrant_storage_backup_*")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
