import os
from dotenv import load_dotenv

load_dotenv()
ACCOUNT_ID = os.getenv("ACCOUNT_ID", 1)

print(f"Resyncing Qdrant...")

from qdrant.indicator_sync import sync_qdrant_payload_from_postgres

stats = sync_qdrant_payload_from_postgres(
    account_id=ACCOUNT_ID,
    dry_run=False,
    clear_unmapped=True,
    limit=None
)

print("✅ Resync Complete!")
print(f"Updated: {stats.get('updated_points')} videos")