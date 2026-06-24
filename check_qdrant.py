import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient

load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")

client = QdrantClient(url=QDRANT_URL)
print("✅ Connected to Qdrant!")

results = client.scroll(collection_name="clan_videos", limit=3)
for point in results[0]:
    print(f"video_id: {point.payload.get('video_id')}")