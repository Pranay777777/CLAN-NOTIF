# setup_collection.py

from qdrant.store import ensure_collection
from qdrant.client import get_qdrant_client, get_collection_name

# Creates collection on the remote server if it doesn't exist
ensure_collection(vector_size=384)

# Verify it was created
client = get_qdrant_client()
collection = get_collection_name()
info = client.get_collection(collection)

print(f"Collection '{collection}' is ready on the server")
print(f"Vectors config: {info.config.params.vectors}")
print(f"Points count: {info.points_count}")