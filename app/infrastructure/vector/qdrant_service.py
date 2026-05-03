from qdrant_client import QdrantClient

client = QdrantClient("localhost", port=6333)

COLLECTION_NAME = "documents"