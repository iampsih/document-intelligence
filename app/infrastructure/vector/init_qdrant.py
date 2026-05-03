from qdrant_client.models import Distance, VectorParams
from app.infrastructure.vector.qdrant_service import client, COLLECTION_NAME

def init_qdrant():
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=384,  # размер вектора
            distance=Distance.COSINE,
        ),
    )

if __name__ == "__main__":
    init_qdrant()