import uuid
import json

from fastapi import UploadFile, File
from app.infrastructure.storage.s3_client import s3_client, BUCKET_NAME
from fastapi.encoders import jsonable_encoder
from app.infrastructure.cache.redis_client import redis_client
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.broker.kafka_producer import send_message
from app.infrastructure.db.models import Document
from app.infrastructure.db.repositories import DocumentRepository
from app.infrastructure.db.session import get_db
from app.presentation.api.schemas import DocumentCreateRequest, DocumentResponse
from app.infrastructure.search.elasticsearch_client import es, INDEX_NAME

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])


@router.post("/", response_model=DocumentResponse)
async def create_document(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    file_id = str(uuid.uuid4())
    storage_key = f"{file_id}_{file.filename}"

    # загрузка в MinIO
    s3_client.upload_fileobj(
        file.file,
        BUCKET_NAME,
        storage_key,
    )

    repository = DocumentRepository(db)

    document = await repository.create(
        filename=file.filename,
        content_type=file.content_type,
        storage_key=storage_key,
    )

    await send_message(
        "documents",
        {
            "document_id": str(document.id),
            "storage_key": document.storage_key,
        },
    )

    return document

@router.get("/search")
async def search_documents(q: str):
    response = es.search(
        index=INDEX_NAME,
        query={
            "bool": {
                "should": [
                    {"wildcard": {"text.keyword": f"*{q}*"}},
                    {"wildcard": {"filename.keyword": f"*{q}*"}},
                ]
            }
        },
    )

    hits = response["hits"]["hits"]

    return [
        {
            "document_id": hit["_source"]["document_id"],
            "text": hit["_source"]["text"],
        }
        for hit in hits
    ]

@router.get("/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    cache_key = f"document:{document_id}"

    cached_document = await redis_client.get(cache_key)
    if cached_document is not None:
        return json.loads(cached_document)

    repository = DocumentRepository(db)
    document = await repository.get_by_id(document_id)

    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")

    document_data = jsonable_encoder(document)
    await redis_client.set(cache_key, json.dumps(document_data), ex=60)

    return document_data