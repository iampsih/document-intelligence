import asyncio
import json
import uuid

from aiokafka import AIOKafkaConsumer
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.db.models import Document
from app.infrastructure.search.elasticsearch_client import INDEX_NAME, es
from app.infrastructure.vector.embedding import get_embedding
from app.infrastructure.vector.qdrant_service import client, COLLECTION_NAME
from qdrant_client.models import PointStruct
from app.infrastructure.parser.pdf_parser import extract_text_from_pdf
import tempfile
from app.infrastructure.storage.s3_client import s3_client, BUCKET_NAME

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "documents"

DATABASE_URL = "postgresql+asyncpg://aml_user:aml_pass@localhost:5434/aml_db"

def download_file_from_s3(storage_key: str) -> str:
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")

    s3_client.download_fileobj(
        BUCKET_NAME,
        storage_key,
        temp_file,
    )

    temp_file.close()
    return temp_file.name

async def main():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="document-workers",
    )

    engine = create_async_engine(DATABASE_URL)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    await consumer.start()
    print("Worker started. Waiting for documents...")

    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            document_id = uuid.UUID(data["document_id"])
            storage_key = data["storage_key"]

            print(f"Получили задачу: {document_id}")

            async with SessionLocal() as session:
                try:
                    await session.execute(
                        update(Document)
                        .where(Document.id == document_id)
                        .values(status="processing", error_message=None)
                    )
                    await session.commit()

                    await asyncio.sleep(3)

                    if data.get("force_error"):
                        raise RuntimeError("Fake OCR error")

                    file_path = download_file_from_s3(storage_key)
                    parsed_text = extract_text_from_pdf(file_path)
                    if not parsed_text:
                        parsed_text = "No text extracted from PDF"

                    vector = get_embedding(parsed_text)

                    client.upsert(
                        collection_name=COLLECTION_NAME,
                        points=[
                            PointStruct(
                                id=str(document_id),
                                vector=vector,
                                payload={
                                    "document_id": str(document_id),
                                    "text": parsed_text,
                                },
                            )
                        ],
                    )

                    print("→ Отправляем в Elasticsearch")

                    response = es.index(
                        index=INDEX_NAME,
                        id=str(document_id),
                        document={
                            "document_id": str(document_id),
                            "filename": storage_key,
                            "text": parsed_text,
                        }
                    )

                    print("→ Ответ Elasticsearch:", response)

                    await session.execute(
                        update(Document)
                        .where(Document.id == document_id)
                        .values(
                            status="processed",
                            text_content=parsed_text,
                            error_message=None,
                        )
                    )
                    await session.commit()

                    print(f"Обработали документ: {document_id}")

                except Exception as exc:
                    await session.rollback()

                    await session.execute(
                        update(Document)
                        .where(Document.id == document_id)
                        .values(status="failed", error_message=str(exc))
                    )
                    await session.commit()

                    print(f"Ошибка обработки документа {document_id}: {exc}")

    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())