import asyncio
import json
import uuid

from aiokafka import AIOKafkaConsumer
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.db.models import Document

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "documents"

DATABASE_URL = "postgresql+asyncpg://aml_user:aml_pass@localhost:5434/aml_db"


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

                    await session.execute(
                        update(Document)
                        .where(Document.id == document_id)
                        .values(status="processed", error_message=None)
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