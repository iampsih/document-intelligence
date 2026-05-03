import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import Document


class DocumentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self,
        filename: str,
        content_type: str,
        storage_key: str,
    ) -> Document:
        document = Document(
            filename=filename,
            content_type=content_type,
            storage_key=storage_key,
            status="uploaded",
            text_content=None,
        )

        self.session.add(document)
        await self.session.commit()
        await self.session.refresh(document)

        return document

    async def get_by_id(self, document_id: uuid.UUID) -> Document | None:
        result = await self.session.execute(
            select(Document).where(Document.id == document_id)
        )
        return result.scalar_one_or_none()