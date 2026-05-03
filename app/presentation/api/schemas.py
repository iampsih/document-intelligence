import uuid
from pydantic import BaseModel


class DocumentCreateRequest(BaseModel):
    filename: str
    content_type: str
    storage_key: str


class DocumentResponse(BaseModel):
    id: uuid.UUID
    filename: str
    content_type: str
    storage_key: str
    status: str
    text_content: str | None