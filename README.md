# 📄 Document Intelligence System

Backend сервис для обработки и интеллектуального поиска документов.

Проект демонстрирует архитектуру production-level backend с использованием:
FastAPI, Kafka, PostgreSQL, Redis, MinIO, Elasticsearch и Qdrant.

---

## 🔥 Основные возможности

- 📥 Загрузка документов (PDF)
- 🗄 Хранение файлов в MinIO (S3)
- 🧾 Метаданные в PostgreSQL
- ⚡ Кэширование через Redis
- 📬 Асинхронная обработка через Kafka + Worker
- 🔎 Поиск по словам (Elasticsearch)
- 🧠 Поиск по смыслу (Qdrant + embeddings)

---

## 🏗 Архитектура

```text
Client
  ↓
FastAPI
  ↓
PostgreSQL (metadata)
MinIO (files)
  ↓
Kafka (event)
  ↓
Worker
  ↓
Elasticsearch (keyword search)
Qdrant (semantic search)
⚙️ Технологии
FastAPI (REST API)
PostgreSQL + SQLAlchemy + Alembic
Redis (cache)
MinIO (object storage)
Kafka (event-driven architecture)
Elasticsearch (full-text search)
Qdrant (vector search)
SentenceTransformers (embeddings)
Docker / Docker Compose
🚀 Запуск проекта
1. Клонировать репозиторий
git clone https://github.com/your-username/document-intelligence.git
cd document-intelligence
2. Запустить инфраструктуру
docker compose up -d
3. Установить зависимости
uv venv
uv add fastapi uvicorn sqlalchemy asyncpg alembic redis minio aiokafka elasticsearch qdrant-client sentence-transformers
4. Запустить backend
uv run uvicorn main:app --reload
5. Запустить worker
uv run python app/workers/document_worker.py
📡 API
📥 Загрузка документа
POST /api/v1/documents/
📄 Получение документа
GET /api/v1/documents/{id}
🔎 Поиск по словам (Elasticsearch)
GET /api/v1/documents/search?q=cv
🧠 Semantic search (Qdrant)
GET /api/v1/documents/semantic-search?q=backend developer
