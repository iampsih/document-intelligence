from fastapi import FastAPI

from app.presentation.api.health import router as health_router
from app.presentation.api.documents import router as documents_router
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.infrastructure.broker.kafka_producer import start_kafka, stop_kafka

app = FastAPI(title="AML Document Intelligence API")

app.include_router(health_router)
app.include_router(documents_router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Kafka...")
    await start_kafka()
    yield
    print("Stopping Kafka...")
    await stop_kafka()


app = FastAPI(
    title="AML Document Intelligence API",
    lifespan=lifespan,
)

@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "AML Document Intelligence API is running"}