from fastapi import FastAPI
from app.presentation.api.health import router as health_router

app = FastAPI(title="AML Document Intelligence API")

app.include_router(health_router)


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "AML Document Intelligence API is running"}