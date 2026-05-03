from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

DATABASE_URL = "postgresql+asyncpg://aml_user:aml_pass@localhost:5434/aml_db"

engine = create_async_engine(
    DATABASE_URL,
    echo=True,  # показывает SQL в логах (очень удобно)
)

SessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session