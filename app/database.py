from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True, future=True)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

class Base(AsyncAttrs, DeclarativeBase):
    pass

# >>> ДОБАВЬ ЭТУ ЧАСТЬ <<<
import asyncio
async def wait_for_db(retries: int = 40, delay: float = 1.0):
    last_err = None
    for _ in range(retries):
        try:
            async with engine.connect() as conn:
                await conn.execute("SELECT 1")
            return
        except Exception as e:
            last_err = e
            await asyncio.sleep(delay)
    raise last_err

async def init_db():
    # Import models so metadata is populated
    from app import models  # noqa
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
