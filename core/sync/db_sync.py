"""Jetson DB ↔ 기업 DB 동기화."""
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import settings

logger = logging.getLogger(__name__)

_corporate_engine = None
_CorporateSession: async_sessionmaker | None = None


def _get_corporate_session() -> async_sessionmaker | None:
    global _corporate_engine, _CorporateSession
    if not settings.corporate_db_url:
        return None
    if _CorporateSession is None:
        _corporate_engine = create_async_engine(settings.corporate_db_url, future=True)
        _CorporateSession = async_sessionmaker(
            bind=_corporate_engine, class_=AsyncSession, expire_on_commit=False
        )
    return _CorporateSession


async def sync_workers_from_corporate() -> int:
    session_factory = _get_corporate_session()
    if session_factory is None:
        return 0

    from database import AsyncSessionLocal
    from database.models import Worker

    synced = 0
    async with session_factory() as corp_db:
        result = await corp_db.execute(
            text("SELECT employee_id, name FROM workers WHERE is_active = true")
        )
        rows = result.fetchall()

    async with AsyncSessionLocal() as local_db:
        from sqlalchemy import select
        for row in rows:
            existing = await local_db.execute(
                select(Worker).where(Worker.employee_id == row.employee_id)
            )
            worker = existing.scalar_one_or_none()
            if worker is None:
                local_db.add(Worker(employee_id=row.employee_id, name=row.name))
                synced += 1
            elif worker.name != row.name:
                worker.name = row.name
                synced += 1
        await local_db.commit()

    logger.info("Worker sync: %d records updated", synced)
    return synced


async def sync_processes_from_corporate() -> int:
    session_factory = _get_corporate_session()
    if session_factory is None:
        return 0

    from database import AsyncSessionLocal
    from database.models import Process

    synced = 0
    async with session_factory() as corp_db:
        result = await corp_db.execute(
            text("SELECT name, description, location, precautions FROM processes WHERE is_active = true")
        )
        rows = result.fetchall()

    async with AsyncSessionLocal() as local_db:
        from sqlalchemy import select
        for row in rows:
            existing = await local_db.execute(
                select(Process).where(Process.name == row.name)
            )
            process = existing.scalar_one_or_none()
            if process is None:
                local_db.add(Process(
                    name=row.name,
                    description=row.description,
                    location=row.location,
                    precautions=row.precautions,
                ))
                synced += 1
        await local_db.commit()

    logger.info("Process sync: %d records updated", synced)
    return synced


async def run_full_sync() -> dict:
    workers = await sync_workers_from_corporate()
    processes = await sync_processes_from_corporate()
    return {"workers_synced": workers, "processes_synced": processes}
