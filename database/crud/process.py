from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database.models import Process


async def get_all(db: AsyncSession) -> list[Process]:
    result = await db.execute(select(Process).order_by(Process.id))
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, process_id: int) -> Process | None:
    result = await db.execute(
        select(Process)
        .options(selectinload(Process.sensors), selectinload(Process.workers))
        .where(Process.id == process_id)
    )
    return result.scalar_one_or_none()


async def create(db: AsyncSession, **kwargs) -> Process:
    process = Process(**kwargs)
    db.add(process)
    await db.flush()
    await db.refresh(process)
    return process


async def update(db: AsyncSession, process: Process, **kwargs) -> Process:
    for key, value in kwargs.items():
        setattr(process, key, value)
    await db.flush()
    await db.refresh(process)
    return process


async def delete(db: AsyncSession, process: Process) -> None:
    await db.delete(process)
    await db.flush()
