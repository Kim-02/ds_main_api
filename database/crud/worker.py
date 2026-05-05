from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Worker


async def get_all(db: AsyncSession, process_id: int | None = None) -> list[Worker]:
    q = select(Worker)
    if process_id is not None:
        q = q.where(Worker.process_id == process_id)
    result = await db.execute(q.order_by(Worker.id))
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, worker_id: int) -> Worker | None:
    result = await db.execute(select(Worker).where(Worker.id == worker_id))
    return result.scalar_one_or_none()


async def get_by_employee_id(db: AsyncSession, employee_id: str) -> Worker | None:
    result = await db.execute(select(Worker).where(Worker.employee_id == employee_id))
    return result.scalar_one_or_none()


async def create(db: AsyncSession, **kwargs) -> Worker:
    worker = Worker(**kwargs)
    db.add(worker)
    await db.flush()
    await db.refresh(worker)
    return worker


async def update(db: AsyncSession, worker: Worker, **kwargs) -> Worker:
    for key, value in kwargs.items():
        setattr(worker, key, value)
    await db.flush()
    await db.refresh(worker)
    return worker


async def delete(db: AsyncSession, worker: Worker) -> None:
    await db.delete(worker)
    await db.flush()
