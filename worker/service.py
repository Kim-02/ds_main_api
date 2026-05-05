from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import worker as crud
from database.models import Worker

from .schemas import WorkerCreate, WorkerUpdate


async def list_workers(db: AsyncSession, process_id: int | None = None) -> list[Worker]:
    return await crud.get_all(db, process_id=process_id)


async def get_worker(db: AsyncSession, worker_id: int) -> Worker:
    w = await crud.get_by_id(db, worker_id)
    if not w:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Worker not found")
    return w


async def create_worker(db: AsyncSession, data: WorkerCreate) -> Worker:
    if await crud.get_by_employee_id(db, data.employee_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="employee_id already exists")
    return await crud.create(db, **data.model_dump())


async def update_worker(db: AsyncSession, worker_id: int, data: WorkerUpdate) -> Worker:
    w = await get_worker(db, worker_id)
    return await crud.update(db, w, **data.model_dump(exclude_none=True))


async def delete_worker(db: AsyncSession, worker_id: int) -> None:
    w = await get_worker(db, worker_id)
    await crud.delete(db, w)
