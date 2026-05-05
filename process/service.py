from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import process as crud
from database.models import Process

from .schemas import ProcessCreate, ProcessUpdate


async def list_processes(db: AsyncSession) -> list[Process]:
    return await crud.get_all(db)


async def get_process(db: AsyncSession, process_id: int) -> Process:
    p = await crud.get_by_id(db, process_id)
    if not p:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process not found")
    return p


async def create_process(db: AsyncSession, data: ProcessCreate) -> Process:
    return await crud.create(db, **data.model_dump())


async def update_process(db: AsyncSession, process_id: int, data: ProcessUpdate) -> Process:
    p = await get_process(db, process_id)
    return await crud.update(db, p, **data.model_dump(exclude_none=True))


async def delete_process(db: AsyncSession, process_id: int) -> None:
    p = await get_process(db, process_id)
    await crud.delete(db, p)
