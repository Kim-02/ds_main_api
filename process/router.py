from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import ProcessCreate, ProcessOut, ProcessUpdate

DB = Annotated[AsyncSession, Depends(get_db)]
router = APIRouter(prefix="/processes", tags=["process"])


@router.get("/", response_model=list[ProcessOut])
async def list_processes(db: DB):
    return await service.list_processes(db)


@router.post("/", response_model=ProcessOut, status_code=status.HTTP_201_CREATED)
async def create_process(data: ProcessCreate, db: DB):
    return await service.create_process(db, data)


@router.get("/{process_id}", response_model=ProcessOut)
async def get_process(process_id: int, db: DB):
    return await service.get_process(db, process_id)


@router.put("/{process_id}", response_model=ProcessOut)
async def update_process(process_id: int, data: ProcessUpdate, db: DB):
    return await service.update_process(db, process_id, data)


@router.delete("/{process_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_process(process_id: int, db: DB):
    await service.delete_process(db, process_id)
