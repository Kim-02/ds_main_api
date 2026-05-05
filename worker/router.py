from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import WorkerCreate, WorkerOut, WorkerUpdate

DB = Annotated[AsyncSession, Depends(get_db)]
router = APIRouter(prefix="/workers", tags=["worker"])

# 기존 API 호환: 사번으로 작업자 이름 조회
legacy_router = APIRouter(prefix="/api", tags=["worker"])


@legacy_router.get("/worker", summary="사번으로 작업자 이름 조회")
async def get_worker_name(worker_id: str, request: Request):
    name = request.app.state.db.get_worker_name_by_id(worker_id)
    if not name:
        raise HTTPException(status_code=404, detail="해당 사번을 가진 작업자가 없습니다.")
    return {"status": "success", "worker_name": name}


@router.get("/", response_model=list[WorkerOut])
async def list_workers(db: DB, process_id: Optional[int] = Query(None)):
    return await service.list_workers(db, process_id=process_id)


@router.post("/", response_model=WorkerOut, status_code=status.HTTP_201_CREATED)
async def create_worker(data: WorkerCreate, db: DB):
    return await service.create_worker(db, data)


@router.get("/{worker_id}", response_model=WorkerOut)
async def get_worker(worker_id: int, db: DB):
    return await service.get_worker(db, worker_id)


@router.put("/{worker_id}", response_model=WorkerOut)
async def update_worker(worker_id: int, data: WorkerUpdate, db: DB):
    return await service.update_worker(db, worker_id, data)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker(worker_id: int, db: DB):
    await service.delete_worker(db, worker_id)
