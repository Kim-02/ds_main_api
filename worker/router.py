from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request, status

from . import service
from .schemas import (
    AssignHeartBandRequest,
    AssignHeartBandResponse,
    UnassignSensorResponse,
    WorkerCreate,
    WorkerDbOut,
    WorkerOut,
    WorkerUpdate,
)

router = APIRouter(prefix="/workers", tags=["worker"])
legacy_router = APIRouter(prefix="/api", tags=["worker"])


@legacy_router.get("/worker", summary="사번으로 작업자 이름 조회")
async def get_worker_name(worker_id: str, request: Request):
    name = request.app.state.db.get_worker_name_by_id(worker_id)
    if not name:
        raise HTTPException(status_code=404, detail="해당 사번을 가진 작업자가 없습니다.")
    return {"status": "success", "worker_name": name}


@router.get("/db", response_model=list[WorkerDbOut], summary="실제 MariaDB worker 목록 조회")
async def list_db_workers(
    request: Request,
    is_manager: Optional[int] = Query(None),
):
    return await service.list_db_workers(request, is_manager=is_manager)


@router.get("/db/{dept_id}", response_model=WorkerDbOut, summary="사번으로 실제 MariaDB worker 조회")
async def get_db_worker(dept_id: int, request: Request):
    return await service.get_db_worker(request, dept_id)


@router.post(
    "/{dept_id}/assign-heart-band",
    response_model=AssignHeartBandResponse,
    summary="mDNS로 발견된 워치를 작업자에게 등록 및 매핑",
)
async def assign_heart_band_to_worker(dept_id: int, data: AssignHeartBandRequest, request: Request):
    return await service.assign_heart_band_to_worker(
        request=request,
        dept_id=dept_id,
        sensor_id=data.sensor_id,
        jetson_id=data.jetson_id,
        interval_ms=data.interval_ms,
    )


@router.post(
    "/{dept_id}/unassign-sensor",
    response_model=UnassignSensorResponse,
    summary="작업자와 센서 매핑 해제",
)
async def unassign_worker_sensor(dept_id: int, request: Request):
    return await service.unassign_worker_sensor(request=request, dept_id=dept_id)


@router.get("/", response_model=list[WorkerOut])
async def list_workers(request: Request, process_id: Optional[int] = Query(None)):
    return await service.list_workers(request.app.state.db, process_id=process_id)


@router.post("/", response_model=WorkerOut, status_code=status.HTTP_201_CREATED)
async def create_worker(data: WorkerCreate, request: Request):
    return await service.create_worker(request.app.state.db, data)


@router.get("/{worker_id}", response_model=WorkerOut)
async def get_worker(worker_id: int, request: Request):
    return await service.get_worker(request.app.state.db, worker_id)


@router.put("/{worker_id}", response_model=WorkerOut)
async def update_worker(worker_id: int, data: WorkerUpdate, request: Request):
    return await service.update_worker(request.app.state.db, worker_id, data)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker(worker_id: int, request: Request):
    await service.delete_worker(request.app.state.db, worker_id)
