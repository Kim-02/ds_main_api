from fastapi import APIRouter, Request, status
from fastapi.concurrency import run_in_threadpool

from . import service
from .schemas import ProcessCreate, ProcessOut, ProcessUpdate

router = APIRouter(prefix="/processes", tags=["process"])

# Jetson 등록 시 공간 선택용 전용 엔드포인트
spaces_router = APIRouter(prefix="/spaces", tags=["spaces"])


@spaces_router.get("", summary="공간 목록 조회 (Jetson 등록 시 공간 선택용)")
async def list_spaces(request: Request):
    rows = await run_in_threadpool(request.app.state.db.get_all_spaces)
    return {"success": True, "data": rows}


@router.get("/", response_model=list[ProcessOut])
async def list_processes(request: Request):
    return await service.list_processes(request.app.state.db)


@router.post("/", response_model=ProcessOut, status_code=status.HTTP_201_CREATED)
async def create_process(data: ProcessCreate, request: Request):
    return await service.create_process(request.app.state.db, data)


@router.get("/{process_id}", response_model=ProcessOut)
async def get_process(process_id: int, request: Request):
    return await service.get_process(request.app.state.db, process_id)


@router.put("/{process_id}", response_model=ProcessOut)
async def update_process(process_id: int, data: ProcessUpdate, request: Request):
    return await service.update_process(request.app.state.db, process_id, data)


@router.delete("/{process_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_process(process_id: int, request: Request):
    await service.delete_process(request.app.state.db, process_id)

