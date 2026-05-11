from fastapi import APIRouter, Request, status

from . import service
from .schemas import ProcessCreate, ProcessOut, ProcessUpdate

router = APIRouter(prefix="/processes", tags=["process"])


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
