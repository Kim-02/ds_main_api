from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import CameraCreate, CameraOut, CameraUpdate

from pydantic import BaseModel

DB = Annotated[AsyncSession, Depends(get_db)]
router = APIRouter(prefix="/cctv/cameras", tags=["cctv"])

# 기존 API 호환
legacy_router = APIRouter(prefix="/api/cameras", tags=["cctv"])


class AppCameraReq(BaseModel):
    ip_address: str
    camera_id: str
    camera_pw: str


@legacy_router.post("/register", summary="카메라 등록 (기존 앱 호환)")
def register_camera_legacy(req: AppCameraReq, request: Request):
    from fastapi import HTTPException, Request as Req
    db = request.app.state.db
    result = db.register_camera_info(req.ip_address, req.camera_id, req.camera_pw)
    if result is None:
        raise HTTPException(status_code=400, detail="이미 등록된 카메라입니다.")
    elif result is False:
        raise HTTPException(status_code=404, detail="Jetson 정보가 DB에 없습니다.")
    return {"status": "success", "message": "카메라가 성공적으로 등록되었습니다."}


@legacy_router.get("", summary="CCTV 목록 조회 (기존 앱 호환)")
def get_cameras_legacy(request: Request):
    cctv_list = request.app.state.db.get_cctv_list()
    result = [
        {"ip_address": c["ip_address"], "sen_name": c["sen_name"], "sen_locate": c["sen_locate"], "health": 1}
        for c in cctv_list
    ]
    return {"status": "success", "data": result}


@router.get("/", response_model=list[CameraOut])
async def list_cameras(db: DB, process_id: Optional[int] = Query(None)):
    return await service.list_cameras(db, process_id=process_id)


@router.post("/", response_model=CameraOut, status_code=status.HTTP_201_CREATED)
async def create_camera(data: CameraCreate, db: DB):
    return await service.create_camera(db, data)


@router.get("/{sensor_id}", response_model=CameraOut)
async def get_camera(sensor_id: int, db: DB):
    return await service.get_camera(db, sensor_id)


@router.put("/{sensor_id}", response_model=CameraOut)
async def update_camera(sensor_id: int, data: CameraUpdate, db: DB):
    return await service.update_camera(db, sensor_id, data)


@router.delete("/{sensor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_camera(sensor_id: int, db: DB):
    await service.delete_camera(db, sensor_id)
