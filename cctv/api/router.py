from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import (
    AppCameraRegisterReq,
    CameraCreate,
    CameraOut,
    CameraUpdate,
    FirePipelineActionResponse,
    FirePipelineStatus,
)

DB = Annotated[AsyncSession, Depends(get_db)]

router = APIRouter(prefix="/cctv/cameras", tags=["cctv"])


# ── CCTV CRUD ────────────────────────────────────────────────────────────────

@router.get(
    "/",
    response_model=list[CameraOut],
    summary="CCTV 목록 조회",
)
async def list_cameras(
    db: DB,
    process_id: Optional[int] = Query(None),
):
    """
    등록된 CCTV 목록을 조회합니다.

    process_id를 지정하면 해당 process에 속한 CCTV만 조회합니다.
    """
    return await service.list_cameras(db, process_id=process_id)


@router.post(
    "/",
    response_model=CameraOut,
    status_code=status.HTTP_201_CREATED,
    summary="CCTV 직접 생성",
)
async def create_camera(
    data: CameraCreate,
    db: DB,
):
    """
    RTSP URL을 이미 알고 있을 때 직접 CCTV를 생성합니다.

    일반 앱 등록에서는 /cctv/cameras/register를 사용합니다.
    """
    return await service.create_camera(db, data)


@router.post(
    "/register",
    response_model=CameraOut,
    status_code=status.HTTP_201_CREATED,
    summary="CCTV 앱 등록",
)
async def register_camera_from_app(
    data: AppCameraRegisterReq,
    db: DB,
):
    """
    앱에서 CCTV IP와 비밀번호를 입력해 카메라를 등록합니다.

    처리 흐름:
    1. ip_address + camera_username + camera_password로 RTSP URL 생성
    2. Sensor 생성
    3. Camera 상세 정보 생성
    4. RTSP reader/buffer 시작
    5. fire_pipeline_enabled=True이면 fire pipeline 자동 시작
    """
    return await service.register_camera_from_app(db, data)


@router.get(
    "/{sensor_id}",
    response_model=CameraOut,
    summary="CCTV 상세 조회",
)
async def get_camera(
    sensor_id: int,
    db: DB,
):
    """
    Sensor ID 기준으로 CCTV 상세 정보를 조회합니다.
    """
    return await service.get_camera(db, sensor_id)


@router.put(
    "/{sensor_id}",
    response_model=CameraOut,
    summary="CCTV 수정",
)
async def update_camera(
    sensor_id: int,
    data: CameraUpdate,
    db: DB,
):
    """
    Sensor ID 기준으로 CCTV 정보를 수정합니다.

    수정 가능:
    - name
    - is_active
    - rtsp_url
    """
    return await service.update_camera(db, sensor_id, data)


@router.delete(
    "/{sensor_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="CCTV 삭제",
)
async def delete_camera(
    sensor_id: int,
    db: DB,
):
    """
    Sensor ID 기준으로 CCTV를 삭제합니다.

    삭제 시:
    - fire pipeline 중단
    - RTSP buffer 중단
    - RTSP reader 중단
    - DB 삭제
    """
    await service.delete_camera(db, sensor_id)


# ── Fire Pipeline 상태 및 제어 ────────────────────────────────────────────────

@router.get(
    "/{sensor_id}/fire-pipeline",
    response_model=FirePipelineStatus,
    summary="Fire pipeline 상태 조회",
)
async def get_fire_pipeline_status(
    sensor_id: int,
    db: DB,
):
    """
    Sensor ID 기준으로 연결된 Camera ID를 찾고,
    해당 Camera ID로 fire pipeline 상태를 조회합니다.
    """
    return await service.get_fire_pipeline_status(db, sensor_id)


@router.post(
    "/{sensor_id}/fire-pipeline/start",
    response_model=FirePipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="Fire pipeline 수동 시작",
)
async def start_fire_pipeline(
    sensor_id: int,
    db: DB,
):
    """
    Sensor ID 기준으로 fire pipeline을 수동 시작합니다.
    """
    return await service.start_fire_pipeline(db, sensor_id)


@router.post(
    "/{sensor_id}/fire-pipeline/stop",
    response_model=FirePipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="Fire pipeline 수동 중단",
)
async def stop_fire_pipeline(
    sensor_id: int,
    db: DB,
):
    """
    Sensor ID 기준으로 fire pipeline을 수동 중단합니다.
    """
    return await service.stop_fire_pipeline(db, sensor_id)