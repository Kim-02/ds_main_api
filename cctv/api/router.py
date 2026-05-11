from typing import Optional

from fastapi import APIRouter, Query, Request, status

from . import service
from .schemas import (
    AppCameraRegisterReq,
    CameraCreate,
    CameraOut,
    CameraUpdate,
    FirePipelineActionResponse,
    FirePipelineStatus,
)

router = APIRouter(prefix="/cctv/cameras", tags=["cctv"])


@router.get(
    "/",
    response_model=list[CameraOut],
    summary="CCTV 목록 조회",
)
def list_cameras(
    request: Request,
    space_id: Optional[int] = Query(None),
    process_id: Optional[int] = Query(None, description="기존 앱 호환 필드. MariaDB에서는 space_id로 처리"),
):
    return service.list_cameras(request.app.state.db, space_id=space_id if space_id is not None else process_id)


@router.post(
    "/",
    response_model=CameraOut,
    status_code=status.HTTP_201_CREATED,
    summary="CCTV 직접 생성",
)
def create_camera(
    data: CameraCreate,
    request: Request,
):
    return service.create_camera(request.app.state.db, data)


@router.post(
    "/register",
    response_model=CameraOut,
    status_code=status.HTTP_201_CREATED,
    summary="CCTV 앱 등록",
)
def register_camera_from_app(
    data: AppCameraRegisterReq,
    request: Request,
):
    return service.register_camera_from_app(request.app.state.db, data)


@router.get(
    "/{sensor_id}",
    response_model=CameraOut,
    summary="CCTV 상세 조회",
)
def get_camera(
    sensor_id: int,
    request: Request,
):
    return service.get_camera(request.app.state.db, sensor_id)


@router.put(
    "/{sensor_id}",
    response_model=CameraOut,
    summary="CCTV 수정",
)
def update_camera(
    sensor_id: int,
    data: CameraUpdate,
    request: Request,
):
    return service.update_camera(request.app.state.db, sensor_id, data)


@router.delete(
    "/{sensor_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="CCTV 삭제",
)
def delete_camera(
    sensor_id: int,
    request: Request,
):
    service.delete_camera(request.app.state.db, sensor_id)


@router.get(
    "/{sensor_id}/fire-pipeline",
    response_model=FirePipelineStatus,
    summary="Fire pipeline 상태 조회",
)
def get_fire_pipeline_status(
    sensor_id: int,
    request: Request,
):
    return service.get_fire_pipeline_status(request.app.state.db, sensor_id)


@router.post(
    "/{sensor_id}/fire-pipeline/start",
    response_model=FirePipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="Fire pipeline 수동 시작",
)
def start_fire_pipeline(
    sensor_id: int,
    request: Request,
):
    return service.start_fire_pipeline(request.app.state.db, sensor_id)


@router.post(
    "/{sensor_id}/fire-pipeline/stop",
    response_model=FirePipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="Fire pipeline 수동 중단",
)
def stop_fire_pipeline(
    sensor_id: int,
    request: Request,
):
    return service.stop_fire_pipeline(request.app.state.db, sensor_id)
