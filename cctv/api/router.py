from typing import Optional

import cv2
from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

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
    "/{sen_id}/stream",
    summary="CCTV MJPEG 스트리밍",
)
def stream_camera_mjpeg(sen_id: int, request: Request):
    """RTSP 스트림을 MJPEG로 변환하여 HTTP로 전달합니다.

    Android WebView의 <img src="..."> 태그로 바로 표시할 수 있습니다.
    """
    row = request.app.state.db.get_cctv_by_sen_id(sen_id)
    if not row:
        raise HTTPException(status_code=404, detail="CCTV를 찾을 수 없습니다.")

    rtsp_url = service.build_rtsp_url(
        ip_address=str(row["ip_address"]),
        username=str(row["camera_id"]),
        password=str(row["camera_pw"]),
    )

    def _generate_frames():
        cap = cv2.VideoCapture(rtsp_url)
        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                _, jpeg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n"
                    + jpeg.tobytes()
                    + b"\r\n"
                )
        finally:
            cap.release()

    return StreamingResponse(
        _generate_frames(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


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
