import logging
import os
import time
from typing import Optional

import cv2
import numpy as np
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
    VideoPipelineActionResponse,
    VideoPipelineStartReq,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cctv/cameras", tags=["cctv"])

_MJPEG_FPS_LIMIT = 0.1          # 10fps max
_MJPEG_PLACEHOLDER_FPS = 0.5    # 2fps when no frame


def _mjpeg_part(jpeg_bytes: bytes) -> bytes:
    return (
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n\r\n"
        + jpeg_bytes
        + b"\r\n"
    )


def _make_placeholder(text: str = "대기 중...") -> bytes:
    img = np.zeros((240, 320, 3), dtype=np.uint8)
    img[:] = (20, 20, 20)
    cv2.putText(img, text, (10, 120), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (160, 160, 160), 1, cv2.LINE_AA)
    _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return buf.tobytes()


def _generate_from_existing_reader(sen_id: int):
    """기존 RtspReader / FrameBuffer에서 프레임을 읽어 MJPEG로 전송.
    RTSP 추가 연결 없음.
    """
    from cctv.rtsp import get_latest_frame
    from cctv.buffer import get_recent_frames

    logger.info("[CCTV_STREAM] request camera_sen_id=%s source=buffer", sen_id)
    first_sent = False

    while True:
        frame = None

        # 1순위: 기존 RtspReader 최신 프레임 (공유 연결)
        result = get_latest_frame(sen_id)
        if result is not None:
            _, frame = result

        # 2순위: 10초 롤링 버퍼에서 마지막 프레임
        if frame is None:
            frames = get_recent_frames(sen_id)
            if frames:
                frame = frames[-1]["frame"]

        # 3순위: placeholder
        if frame is None:
            yield _mjpeg_part(_make_placeholder("카메라 준비 중..."))
            time.sleep(_MJPEG_PLACEHOLDER_FPS)
            continue

        if not first_sent:
            logger.info("[CCTV_STREAM] first frame sent camera_sen_id=%s source=buffer", sen_id)
            first_sent = True

        ok, jpeg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
        if ok:
            yield _mjpeg_part(jpeg.tobytes())

        time.sleep(_MJPEG_FPS_LIMIT)


def _generate_from_rtsp_fallback(sen_id: int, rtsp_url: str):
    """fallback: RTSP를 직접 열어서 MJPEG 전송.
    기존 reader/buffer가 없을 때만 사용.
    cv2.CAP_FFMPEG backend 명시.
    """
    logger.info("[CCTV_STREAM] fallback open rtsp camera_sen_id=%s", sen_id)
    os.environ.setdefault(
        "OPENCV_FFMPEG_CAPTURE_OPTIONS",
        "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|max_delay;0",
    )

    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    if not cap.isOpened():
        logger.error("[CCTV_STREAM] open failed camera_sen_id=%s rtsp=%s", sen_id, rtsp_url)
        yield _mjpeg_part(_make_placeholder("연결 실패"))
        cap.release()
        return

    logger.info("[CCTV_STREAM] fallback rtsp opened camera_sen_id=%s", sen_id)
    prev_frame = None
    first_sent = False

    try:
        while True:
            ok, frame = cap.read()

            if not ok:
                # 이전 정상 프레임 유지 (HEVC POC 오류 대응)
                if prev_frame is not None:
                    ok2, jpeg = cv2.imencode(".jpg", prev_frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
                    if ok2:
                        yield _mjpeg_part(jpeg.tobytes())
                else:
                    yield _mjpeg_part(_make_placeholder("프레임 수신 대기"))
                time.sleep(_MJPEG_PLACEHOLDER_FPS)
                continue

            prev_frame = frame.copy()

            if not first_sent:
                logger.info("[CCTV_STREAM] first frame sent camera_sen_id=%s source=rtsp", sen_id)
                first_sent = True

            ok2, jpeg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if ok2:
                yield _mjpeg_part(jpeg.tobytes())

            time.sleep(_MJPEG_FPS_LIMIT)
    finally:
        cap.release()
        logger.info("[CCTV_STREAM] rtsp released camera_sen_id=%s", sen_id)


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


@router.post(
    "/video/start",
    response_model=VideoPipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="같은 디렉터리의 영상 파일을 space_id=1 가상 CCTV로 실행",
)
def start_video_pipeline(
    data: VideoPipelineStartReq,
):
    return service.start_video_pipeline_from_file(data)


@router.post(
    "/video/{camera_id}/stop",
    response_model=VideoPipelineActionResponse,
    status_code=status.HTTP_200_OK,
    summary="영상 파일 가상 CCTV 파이프라인 중단",
)
def stop_video_pipeline(
    camera_id: int,
):
    return service.stop_video_pipeline(camera_id)


@router.get(
    "/video/{camera_id}/status",
    response_model=VideoPipelineActionResponse,
    summary="영상 파일 가상 CCTV 파이프라인 상태 조회",
)
def get_video_pipeline_status(
    camera_id: int,
):
    return service.get_video_pipeline_status(camera_id)


@router.get(
    "/{sen_id}/stream",
    summary="CCTV MJPEG 스트리밍",
)
def stream_camera_mjpeg(
    sen_id: int,
    request: Request,
    source: str = Query(
        "buffer",
        description="frame 소스: buffer(기존 reader 재사용, 기본값) | rtsp(직접 연결 fallback)",
    ),
):
    """기존 RTSP reader/frame buffer를 재사용해 MJPEG로 전송합니다.

    source=buffer (기본값): 이미 열려 있는 RtspReader/FrameBuffer의 프레임을 가져옵니다.
      → RTSP 추가 연결 없음. 429 Stream Up To Limit 방지.
    source=rtsp: RTSP를 새로 열어 스트리밍 (기존 reader가 없을 때만 사용).
      → cv2.CAP_FFMPEG backend 명시.
    """
    row = request.app.state.db.get_cctv_by_sen_id(sen_id)
    if not row:
        raise HTTPException(status_code=404, detail="CCTV를 찾을 수 없습니다.")

    if source == "rtsp":
        rtsp_url = service.build_rtsp_url(
            ip_address=str(row["ip_address"]),
            username=str(row["camera_id"]),
            password=str(row["camera_pw"]),
        )
        gen = _generate_from_rtsp_fallback(sen_id, rtsp_url)
    else:
        gen = _generate_from_existing_reader(sen_id)

    return StreamingResponse(
        gen,
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
