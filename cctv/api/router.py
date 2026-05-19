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
    DemoAnalyzeResponse,
    DemoCctvRegisterReq,
    DemoCctvRegisterResponse,
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


def _make_placeholder(text: str = "Waiting for camera frame...") -> bytes:
    """진단용 placeholder JPEG. 영문 사용 (cv2.putText는 한글 미지원)."""
    img = np.zeros((240, 320, 3), dtype=np.uint8)
    img[:] = (20, 20, 20)
    cv2.putText(img, text[:40], (10, 110), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (160, 160, 160), 1, cv2.LINE_AA)
    _, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
    return buf.tobytes()


def _generate_from_existing_reader(sen_id: int, db=None):
    """기존 RtspReader / FrameBuffer에서 프레임을 읽어 MJPEG로 전송.
    RTSP 추가 연결 없음.
    db가 전달되면 reader가 없을 때 auto-start를 시도한다.
    """
    from cctv.rtsp import get_latest_frame, get_reader_status, get_all_reader_ids, register_reader
    from cctv.buffer import get_recent_frames, get_buffer_status, get_all_buffer_ids

    logger.info("[CCTV_STREAM] request camera_sen_id=%s source=buffer", sen_id)

    # ── 1. 진단 로그: 현재 등록된 reader/buffer key 목록 ──────────────────
    reader_ids = get_all_reader_ids()
    buffer_ids = get_all_buffer_ids()
    logger.info("[CCTV_STREAM] reader_ids=%s buffer_ids=%s", reader_ids, buffer_ids)

    r_status = get_reader_status(sen_id)
    b_status = get_buffer_status(sen_id)
    logger.info(
        "[CCTV_STREAM] reader_status camera_sen_id=%s running=%s opened=%s has_frame=%s",
        sen_id, r_status.get("running"), r_status.get("opened"), r_status.get("has_frame"),
    )
    logger.info(
        "[CCTV_STREAM] buffer_status camera_sen_id=%s running=%s frame_count=%s",
        sen_id, b_status.get("running"), b_status.get("frame_count"),
    )

    # ── 2. reader가 없으면 auto-start 시도 (camera_info가 있을 때만) ──────
    if not r_status.get("running") and db is not None:
        row = db.get_camera_rtsp_by_sen_id(sen_id)
        if row:
            rtsp_url = service.build_rtsp_url(
                ip_address=str(row["ip_address"]),
                username=str(row["camera_id"]),
                password=str(row["camera_pw"]),
            )
            logger.info(
                "[CCTV_STREAM] auto-starting reader camera_sen_id=%s ip=%s",
                sen_id, row["ip_address"],
            )
            started = register_reader(sen_id, rtsp_url)
            if started:
                logger.info("[CCTV_STREAM] reader started, waiting 2s for first frame camera_sen_id=%s", sen_id)
                time.sleep(2.0)
            else:
                logger.info("[CCTV_STREAM] reader already running (same url) camera_sen_id=%s", sen_id)
        else:
            logger.warning(
                "[CCTV_STREAM] no camera_info found for sen_id=%s → placeholder only", sen_id
            )

    # ── 3. 스트리밍 루프 ────────────────────────────────────────────────────
    first_sent = False
    placeholder_count = 0

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

        # 3순위: placeholder (영문, 한글 깨짐 없음)
        if frame is None:
            placeholder_count += 1
            if placeholder_count == 1 or placeholder_count % 20 == 0:
                logger.warning(
                    "[CCTV_STREAM] no frame camera_sen_id=%s placeholder_count=%s",
                    sen_id, placeholder_count,
                )
            yield _mjpeg_part(_make_placeholder(f"No frame / sen_id={sen_id}"))
            time.sleep(_MJPEG_PLACEHOLDER_FPS)
            continue

        placeholder_count = 0
        if not first_sent:
            logger.info(
                "[CCTV_STREAM] first real frame sent camera_sen_id=%s shape=%s",
                sen_id, getattr(frame, "shape", "?"),
            )
            first_sent = True

        ok, jpeg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
        if ok:
            yield _mjpeg_part(jpeg.tobytes())
        else:
            logger.error("[CCTV_STREAM] jpeg encode failed camera_sen_id=%s", sen_id)

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
    "/register-demo",
    response_model=DemoCctvRegisterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="시연용 가상 CCTV 등록",
    description=(
        "실제 IP 카메라 없이 demo video를 연결한 가상 CCTV를 DB에 등록한다.\n"
        "등록 후 평면도에 배치 가능하며, 클릭 시 앱 내부 raw 영상이 재생된다.\n"
        "같은 space_id + demo_video_key 조합이 이미 있으면 기존 항목을 반환한다."
    ),
)
def register_demo_camera(
    data: DemoCctvRegisterReq,
    request: Request,
):
    result = service.register_demo_camera(request.app.state.db, data)
    return DemoCctvRegisterResponse(
        success=True,
        message="시연용 CCTV가 등록되었습니다.",
        data={
            "sen_id": result.get("sen_id"),
            "sensor_id": result.get("sensor_id") or "",
            "sen_name": result.get("sen_name") or "",
            "space_id": result.get("space_id"),
            "is_demo": bool(result.get("is_demo")),
            "demo_video_key": result.get("demo_video_key") or "",
        },
    )


@router.post(
    "/{camera_sen_id}/demo/analyze",
    response_model=DemoAnalyzeResponse,
    status_code=status.HTTP_200_OK,
    summary="시연용 CCTV demo video VLM 분석 실행",
    description=(
        "서버의 media/demo_videos/{demo_video_key}.mp4 파일을 fire_pipeline으로 분석한다.\n"
        "분석 결과는 기존과 동일하게 alert_event에 저장되고 WebSocket /ws/alerts로 브로드캐스트된다.\n"
        "※ 서버 분석용 영상(media/demo_videos/)과 앱 내부 raw 영상은 별개 파일입니다."
    ),
)
def analyze_demo_camera(
    camera_sen_id: int,
    request: Request,
):
    result = service.analyze_demo_camera(request.app.state.db, camera_sen_id)
    return result


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
      → camera_info DB 조회 불필요. RTSP 추가 연결 없음. 429 방지.
    source=rtsp: RTSP를 새로 열어 스트리밍. camera_info에서 자격증명 조회 필요.
    """
    db = request.app.state.db

    if source == "rtsp":
        # RTSP fallback: 카메라 자격증명이 필요하므로 DB 조회
        row = db.get_cctv_by_sen_id(sen_id)
        if not row:
            # sensor_type 필터 없이 재시도
            row = db.get_camera_rtsp_by_sen_id(sen_id)
        if not row:
            raise HTTPException(status_code=404, detail="CCTV 정보를 찾을 수 없습니다. (camera_info 없음)")
        rtsp_url = service.build_rtsp_url(
            ip_address=str(row["ip_address"]),
            username=str(row["camera_id"]),
            password=str(row["camera_pw"]),
        )
        gen = _generate_from_rtsp_fallback(sen_id, rtsp_url)
    else:
        # buffer mode: 기존 reader/buffer에서 직접 프레임 획득
        # reader가 없으면 camera_info 조회 후 auto-start 시도
        gen = _generate_from_existing_reader(sen_id, db=db)

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
