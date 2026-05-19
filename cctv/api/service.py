"""CCTV 카메라 서비스 레이어 — MariaDB sensor + camera_info 기반."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse
import zlib

from fastapi import HTTPException, status

from config import settings
from database.db_handler import DatabaseHandler

from .schemas import AppCameraRegisterReq, CameraCreate, CameraUpdate, DemoCctvRegisterReq, VideoPipelineStartReq

logger = logging.getLogger(__name__)

_VIDEO_RUNTIME_REGISTRY: dict[int, dict[str, Any]] = {}

# 시연용 영상 파일 경로 매핑
# 앱 내부 raw 리소스(app/src/main/res/raw/scenario3_fire.mp4)와는 별개로,
# 서버 VLM 분석용 파일은 아래 경로에 별도로 준비해야 합니다.
DEMO_VIDEO_MAP: dict[str, str] = {
    "scenario3_fire": "media/demo_videos/scenario3_fire.mp4",
}


def build_rtsp_url(
    ip_address: str,
    username: str,
    password: str,
    rtsp_path: str | None = None,
) -> str:
    """IP + 계정 정보로 RTSP URL을 생성한다."""
    path = rtsp_path or settings.fire_pipeline_rtsp_path

    if not path.startswith("/"):
        path = f"/{path}"

    encoded_username = quote(username, safe="")
    encoded_password = quote(password, safe="")

    return f"rtsp://{encoded_username}:{encoded_password}@{ip_address}{path}"


def _validate_camera_reachable(ip_address: str, timeout: float = 3.0) -> bool:
    """IP 주소의 RTSP 포트(554)로 소켓 접속이 가능한지 확인한다."""
    import socket
    try:
        with socket.create_connection((ip_address, 554), timeout=timeout):
            return True
    except (socket.timeout, socket.error, OSError):
        return False


def list_cameras(
    db: DatabaseHandler,
    space_id: int | None = None,
) -> list[dict]:
    return [_row_to_camera_out(row) for row in db.get_cctv_list(space_id=space_id)]


def start_registered_camera_runtime_components(
    db: DatabaseHandler,
    space_id: int | None = None,
) -> dict:
    """서버 시작 시 MariaDB에 이미 등록된 CCTV runtime 스레드를 복구한다."""
    rows = db.get_cctv_list(space_id=space_id)
    started = 0
    failed = 0
    cameras = []

    for row in rows:
        # 시연용 CCTV는 실제 RTSP 연결이 없으므로 runtime 시작 생략
        if row.get("is_demo"):
            cameras.append({
                "sensor_id": row.get("sensor_id"),
                "sen_id": row.get("sen_id"),
                "camera_id": row.get("sen_id"),
                "status": "skipped_demo",
            })
            continue

        try:
            rtsp_url = _row_to_rtsp_url(row)
            _start_runtime_components(
                cam_id=int(row["sen_id"]),
                rtsp_url=rtsp_url,
                process_id=int(row.get("space_id") or 0),
            )
            started += 1
            cameras.append({
                "sensor_id": row.get("sensor_id"),
                "sen_id": row.get("sen_id"),
                "camera_id": row.get("sen_id"),
                "status": "started",
            })
        except Exception as exc:
            failed += 1
            logger.warning(
                "등록 CCTV runtime 복구 실패 sen_id=%s sensor_id=%s: %s",
                row.get("sen_id"),
                row.get("sensor_id"),
                exc,
            )
            cameras.append({
                "sensor_id": row.get("sensor_id"),
                "sen_id": row.get("sen_id"),
                "camera_id": row.get("sen_id"),
                "status": "failed",
                "error": str(exc),
            })

    result = {
        "started": started,
        "skipped": 0,
        "failed": failed,
        "cameras": cameras,
    }
    logger.info("등록 CCTV runtime 복구 완료: %s", result)
    return result


def start_video_pipeline_from_file(data: VideoPipelineStartReq) -> dict:
    """프로젝트 실행 디렉터리의 영상 파일을 space_id에 연결된 가상 CCTV로 실행한다."""
    video_path = _resolve_local_video(data.video_name)
    space_id = int(data.space_id or 1)
    camera_id = int(data.camera_id) if data.camera_id is not None else _virtual_camera_id(video_path, space_id)

    if data.restart:
        _stop_runtime_components(camera_id)

    camera_meta = {
        "camera_id": camera_id,
        "sensor_id": f"video-{camera_id}",
        "space_id": space_id,
        "space_name": None,
        "sen_name": f"VIDEO-{video_path.name}",
        "video_name": video_path.name,
        "video_path": str(video_path),
        "source_type": "video_file",
    }
    _VIDEO_RUNTIME_REGISTRY[camera_id] = camera_meta

    _start_runtime_components(
        cam_id=camera_id,
        rtsp_url=str(video_path),
        process_id=space_id,
        camera_meta=camera_meta,
    )

    logger.info(
        "Video CCTV runtime started camera_id=%s space_id=%s video=%s",
        camera_id,
        space_id,
        video_path,
    )
    return _video_pipeline_response("started", camera_id, video_path=video_path, space_id=space_id)


def stop_video_pipeline(camera_id: int) -> dict:
    meta = _VIDEO_RUNTIME_REGISTRY.get(camera_id)
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="실행 중인 영상 파이프라인이 없습니다.",
        )

    video_path = Path(meta.get("video_path"))
    space_id = int(meta.get("space_id") or 1)

    _stop_runtime_components(camera_id)
    _VIDEO_RUNTIME_REGISTRY.pop(camera_id, None)
    logger.info("Video CCTV runtime stopped camera_id=%s", camera_id)
    return _video_pipeline_response("stopped", camera_id, video_path=video_path, space_id=space_id)


def get_video_pipeline_status(camera_id: int) -> dict:
    meta = _VIDEO_RUNTIME_REGISTRY.get(camera_id)
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="실행 중인 영상 파이프라인이 없습니다.",
        )

    return _video_pipeline_response(
        "running",
        camera_id,
        video_path=Path(meta["video_path"]),
        space_id=int(meta.get("space_id") or 1),
    )


def get_video_runtime_cameras_by_space_id(space_id: int) -> list[dict]:
    """온습도/워치 트리거에서 같은 space_id의 테스트 영상 카메라를 함께 사용한다."""
    cameras = []
    for camera_id, meta in list(_VIDEO_RUNTIME_REGISTRY.items()):
        if int(meta.get("space_id") or -1) != int(space_id):
            continue
        cameras.append({
            "sen_id": camera_id,
            "sensor_id": meta.get("sensor_id") or f"video-{camera_id}",
            "sen_name": meta.get("sen_name") or f"VIDEO-{meta.get('video_name')}",
            "space_id": int(meta.get("space_id") or space_id),
            "space_name": meta.get("space_name"),
            "hazard_type": meta.get("hazard_type"),
            "is_hazard": bool(meta.get("is_hazard", False)),
            "ip_address": meta.get("video_path") or "",
            "health": 1,
            "is_online": 1,
            "camera_id": "video",
            "camera_pw": "",
            "rtsp_url": meta.get("video_path") or "",
            "video_path": meta.get("video_path") or "",
            "source_type": "video_file",
        })
    return cameras


def get_camera(
    db: DatabaseHandler,
    sensor_id: int,
) -> dict:
    row = db.get_cctv_by_sen_id(sensor_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )
    return _row_to_camera_out(row)


def _start_runtime_components(
    cam_id: int,
    rtsp_url: str,
    process_id: int,
    camera_meta: dict[str, Any] | None = None,
) -> None:
    """카메라 등록/복구 후 RTSP reader, 10초 버퍼, fire pipeline을 시작한다."""
    try:
        from cctv.rtsp import register_reader
        from cctv.buffer import start_buffer

        register_reader(
            cam_id,
            rtsp_url,
            reconnect_delay=settings.rtsp_reconnect_delay_seconds,
        )
        start_buffer(
            cam_id,
            process_id,
            buffer_seconds=settings.frame_buffer_seconds,
            sample_interval=settings.frame_buffer_sample_interval_seconds,
        )
    except Exception:
        logger.warning(
            "RTSP reader/buffer 시작 실패 (cam_id=%s, rtsp=%s) — DB 등록은 유지됩니다.",
            cam_id,
            rtsp_url,
        )

    if settings.fire_pipeline_enabled:
        try:
            from cctv.fire_pipeline import manager as fire_manager
            fire_manager.start_pipeline(cam_id, rtsp_url, metadata=camera_meta)
        except ImportError:
            logger.warning("fire_pipeline 모듈 없음 — 파이프라인 건너뜀 (cam_id=%s)", cam_id)
        except Exception:
            logger.warning("fire pipeline 시작 실패 (cam_id=%s) — DB 등록은 유지됩니다.", cam_id)


def _stop_runtime_components(cam_id: int) -> None:
    try:
        from cctv.fire_pipeline import manager as fire_manager
        fire_manager.stop_pipeline(cam_id)
    except ImportError:
        logger.warning("fire_pipeline 모듈 없음 — 파이프라인 중단 건너뜀 (cam_id=%s)", cam_id)
    except Exception:
        logger.warning("fire pipeline 중단 실패 (cam_id=%s)", cam_id)

    try:
        from cctv.buffer import stop_buffer
        stop_buffer(cam_id)
    except Exception:
        logger.warning("buffer 중단 실패 (cam_id=%s)", cam_id)

    try:
        from cctv.rtsp import stop_reader
        stop_reader(cam_id)
    except Exception:
        logger.warning("RTSP reader 중단 실패 (cam_id=%s)", cam_id)


def create_camera(
    db: DatabaseHandler,
    data: CameraCreate,
) -> dict:
    parsed = _parse_create_camera(data)
    row = db.register_camera_info(
        ip_address=parsed["ip_address"],
        camera_id=parsed["camera_username"],
        camera_pw=parsed["camera_password"],
        rtsp_url=parsed["rtsp_url"],
        space_id=parsed["space_id"],
        sen_name=parsed["name"],
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="카메라 등록에 실패했습니다.",
        )

    _start_runtime_components(
        cam_id=int(row["sen_id"]),
        rtsp_url=parsed["rtsp_url"],
        process_id=int(row.get("space_id") or 0),
    )
    row["rtsp_url"] = parsed["rtsp_url"]
    return _row_to_camera_out(row)


def register_camera_from_app(
    db: DatabaseHandler,
    data: AppCameraRegisterReq,
) -> dict:
    # DB insert 전 카메라 네트워크 연결 검증
    if not _validate_camera_reachable(data.ip_address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="CCTV 연결 실패: IP, ID, 비밀번호 또는 RTSP 경로를 확인해주세요.",
        )

    space_id = _space_id_from_payload(data)
    camera_name = data.name or f"CCTV-{data.ip_address}"
    rtsp_url = build_rtsp_url(
        ip_address=data.ip_address,
        username=data.camera_username,
        password=data.camera_password,
        rtsp_path=data.rtsp_path,
    )

    row = db.register_camera_info(
        ip_address=data.ip_address,
        camera_id=data.camera_username,
        camera_pw=data.camera_password,
        rtsp_url=rtsp_url,
        space_id=space_id,
        sen_name=camera_name,
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="카메라 등록에 실패했습니다.",
        )

    logger.info(
        "Register CCTV from app | ip=%s | username=%s | space_id=%s | name=%s",
        data.ip_address,
        data.camera_username,
        space_id,
        camera_name,
    )

    _start_runtime_components(
        cam_id=int(row["sen_id"]),
        rtsp_url=rtsp_url,
        process_id=int(row.get("space_id") or 0),
    )
    row["rtsp_url"] = rtsp_url
    return _row_to_camera_out(row)


def update_camera(
    db: DatabaseHandler,
    sensor_id: int,
    data: CameraUpdate,
) -> dict:
    current = db.get_cctv_by_sen_id(sensor_id)
    if not current:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    update_data = data.model_dump(exclude_none=True)
    parsed_rtsp = _parse_rtsp_url(update_data["rtsp_url"]) if "rtsp_url" in update_data else {}
    space_id = update_data.get("space_id", update_data.get("process_id"))

    row = db.update_camera_info(
        sensor_id,
        ip_address=update_data.get("ip_address") or parsed_rtsp.get("ip_address"),
        camera_id=update_data.get("camera_username") or parsed_rtsp.get("camera_username"),
        camera_pw=update_data.get("camera_password") or parsed_rtsp.get("camera_password"),
        sen_name=update_data.get("name"),
        is_online=update_data.get("is_active"),
        health=update_data.get("health"),
        space_id=space_id,
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="카메라 수정에 실패했습니다.",
        )

    _stop_runtime_components(sensor_id)
    rtsp_url = update_data.get("rtsp_url") or _row_to_rtsp_url(row, rtsp_path=update_data.get("rtsp_path"))
    _start_runtime_components(
        cam_id=int(row["sen_id"]),
        rtsp_url=rtsp_url,
        process_id=int(row.get("space_id") or 0),
    )
    row["rtsp_url"] = rtsp_url
    return _row_to_camera_out(row)


def delete_camera(
    db: DatabaseHandler,
    sensor_id: int,
) -> None:
    row = db.get_cctv_by_sen_id(sensor_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    _stop_runtime_components(sensor_id)

    if not db.delete_camera_info(sensor_id):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="카메라 삭제에 실패했습니다.",
        )


def get_fire_pipeline_status(
    db: DatabaseHandler,
    sensor_id: int,
) -> dict:
    row = db.get_cctv_by_sen_id(sensor_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    try:
        from cctv.fire_pipeline import manager as fire_manager
        status_data = fire_manager.get_status(sensor_id)
    except ImportError:
        status_data = {"running": False, "latest_result": "", "latest_error": ""}

    try:
        from cctv.rtsp import get_reader_status
        reader_status = get_reader_status(sensor_id)
    except Exception:
        reader_status = {"camera_id": sensor_id, "running": False, "latest_error": "status_unavailable"}

    try:
        from cctv.buffer import get_buffer_status
        buffer_status = get_buffer_status(sensor_id)
    except Exception:
        buffer_status = {"camera_id": sensor_id, "running": False, "latest_error": "status_unavailable"}

    return {
        "sensor_id": sensor_id,
        "camera_id": sensor_id,
        "running": status_data.get("running", False),
        "latest_result": status_data.get("latest_result", ""),
        "latest_error": status_data.get("latest_error", ""),
        "rtsp_reader": reader_status,
        "frame_buffer": buffer_status,
        "fire_pipeline": status_data,
    }


def start_fire_pipeline(
    db: DatabaseHandler,
    sensor_id: int,
) -> dict:
    row = db.get_cctv_by_sen_id(sensor_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    try:
        from cctv.fire_pipeline import manager as fire_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="fire_pipeline 모듈이 설치되지 않았습니다.",
        )

    started = fire_manager.start_pipeline(sensor_id, _row_to_rtsp_url(row))
    if not started:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 실행 중인 파이프라인입니다.",
        )

    return {
        "status": "started",
        "sensor_id": sensor_id,
        "camera_id": sensor_id,
    }


def stop_fire_pipeline(
    db: DatabaseHandler,
    sensor_id: int,
) -> dict:
    row = db.get_cctv_by_sen_id(sensor_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    try:
        from cctv.fire_pipeline import manager as fire_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="fire_pipeline 모듈이 설치되지 않았습니다.",
        )

    stopped = fire_manager.stop_pipeline(sensor_id)
    if not stopped:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="실행 중인 파이프라인이 없습니다.",
        )

    return {
        "status": "stopped",
        "sensor_id": sensor_id,
        "camera_id": sensor_id,
    }


def _resolve_local_video(video_name: str) -> Path:
    """서버 실행 디렉터리에 있는 영상 파일명을 안전하게 해석한다."""
    name = str(video_name or "").strip()
    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="video_name이 필요합니다.",
        )

    if Path(name).name != name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="video_name에는 경로를 넣을 수 없습니다. 서버 실행 디렉터리의 파일명만 보내주세요.",
        )

    allowed_exts = {".mp4", ".avi", ".mov", ".mkv", ".m4v", ".webm"}
    path = (Path.cwd() / name).resolve()
    cwd = Path.cwd().resolve()
    if path.parent != cwd:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="서버 실행 디렉터리 밖의 파일은 사용할 수 없습니다.",
        )
    if path.suffix.lower() not in allowed_exts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"지원하지 않는 영상 확장자입니다: {path.suffix}",
        )
    if not path.exists() or not path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"영상 파일을 찾을 수 없습니다: {name}",
        )
    return path


def _virtual_camera_id(video_path: Path, space_id: int) -> int:
    key = f"{space_id}:{video_path.name}".encode("utf-8")
    # MariaDB auto_increment sen_id와 충돌 가능성을 낮추기 위해 높은 양수 대역을 사용한다.
    return 900_000_000 + (zlib.crc32(key) % 100_000_000)


def _video_pipeline_response(
    status_text: str,
    camera_id: int,
    *,
    video_path: Path,
    space_id: int,
) -> dict:
    try:
        from cctv.rtsp import get_reader_status
        reader_status = get_reader_status(camera_id)
    except Exception:
        reader_status = {"camera_id": camera_id, "running": False, "latest_error": "status_unavailable"}

    try:
        from cctv.buffer import get_buffer_status
        buffer_status = get_buffer_status(camera_id)
    except Exception:
        buffer_status = {"camera_id": camera_id, "running": False, "latest_error": "status_unavailable"}

    try:
        from cctv.fire_pipeline import manager as fire_manager
        fire_status = fire_manager.get_status(camera_id)
    except Exception:
        fire_status = {"running": False, "latest_result": "", "latest_error": "status_unavailable"}

    video_name = video_path.name if str(video_path) else ""
    return {
        "status": status_text,
        "camera_id": camera_id,
        "space_id": int(space_id),
        "video_name": video_name,
        "video_path": str(video_path) if str(video_path) else "",
        "rtsp_reader": reader_status,
        "frame_buffer": buffer_status,
        "fire_pipeline": fire_status,
    }


def _space_id_from_payload(data: Any) -> int | None:
    space_id = getattr(data, "space_id", None)
    if space_id is None:
        space_id = getattr(data, "process_id", None)
    return space_id


def register_demo_camera(
    db: DatabaseHandler,
    data: DemoCctvRegisterReq,
) -> dict:
    """시연용 가상 CCTV를 DB에 등록한다. 실제 네트워크 연결 검증 없이 즉시 등록."""
    jetson_id = data.jetson_id
    if jetson_id is None:
        # jetson_id 미전달 시 DB의 첫 번째 Jetson 사용
        import pymysql
        try:
            with db._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT jetson_id FROM jetson ORDER BY jetson_id LIMIT 1")
                    row = cursor.fetchone()
                    if row:
                        jetson_id = int(row["jetson_id"])
        except Exception:
            pass

    if jetson_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Jetson 정보를 찾을 수 없습니다. jetson_id를 직접 전달하거나 Jetson을 먼저 등록하세요.",
        )

    row = db.register_demo_camera(
        space_id=data.space_id,
        jetson_id=jetson_id,
        name=data.name,
        demo_video_key=data.demo_video_key,
    )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="시연용 CCTV 등록에 실패했습니다.",
        )

    logger.info(
        "Demo CCTV registered space_id=%s jetson_id=%s demo_video_key=%s sen_id=%s",
        data.space_id, jetson_id, data.demo_video_key, row.get("sen_id"),
    )
    return row


def analyze_demo_camera(
    db: DatabaseHandler,
    camera_sen_id: int,
    broadcast_fn=None,
) -> dict:
    """시연용 CCTV의 demo video를 VLM/fire_pipeline 분석에 넘긴다.

    서버 파일 경로: media/demo_videos/{demo_video_key}.mp4
    앱 파일 경로(앱 내부): app/src/main/res/raw/{demo_video_key}.mp4 — 별개의 파일
    """
    row = db.get_cctv_by_sen_id(camera_sen_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"카메라 정보를 찾을 수 없습니다. camera_sen_id={camera_sen_id}",
        )

    if not row.get("is_demo"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="시연용 CCTV가 아닙니다. is_demo=1인 카메라만 demo/analyze를 사용할 수 있습니다.",
        )

    demo_video_key = row.get("demo_video_key") or "scenario3_fire"
    relative_path = DEMO_VIDEO_MAP.get(demo_video_key)
    if not relative_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"demo_video_key='{demo_video_key}'에 대응하는 영상 경로가 없습니다.",
        )

    video_path = (Path.cwd() / relative_path).resolve()
    if not video_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"서버 demo 영상 파일이 없습니다: {relative_path}\n"
                "Jetson 서버에 media/demo_videos/scenario3_fire.mp4 파일을 추가하세요.\n"
                "※ 앱 내부 raw 영상(app/src/main/res/raw/)과는 별개의 파일입니다."
            ),
        )

    try:
        from cctv.fire_pipeline import manager as fire_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="fire_pipeline 모듈이 설치되지 않았습니다.",
        )

    import uuid
    scenario_id = str(uuid.uuid4())

    ok = fire_manager.start_pipeline(
        camera_id=camera_sen_id,
        rtsp_url=str(video_path),
    )
    if not ok:
        logger.warning("Demo analyze pipeline already running camera_sen_id=%s", camera_sen_id)

    logger.info(
        "Demo VLM analysis started camera_sen_id=%s video_path=%s scenario_id=%s",
        camera_sen_id, video_path, scenario_id,
    )

    return {
        "success": True,
        "message": "Demo VLM 분석을 시작했습니다. 분석 결과는 WebSocket /ws/alerts로 전달됩니다.",
        "camera_id": camera_sen_id,
        "scenario_id": scenario_id,
    }


def _parse_create_camera(data: CameraCreate) -> dict:
    rtsp_url = data.rtsp_url
    parsed = _parse_rtsp_url(rtsp_url)

    ip_address = data.ip_address or data.device_id or parsed.get("ip_address")
    camera_username = data.camera_username or parsed.get("camera_username") or "admin"
    camera_password = data.camera_password or parsed.get("camera_password")

    if not ip_address or not camera_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="ip_address와 camera_password가 필요합니다.",
        )

    return {
        "ip_address": ip_address,
        "camera_username": camera_username,
        "camera_password": camera_password,
        "rtsp_url": rtsp_url,
        "space_id": _space_id_from_payload(data),
        "name": data.name or f"CCTV-{ip_address}",
    }


def _parse_rtsp_url(rtsp_url: str) -> dict:
    parsed = urlparse(rtsp_url)
    if parsed.scheme != "rtsp":
        return {}

    return {
        "ip_address": parsed.hostname,
        "camera_username": unquote(parsed.username or ""),
        "camera_password": unquote(parsed.password or ""),
        "rtsp_path": parsed.path or None,
    }


def _row_to_rtsp_url(row: dict, rtsp_path: str | None = None) -> str:
    return build_rtsp_url(
        ip_address=str(row["ip_address"]),
        username=str(row["camera_id"]),
        password=str(row["camera_pw"]),
        rtsp_path=rtsp_path,
    )


def _row_to_camera_out(row: dict) -> dict:
    is_demo = bool(row.get("is_demo"))
    # demo CCTV는 실제 RTSP URL이 없으므로 빌드하지 않음
    rtsp_url = row.get("rtsp_url") or ("" if is_demo else _row_to_rtsp_url(row))
    registered_at = _serialize_datetime(row.get("registered_at") or row.get("created_at"))
    is_online = bool(row.get("is_online"))

    return {
        "id": int(row["sen_id"]),
        "sen_id": int(row["sen_id"]),
        "sensor_id": row.get("sensor_id"),
        "device_id": row.get("ip_address"),
        "name": row.get("sen_name") or f"CCTV-{row.get('ip_address')}",
        "process_id": row.get("space_id"),
        "space_id": row.get("space_id"),
        "space_name": row.get("space_name"),
        "hazard_type": row.get("hazard_type"),
        "is_hazard": bool(row.get("is_hazard")),
        "is_active": is_online,
        "is_online": is_online,
        "is_demo": is_demo,
        "demo_video_key": row.get("demo_video_key"),
        "registered_at": registered_at,
        "camera": {
            "rtsp_url": rtsp_url,
            "ip_address": row.get("ip_address"),
            "camera_id": row.get("camera_id"),
            "health": bool(row.get("health")),
        },
    }


def _serialize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    return None
