"""CCTV 카메라 서비스 레이어 — MariaDB sensor + camera_info 기반."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from urllib.parse import quote, unquote, urlparse

from fastapi import HTTPException, status

from config import settings
from database.db_handler import DatabaseHandler

from .schemas import AppCameraRegisterReq, CameraCreate, CameraUpdate

logger = logging.getLogger(__name__)


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
            fire_manager.start_pipeline(cam_id, rtsp_url)
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


def _space_id_from_payload(data: Any) -> int | None:
    space_id = getattr(data, "space_id", None)
    if space_id is None:
        space_id = getattr(data, "process_id", None)
    return space_id


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
    rtsp_url = row.get("rtsp_url") or _row_to_rtsp_url(row)
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
