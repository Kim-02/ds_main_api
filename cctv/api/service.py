"""CCTV 카메라 서비스 레이어.

[MissingGreenlet 대응]
AsyncSession에서 SQLAlchemy relationship을 lazy load하면
"greenlet_spawn has not been called" 예외가 발생한다.
모든 Sensor 반환 시점에 camera relationship이 eager load된 상태임을 보장해야 한다.

→ database/crud/sensor.py 의 모든 함수가 selectinload(Sensor.camera)를 포함한
  재조회를 수행하도록 수정되었으므로, 이 파일에서 sensor.camera 접근은 안전하다.
"""
import logging
from urllib.parse import quote

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database.crud import sensor as crud
from database.models import Sensor, SensorType

from .schemas import AppCameraRegisterReq, CameraCreate, CameraUpdate

logger = logging.getLogger(__name__)


def build_rtsp_url(
    ip_address: str,
    username: str,
    password: str,
    rtsp_path: str | None = None,
) -> str:
    """IP + 계정 정보로 RTSP URL을 생성합니다.

    password에 @, :, / 같은 문자가 포함될 수 있으므로 URL encoding을 적용합니다.
    rtsp_path 생략 시 settings.fire_pipeline_rtsp_path 사용.
    """
    path = settings.fire_pipeline_rtsp_path

    if not path.startswith("/"):
        path = f"/{path}"

    encoded_username = quote(username, safe="")
    encoded_password = quote(password, safe="")

    return f"rtsp://{encoded_username}:{encoded_password}@{ip_address}{path}"


async def list_cameras(
    db: AsyncSession,
    process_id: int | None = None,
) -> list[Sensor]:
    sensors = await crud.get_all(db, process_id=process_id)
    return [s for s in sensors if s.sensor_type == SensorType.camera]


async def get_camera(
    db: AsyncSession,
    sensor_id: int,
) -> Sensor:
    """Sensor ID로 CCTV Sensor를 조회합니다. camera relationship이 eager load된 상태로 반환됩니다."""
    sensor = await crud.get_by_id(db, sensor_id)

    if not sensor or sensor.sensor_type != SensorType.camera:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Camera not found",
        )

    return sensor


def _get_camera_detail_or_400(sensor: Sensor):
    """sensor.camera가 eager load된 상태에서만 호출해야 합니다.

    crud 함수를 통해 얻은 Sensor는 항상 selectinload로 camera를 포함하므로 안전합니다.
    """
    if not sensor.camera:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="카메라 RTSP 정보가 없습니다.",
        )

    return sensor.camera


def _start_runtime_components(
    cam_id: int,
    rtsp_url: str,
    process_id: int,
) -> None:
    """카메라 등록 후 런타임 컴포넌트를 시작합니다.

    실패해도 DB 등록은 이미 완료된 상태이므로 경고 로그만 남기고 예외를 전파하지 않습니다.
    RTSP 연결 실패 / fire pipeline 모듈 미설치 모두 여기서 흡수됩니다.
    """
    try:
        from cctv.rtsp import register_reader
        from cctv.buffer import start_buffer

        register_reader(cam_id, rtsp_url)
        start_buffer(cam_id, process_id)
    except Exception:
        logger.warning(
            "RTSP reader/buffer 시작 실패 (cam_id=%s, rtsp=%s) — DB 등록은 유지됩니다.",
            cam_id,
            rtsp_url,
        )
        return  # runtime 실패가 등록 전체를 롤백하지 않도록 반환

    if settings.fire_pipeline_enabled:
        try:
            from cctv.fire_pipeline import manager as fire_manager
            fire_manager.start_pipeline(cam_id, rtsp_url)
        except ImportError:
            logger.warning("fire_pipeline 모듈 없음 — 파이프라인 건너뜀 (cam_id=%s)", cam_id)
        except Exception:
            logger.warning("fire pipeline 시작 실패 (cam_id=%s) — DB 등록은 유지됩니다.", cam_id)


def _stop_runtime_components(cam_id: int) -> None:
    """카메라 삭제 시 런타임 컴포넌트를 중단합니다.

    실패해도 DB 삭제는 계속 진행되도록 경고 로그만 남깁니다.
    """
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


async def create_camera(
    db: AsyncSession,
    data: CameraCreate,
) -> Sensor:
    """RTSP URL을 직접 받아 CCTV를 생성합니다.

    crud.create_camera()가 selectinload 재조회로 sensor.camera를 eager load하므로
    _get_camera_detail_or_400() 호출 시 MissingGreenlet이 발생하지 않습니다.
    """
    sensor_data = {
        "device_id": data.device_id,
        "name": data.name,
        "process_id": data.process_id,
    }

    try:
        sensor = await crud.create_camera(
            db,
            sensor_data=sensor_data,
            rtsp_url=data.rtsp_url,
        )
    except Exception as exc:
        logger.exception(
            "Camera create failed | device_id=%s | process_id=%s",
            data.device_id,
            data.process_id,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"카메라 등록에 실패했습니다: {exc}",
        ) from exc

    # crud.create_camera()에서 selectinload로 eager load된 상태이므로 안전하게 접근 가능
    cam = _get_camera_detail_or_400(sensor)

    # runtime 컴포넌트 실패는 DB 등록 결과에 영향을 주지 않음
    _start_runtime_components(
        cam_id=cam.id,
        rtsp_url=cam.rtsp_url,
        process_id=data.process_id,
    )

    return sensor


async def register_camera_from_app(
    db: AsyncSession,
    data: AppCameraRegisterReq,
) -> Sensor:
    """앱에서 IP/PW 기반으로 CCTV를 등록합니다.

    camera_username: 생략 시 "admin" (스키마 기본값)
    name: 생략 시 "CCTV-{ip_address}" 자동 설정
    """
    camera_name = data.name or f"CCTV-{data.ip_address}"

    rtsp_url = build_rtsp_url(
        ip_address=data.ip_address,
        username=data.camera_username,
        password=data.camera_password,
        rtsp_path=data.rtsp_path,
    )

    camera_data = CameraCreate(
        device_id=data.ip_address,
        name=camera_name,
        process_id=data.process_id,
        rtsp_url=rtsp_url,
    )

    logger.info(
        "Register CCTV from app | ip=%s | username=%s | process_id=%s | name=%s",
        data.ip_address,
        data.camera_username,
        data.process_id,
        camera_name,
    )

    return await create_camera(db, camera_data)


async def update_camera(
    db: AsyncSession,
    sensor_id: int,
    data: CameraUpdate,
) -> Sensor:
    """Sensor ID 기준으로 CCTV 정보를 수정합니다.

    crud.update()가 내부적으로 db.refresh()를 호출하면 relationship이 expire되어
    MissingGreenlet이 발생할 수 있다.
    수정 후 selectinload 재조회(crud.get_by_id)를 통해 camera를 eager load한 상태로 반환한다.
    """
    sensor = await get_camera(db, sensor_id)

    top_level_fields = {"name", "is_active"}
    top_level_update = {}
    camera_update = {}

    for field, value in data.model_dump(exclude_none=True).items():
        if field in top_level_fields:
            top_level_update[field] = value
        else:
            camera_update[field] = value

    if top_level_update:
        for key, value in top_level_update.items():
            setattr(sensor, key, value)

    if camera_update:
        cam = _get_camera_detail_or_400(sensor)
        for key, value in camera_update.items():
            setattr(cam, key, value)

    await db.flush()

    # flush 후 selectinload 재조회 — db.refresh()는 relationship을 expire시켜
    # 이후 sensor.camera 접근 시 MissingGreenlet이 발생하므로 사용하지 않는다.
    return await crud.get_by_id(db, sensor.id)


async def delete_camera(
    db: AsyncSession,
    sensor_id: int,
) -> None:
    """Sensor ID 기준으로 CCTV를 삭제합니다."""
    sensor = await get_camera(db, sensor_id)
    cam = _get_camera_detail_or_400(sensor)

    # runtime 컴포넌트 중단 실패는 DB 삭제를 막지 않음
    _stop_runtime_components(cam.id)

    await crud.delete(db, sensor)


async def get_fire_pipeline_status(
    db: AsyncSession,
    sensor_id: int,
) -> dict:
    """Sensor ID 기준으로 fire pipeline 상태를 조회합니다."""
    sensor = await get_camera(db, sensor_id)
    cam = _get_camera_detail_or_400(sensor)

    try:
        from cctv.fire_pipeline import manager as fire_manager
        status_data = fire_manager.get_status(cam.id)
    except ImportError:
        status_data = {"running": False, "latest_result": ""}

    return {
        "sensor_id": sensor_id,
        "camera_id": cam.id,
        "running": status_data.get("running", False),
        "latest_result": status_data.get("latest_result", ""),
    }


async def start_fire_pipeline(
    db: AsyncSession,
    sensor_id: int,
) -> dict:
    """Sensor ID 기준으로 fire pipeline을 수동 시작합니다."""
    sensor = await get_camera(db, sensor_id)
    cam = _get_camera_detail_or_400(sensor)

    try:
        from cctv.fire_pipeline import manager as fire_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="fire_pipeline 모듈이 설치되지 않았습니다.",
        )

    started = fire_manager.start_pipeline(cam.id, cam.rtsp_url)

    if not started:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이미 실행 중인 파이프라인입니다.",
        )

    return {
        "status": "started",
        "sensor_id": sensor_id,
        "camera_id": cam.id,
    }


async def stop_fire_pipeline(
    db: AsyncSession,
    sensor_id: int,
) -> dict:
    """Sensor ID 기준으로 fire pipeline을 수동 중단합니다."""
    sensor = await get_camera(db, sensor_id)
    cam = _get_camera_detail_or_400(sensor)

    try:
        from cctv.fire_pipeline import manager as fire_manager
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="fire_pipeline 모듈이 설치되지 않았습니다.",
        )

    stopped = fire_manager.stop_pipeline(cam.id)

    if not stopped:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="실행 중인 파이프라인이 없습니다.",
        )

    return {
        "status": "stopped",
        "sensor_id": sensor_id,
        "camera_id": cam.id,
    }
