"""CCTV 이상치 감지 진입점 — 화염/연기/스파크 탐지 시 호출."""
import logging

from database import AsyncSessionLocal
from database.crud import report as report_crud
from database.models import AnomalyStatus, AnomalyType

logger = logging.getLogger(__name__)

ANOMALY_LABELS = {"fire", "flame", "smoke", "spark"}

_active_events: dict[int, int] = {}  # camera_id → event_id


async def check(camera_id: int, detections: dict, frame_path: str) -> None:
    detected = [
        obj["label"] for obj in detections.get("objects", [])
        if obj.get("label", "").lower() in ANOMALY_LABELS
    ]
    if not detected or camera_id in _active_events:
        return

    async with AsyncSessionLocal() as db:
        from database.models import Camera, Sensor
        from sqlalchemy import select
        result = await db.execute(
            select(Sensor).join(Camera, Camera.sensor_id == Sensor.id).where(Camera.id == camera_id)
        )
        sensor = result.scalar_one_or_none()
        if not sensor:
            return

        event = await report_crud.create_anomaly_event(
            db,
            anomaly_type=AnomalyType.cctv,
            process_id=sensor.process_id,
            sensor_id=sensor.id,
            triggered_value=None,
            status=AnomalyStatus.active,
        )
        await db.commit()

        _active_events[camera_id] = event.id
        logger.info("CCTV anomaly: camera=%s labels=%s event=%s", camera_id, detected, event.id)

        from cctv.detection.handler import CCTVAnomalySession
        from core.detection_manager import manager

        manager.add(CCTVAnomalySession(
            event_id=event.id,
            process_id=sensor.process_id,
            sensor_id=sensor.id,
            camera_id=camera_id,
            initial_labels=detected,
            initial_frame_path=frame_path,
        ))


def clear_active_event(camera_id: int) -> None:
    _active_events.pop(camera_id, None)
