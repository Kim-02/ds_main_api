"""온도 이상치 감지 진입점."""
import logging

from database import AsyncSessionLocal
from database.crud import report as report_crud
from database.crud import sensor as sensor_crud
from database.models import AnomalyStatus, AnomalyType

logger = logging.getLogger(__name__)

_active_events: dict[int, int] = {}  # sensor_id → event_id


async def check(device_id: str, temperature: float, humidity: float) -> None:
    async with AsyncSessionLocal() as db:
        sensor = await sensor_crud.get_by_device_id(db, device_id)
        if not sensor or not sensor.is_active or not sensor.temperature_sensor:
            return

        threshold = sensor.temperature_sensor.threshold_temperature
        sensor_id = sensor.id

        if temperature >= threshold:
            if sensor_id in _active_events:
                return

            event = await report_crud.create_anomaly_event(
                db,
                anomaly_type=AnomalyType.temperature,
                process_id=sensor.process_id,
                sensor_id=sensor_id,
                triggered_value=temperature,
                status=AnomalyStatus.active,
            )
            await db.commit()

            _active_events[sensor_id] = event.id
            logger.info("Temperature anomaly: sensor=%s temp=%.1f threshold=%.1f event=%s",
                        device_id, temperature, threshold, event.id)

            from temperature.detection.handler import TemperatureAnomalySession
            from core.detection_manager import manager

            manager.add(TemperatureAnomalySession(
                event_id=event.id,
                process_id=sensor.process_id,
                sensor_id=sensor_id,
                device_id=device_id,
                threshold=threshold,
            ))

        else:
            event_id = _active_events.pop(sensor_id, None)
            if event_id:
                from core.detection_manager import manager
                await manager.stop(event_id)
                logger.info("Temperature normal, stopping event %s", event_id)


def clear_active_event(sensor_id: int) -> None:
    _active_events.pop(sensor_id, None)
