"""심박 MQTT 메시지 수신 처리."""
import logging

from database import AsyncSessionLocal
from database.crud import sensor as sensor_crud
from database.models import SensorReading

logger = logging.getLogger(__name__)


async def handle(device_id: str, payload: dict) -> None:
    """payload: {"heartrate": 110}"""
    heartrate = payload.get("heartrate")
    if heartrate is None:
        logger.warning("Heartrate payload missing 'heartrate': %s", payload)
        return

    async with AsyncSessionLocal() as db:
        sensor = await sensor_crud.get_by_device_id(db, device_id)
        if not sensor:
            logger.warning("Unknown heartrate band device_id=%s", device_id)
            return

        db.add(SensorReading(sensor_id=sensor.id, data={"heartrate": heartrate}))
        await db.commit()

    from heartrate.detection.detector import check
    await check(device_id=device_id, heartrate=heartrate)
