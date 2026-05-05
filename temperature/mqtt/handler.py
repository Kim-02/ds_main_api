"""온습도 MQTT 메시지 수신 처리."""
import logging

from database import AsyncSessionLocal
from database.crud import sensor as sensor_crud
from database.models import SensorReading

logger = logging.getLogger(__name__)


async def handle(device_id: str, payload: dict) -> None:
    """payload: {"temperature": 36.5, "humidity": 60.0}"""
    temperature = payload.get("temperature")
    humidity = payload.get("humidity")

    if temperature is None:
        logger.warning("Temperature payload missing 'temperature': %s", payload)
        return

    async with AsyncSessionLocal() as db:
        sensor = await sensor_crud.get_by_device_id(db, device_id)
        if not sensor:
            logger.warning("Unknown temperature sensor device_id=%s", device_id)
            return

        db.add(SensorReading(sensor_id=sensor.id, data={"temperature": temperature, "humidity": humidity}))
        await db.commit()

    from temperature.detection.detector import check
    await check(device_id=device_id, temperature=temperature, humidity=humidity or 0.0)
