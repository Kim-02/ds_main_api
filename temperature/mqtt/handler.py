"""온습도 MQTT 메시지 수신 처리."""
import logging

from database.db_handler import DatabaseHandler

logger = logging.getLogger(__name__)

_db: DatabaseHandler | None = None


def set_db(db: DatabaseHandler) -> None:
    global _db
    _db = db


async def handle(device_id: str, payload: dict) -> None:
    """payload: {"sensor_id": "...", "temperature": 36.5, "humidity": 60.0}"""
    temperature = payload.get("temperature")
    humidity = payload.get("humidity")

    if temperature is None:
        logger.warning("온습도 페이로드에 temperature 없음: %s", payload)
        return

    if _db is None:
        logger.warning("DB 핸들러 미설정 — 온습도 데이터 저장 스킵")
        return

    sensor_id = payload.get("sensor_id", device_id)
    _db.save_sensor_telemetry(
        sensor_id=sensor_id,
        temperature=temperature,
        humidity=humidity or 0.0,
    )
    logger.debug("온습도 저장: sensor=%s T=%.1f H=%.1f", sensor_id, temperature, humidity or 0)
