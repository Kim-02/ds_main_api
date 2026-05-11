from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from config import settings
from .schemas import TemperatureSensorCreate, TemperatureSensorUpdate


def _row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": row["sen_id"],
        "device_id": row.get("sensor_id") or "",
        "name": row.get("sen_name") or "",
        "process_id": row.get("space_id"),
        "is_active": bool(row.get("is_online", False)),
        "registered_at": row.get("registered_at"),
        "temperature_sensor": {
            "threshold_temperature": settings.default_temp_threshold,
            "threshold_humidity": settings.default_humidity_threshold,
        },
    }


async def list_sensors(db, process_id: int | None = None) -> list[dict]:
    rows = await run_in_threadpool(db.get_temp_sensors, process_id)
    return [_row_to_out(r) for r in rows]


async def get_sensor(db, sensor_id: int) -> dict:
    row = await run_in_threadpool(db.get_temp_sensor_by_id, sensor_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Temperature sensor not found")
    return _row_to_out(row)


async def create_sensor(db, data: TemperatureSensorCreate) -> dict:
    row = await run_in_threadpool(
        db.create_temp_sensor,
        data.device_id,
        data.name,
        data.process_id,
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="생성 실패")
    return _row_to_out(row)


async def update_sensor(db, sensor_id: int, data: TemperatureSensorUpdate) -> dict:
    await get_sensor(db, sensor_id)
    kwargs = data.model_dump(exclude_none=True)
    row = await run_in_threadpool(db.update_temp_sensor, sensor_id, **kwargs)
    return _row_to_out(row)


async def delete_sensor(db, sensor_id: int) -> None:
    await get_sensor(db, sensor_id)
    await run_in_threadpool(db.delete_temp_sensor, sensor_id)
