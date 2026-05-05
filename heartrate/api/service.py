from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import sensor as crud
from database.models import Sensor, SensorType

from .schemas import HeartRateBandCreate, HeartRateBandUpdate


async def list_bands(db: AsyncSession, process_id: int | None = None) -> list[Sensor]:
    sensors = await crud.get_all(db, process_id=process_id)
    return [s for s in sensors if s.sensor_type == SensorType.heartrate_band]


async def get_band(db: AsyncSession, sensor_id: int) -> Sensor:
    s = await crud.get_by_id(db, sensor_id)
    if not s or s.sensor_type != SensorType.heartrate_band:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Heart rate band not found")
    return s


async def create_band(db: AsyncSession, data: HeartRateBandCreate) -> Sensor:
    sensor_data = {"device_id": data.device_id, "name": data.name, "process_id": data.process_id}
    return await crud.create_heartrate_band(
        db, sensor_data=sensor_data,
        threshold_heartrate=data.threshold_heartrate,
        worker_id=data.worker_id,
    )


async def update_band(db: AsyncSession, sensor_id: int, data: HeartRateBandUpdate) -> Sensor:
    sensor = await get_band(db, sensor_id)
    top, sub = {}, {}
    for field, value in data.model_dump(exclude_none=True).items():
        if field in ("name", "is_active"):
            top[field] = value
        else:
            sub[field] = value

    if top:
        await crud.update(db, sensor, **top)
    if sub and sensor.heartrate_band:
        for k, v in sub.items():
            setattr(sensor.heartrate_band, k, v)
        await db.flush()
        await db.refresh(sensor)
    return sensor


async def delete_band(db: AsyncSession, sensor_id: int) -> None:
    sensor = await get_band(db, sensor_id)
    await crud.delete(db, sensor)
