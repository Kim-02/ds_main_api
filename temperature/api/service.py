from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import sensor as crud
from database.models import Sensor, SensorType

from .schemas import TemperatureSensorCreate, TemperatureSensorUpdate


async def list_sensors(db: AsyncSession, process_id: int | None = None) -> list[Sensor]:
    sensors = await crud.get_all(db, process_id=process_id)
    return [s for s in sensors if s.sensor_type == SensorType.temperature]


async def get_sensor(db: AsyncSession, sensor_id: int) -> Sensor:
    s = await crud.get_by_id(db, sensor_id)
    if not s or s.sensor_type != SensorType.temperature:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Temperature sensor not found")
    return s


async def create_sensor(db: AsyncSession, data: TemperatureSensorCreate) -> Sensor:
    sensor_data = {"device_id": data.device_id, "name": data.name, "process_id": data.process_id}
    return await crud.create_temperature_sensor(
        db,
        sensor_data=sensor_data,
        threshold_temperature=data.threshold_temperature,
        threshold_humidity=data.threshold_humidity,
    )


async def update_sensor(db: AsyncSession, sensor_id: int, data: TemperatureSensorUpdate) -> Sensor:
    sensor = await get_sensor(db, sensor_id)
    top = {}
    sub = {}
    for field, value in data.model_dump(exclude_none=True).items():
        if field in ("name", "is_active"):
            top[field] = value
        else:
            sub[field] = value

    if top:
        await crud.update(db, sensor, **top)
    if sub and sensor.temperature_sensor:
        for k, v in sub.items():
            setattr(sensor.temperature_sensor, k, v)
        await db.flush()
        await db.refresh(sensor)
    return sensor


async def delete_sensor(db: AsyncSession, sensor_id: int) -> None:
    sensor = await get_sensor(db, sensor_id)
    await crud.delete(db, sensor)
