from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database.models import Camera, HeartRateBand, Sensor, SensorType, TemperatureSensor


async def get_all(db: AsyncSession, process_id: int | None = None) -> list[Sensor]:
    q = select(Sensor).options(
        selectinload(Sensor.temperature_sensor),
        selectinload(Sensor.heartrate_band),
        selectinload(Sensor.camera),
    )
    if process_id is not None:
        q = q.where(Sensor.process_id == process_id)
    result = await db.execute(q.order_by(Sensor.id))
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, sensor_id: int) -> Sensor | None:
    result = await db.execute(
        select(Sensor)
        .options(
            selectinload(Sensor.temperature_sensor),
            selectinload(Sensor.heartrate_band),
            selectinload(Sensor.camera),
        )
        .where(Sensor.id == sensor_id)
    )
    return result.scalar_one_or_none()


async def get_by_device_id(db: AsyncSession, device_id: str) -> Sensor | None:
    result = await db.execute(select(Sensor).where(Sensor.device_id == device_id))
    return result.scalar_one_or_none()


async def get_cameras_by_process(db: AsyncSession, process_id: int) -> list[Camera]:
    result = await db.execute(
        select(Camera)
        .join(Sensor, Camera.sensor_id == Sensor.id)
        .where(Sensor.process_id == process_id, Sensor.is_active == True)
    )
    return list(result.scalars().all())


async def get_bands_by_process(db: AsyncSession, process_id: int) -> list[HeartRateBand]:
    result = await db.execute(
        select(HeartRateBand)
        .join(Sensor, HeartRateBand.sensor_id == Sensor.id)
        .where(Sensor.process_id == process_id, Sensor.is_active == True)
    )
    return list(result.scalars().all())


async def create_temperature_sensor(
    db: AsyncSession,
    sensor_data: dict,
    threshold_temperature: float,
    threshold_humidity: float,
) -> Sensor:
    sensor = Sensor(**sensor_data, sensor_type=SensorType.temperature)
    db.add(sensor)
    await db.flush()

    detail = TemperatureSensor(
        sensor_id=sensor.id,
        threshold_temperature=threshold_temperature,
        threshold_humidity=threshold_humidity,
    )
    db.add(detail)
    await db.flush()
    await db.refresh(sensor)
    return sensor


async def create_heartrate_band(
    db: AsyncSession,
    sensor_data: dict,
    threshold_heartrate: int,
    worker_id: int | None,
) -> Sensor:
    sensor = Sensor(**sensor_data, sensor_type=SensorType.heartrate_band)
    db.add(sensor)
    await db.flush()

    detail = HeartRateBand(
        sensor_id=sensor.id,
        threshold_heartrate=threshold_heartrate,
        worker_id=worker_id,
    )
    db.add(detail)
    await db.flush()
    await db.refresh(sensor)
    return sensor


async def create_camera(db: AsyncSession, sensor_data: dict, rtsp_url: str) -> Sensor:
    sensor = Sensor(**sensor_data, sensor_type=SensorType.camera)
    db.add(sensor)
    await db.flush()

    camera = Camera(sensor_id=sensor.id, rtsp_url=rtsp_url)
    db.add(camera)
    await db.flush()
    await db.refresh(sensor)
    return sensor


async def update(db: AsyncSession, sensor: Sensor, **kwargs) -> Sensor:
    for key, value in kwargs.items():
        setattr(sensor, key, value)
    await db.flush()
    await db.refresh(sensor)
    return sensor


async def delete(db: AsyncSession, sensor: Sensor) -> None:
    await db.delete(sensor)
    await db.flush()
