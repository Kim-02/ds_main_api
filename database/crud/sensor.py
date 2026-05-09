from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database.models import Camera, HeartRateBand, Sensor, SensorType, TemperatureSensor


# 모든 Sensor 조회 시 공통으로 사용하는 eager load 옵션.
# sensor.camera / sensor.temperature_sensor / sensor.heartrate_band 접근 시
# AsyncSession에서 lazy loading → MissingGreenlet 이 발생하지 않도록 한다.
_SENSOR_LOAD_OPTIONS = [
    selectinload(Sensor.temperature_sensor),
    selectinload(Sensor.heartrate_band),
    selectinload(Sensor.camera),
]


async def get_all(db: AsyncSession, process_id: int | None = None) -> list[Sensor]:
    q = select(Sensor).options(*_SENSOR_LOAD_OPTIONS)
    if process_id is not None:
        q = q.where(Sensor.process_id == process_id)
    result = await db.execute(q.order_by(Sensor.id))
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, sensor_id: int) -> Sensor | None:
    result = await db.execute(
        select(Sensor)
        .options(*_SENSOR_LOAD_OPTIONS)
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

    # db.refresh()는 relationship을 eager load하지 않으므로 selectinload 재조회 사용
    result = await db.execute(
        select(Sensor).options(*_SENSOR_LOAD_OPTIONS).where(Sensor.id == sensor.id)
    )
    return result.scalar_one()


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

    # db.refresh()는 relationship을 eager load하지 않으므로 selectinload 재조회 사용
    result = await db.execute(
        select(Sensor).options(*_SENSOR_LOAD_OPTIONS).where(Sensor.id == sensor.id)
    )
    return result.scalar_one()


async def create_camera(db: AsyncSession, sensor_data: dict, rtsp_url: str) -> Sensor:
    sensor = Sensor(**sensor_data, sensor_type=SensorType.camera)
    db.add(sensor)
    await db.flush()

    camera = Camera(sensor_id=sensor.id, rtsp_url=rtsp_url)
    db.add(camera)
    await db.flush()

    # [핵심 수정] db.refresh(sensor)는 scalar 컬럼만 갱신하고 relationship은
    # lazy 상태로 남겨 둔다. AsyncSession에서 lazy load를 시도하면
    # greenlet_spawn has not been called (MissingGreenlet) 예외가 발생한다.
    # selectinload 옵션을 포함한 재조회로 camera relationship을 eager load한다.
    result = await db.execute(
        select(Sensor).options(*_SENSOR_LOAD_OPTIONS).where(Sensor.id == sensor.id)
    )
    return result.scalar_one()


async def update(db: AsyncSession, sensor: Sensor, **kwargs) -> Sensor:
    for key, value in kwargs.items():
        setattr(sensor, key, value)
    await db.flush()

    # db.refresh()는 relationship을 expire시켜 lazy load를 유발하므로
    # selectinload 재조회로 교체한다.
    result = await db.execute(
        select(Sensor).options(*_SENSOR_LOAD_OPTIONS).where(Sensor.id == sensor.id)
    )
    return result.scalar_one()


async def delete(db: AsyncSession, sensor: Sensor) -> None:
    await db.delete(sensor)
    await db.flush()
