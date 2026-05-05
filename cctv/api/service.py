from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import sensor as crud
from database.models import Sensor, SensorType

from .schemas import CameraCreate, CameraUpdate


async def list_cameras(db: AsyncSession, process_id: int | None = None) -> list[Sensor]:
    sensors = await crud.get_all(db, process_id=process_id)
    return [s for s in sensors if s.sensor_type == SensorType.camera]


async def get_camera(db: AsyncSession, sensor_id: int) -> Sensor:
    s = await crud.get_by_id(db, sensor_id)
    if not s or s.sensor_type != SensorType.camera:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Camera not found")
    return s


async def create_camera(db: AsyncSession, data: CameraCreate) -> Sensor:
    sensor_data = {"device_id": data.device_id, "name": data.name, "process_id": data.process_id}
    sensor = await crud.create_camera(db, sensor_data=sensor_data, rtsp_url=data.rtsp_url)

    # 등록 즉시 RTSP 리더와 버퍼 시작
    from cctv.rtsp import register_reader
    from cctv.buffer import start_buffer
    cam = sensor.camera
    if cam:
        register_reader(cam.id, cam.rtsp_url)
        start_buffer(cam.id, data.process_id)

    return sensor


async def update_camera(db: AsyncSession, sensor_id: int, data: CameraUpdate) -> Sensor:
    sensor = await get_camera(db, sensor_id)
    top, sub = {}, {}
    for field, value in data.model_dump(exclude_none=True).items():
        if field in ("name", "is_active"):
            top[field] = value
        else:
            sub[field] = value

    if top:
        await crud.update(db, sensor, **top)
    if sub and sensor.camera:
        for k, v in sub.items():
            setattr(sensor.camera, k, v)
        await db.flush()
        await db.refresh(sensor)
    return sensor


async def delete_camera(db: AsyncSession, sensor_id: int) -> None:
    sensor = await get_camera(db, sensor_id)
    if sensor.camera:
        from cctv.rtsp import stop_reader
        from cctv.buffer import stop_buffer
        stop_buffer(sensor.camera.id)
        stop_reader(sensor.camera.id)
    await crud.delete(db, sensor)
