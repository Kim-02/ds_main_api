from typing import Optional

from fastapi import APIRouter, Query, Request, status

from . import service
from .schemas import TemperatureSensorCreate, TemperatureSensorOut, TemperatureSensorUpdate

router = APIRouter(prefix="/temperature/sensors", tags=["temperature"])


@router.get("/", response_model=list[TemperatureSensorOut])
async def list_sensors(request: Request, process_id: Optional[int] = Query(None)):
    return await service.list_sensors(request.app.state.db, process_id=process_id)


@router.post("/", response_model=TemperatureSensorOut, status_code=status.HTTP_201_CREATED)
async def create_sensor(data: TemperatureSensorCreate, request: Request):
    return await service.create_sensor(request.app.state.db, data)


@router.get("/{sensor_id}", response_model=TemperatureSensorOut)
async def get_sensor(sensor_id: int, request: Request):
    return await service.get_sensor(request.app.state.db, sensor_id)


@router.put("/{sensor_id}", response_model=TemperatureSensorOut)
async def update_sensor(sensor_id: int, data: TemperatureSensorUpdate, request: Request):
    return await service.update_sensor(request.app.state.db, sensor_id, data)


@router.delete("/{sensor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_sensor(sensor_id: int, request: Request):
    await service.delete_sensor(request.app.state.db, sensor_id)


web_router = APIRouter(prefix="/api/web/sensors", tags=["dashboard"])


@web_router.get("/th", summary="최신 온습도 데이터 조회")
def get_web_th(request: Request):
    return {"status": "success", "data": request.app.state.db.get_web_sensor_th()}
