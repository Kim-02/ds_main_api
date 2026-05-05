from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import TemperatureSensorCreate, TemperatureSensorOut, TemperatureSensorUpdate

DB = Annotated[AsyncSession, Depends(get_db)]
router = APIRouter(prefix="/temperature/sensors", tags=["temperature"])


@router.get("/", response_model=list[TemperatureSensorOut])
async def list_sensors(db: DB, process_id: Optional[int] = Query(None)):
    return await service.list_sensors(db, process_id=process_id)


@router.post("/", response_model=TemperatureSensorOut, status_code=status.HTTP_201_CREATED)
async def create_sensor(data: TemperatureSensorCreate, db: DB):
    return await service.create_sensor(db, data)


@router.get("/{sensor_id}", response_model=TemperatureSensorOut)
async def get_sensor(sensor_id: int, db: DB):
    return await service.get_sensor(db, sensor_id)


@router.put("/{sensor_id}", response_model=TemperatureSensorOut)
async def update_sensor(sensor_id: int, data: TemperatureSensorUpdate, db: DB):
    return await service.update_sensor(db, sensor_id, data)


@router.delete("/{sensor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_sensor(sensor_id: int, db: DB):
    await service.delete_sensor(db, sensor_id)


# 웹 대시보드용 최신 온습도 데이터
web_router = APIRouter(prefix="/api/web/sensors", tags=["dashboard"])


@web_router.get("/th", summary="최신 온습도 데이터 조회")
def get_web_th(request: Request):
    return {"status": "success", "data": request.app.state.db.get_web_sensor_th()}
