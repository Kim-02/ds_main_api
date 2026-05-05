from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.session import get_db

from . import service
from .schemas import HeartRateBandCreate, HeartRateBandOut, HeartRateBandUpdate

DB = Annotated[AsyncSession, Depends(get_db)]
router = APIRouter(prefix="/heartrate/bands", tags=["heartrate"])


@router.get("/", response_model=list[HeartRateBandOut])
async def list_bands(db: DB, process_id: Optional[int] = Query(None)):
    return await service.list_bands(db, process_id=process_id)


@router.post("/", response_model=HeartRateBandOut, status_code=status.HTTP_201_CREATED)
async def create_band(data: HeartRateBandCreate, db: DB):
    return await service.create_band(db, data)


@router.get("/{sensor_id}", response_model=HeartRateBandOut)
async def get_band(sensor_id: int, db: DB):
    return await service.get_band(db, sensor_id)


@router.put("/{sensor_id}", response_model=HeartRateBandOut)
async def update_band(sensor_id: int, data: HeartRateBandUpdate, db: DB):
    return await service.update_band(db, sensor_id, data)


@router.delete("/{sensor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_band(sensor_id: int, db: DB):
    await service.delete_band(db, sensor_id)


# 웹 대시보드용 최신 심박 데이터
web_router = APIRouter(prefix="/api/web/sensors", tags=["dashboard"])


@web_router.get("/hb", summary="최신 심박 데이터 조회")
def get_web_hb(request: Request):
    return {"status": "success", "data": request.app.state.db.get_web_sensor_hb()}
