from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class WorkerCreate(BaseModel):
    employee_id: str
    name: str
    process_id: Optional[int] = None
    health_baseline: Optional[dict[str, Any]] = None


class WorkerUpdate(BaseModel):
    name: Optional[str] = None
    process_id: Optional[int] = None
    health_baseline: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None


class WorkerOut(BaseModel):
    id: int
    employee_id: str
    name: str
    process_id: Optional[int]
    health_baseline: Optional[dict[str, Any]]
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# 아래부터 추가한 스키마
class WorkerDbOut(BaseModel):
    dept_id: int
    name: str
    is_manager: int
    sen_id: Optional[int] = None
    sensor_id: Optional[str] = None
    sensor_type: Optional[str] = None
    sensor_name: Optional[str] = None


class AssignHeartBandRequest(BaseModel):
    sensor_id: str
    jetson_id: Optional[int] = None
    interval_ms: int = 5000


class AssignHeartBandResponse(BaseModel):
    success: bool
    message: str
    data: dict[str, Any]


class UnassignSensorResponse(BaseModel):
    success: bool
    message: str
