from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TemperatureSensorCreate(BaseModel):
    device_id: str
    name: str
    process_id: int
    threshold_temperature: float = 35.0
    threshold_humidity: float = 80.0


class TemperatureSensorUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None
    threshold_temperature: Optional[float] = None
    threshold_humidity: Optional[float] = None


class TemperatureSensorDetail(BaseModel):
    threshold_temperature: float
    threshold_humidity: float

    model_config = {"from_attributes": True}


class TemperatureSensorOut(BaseModel):
    id: int
    device_id: str
    name: str
    process_id: int
    is_active: bool
    registered_at: datetime
    temperature_sensor: Optional[TemperatureSensorDetail] = None

    model_config = {"from_attributes": True}
