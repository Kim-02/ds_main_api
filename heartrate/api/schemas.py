from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class HeartRateBandCreate(BaseModel):
    device_id: str
    name: str
    process_id: int
    threshold_heartrate: int = 120
    worker_id: Optional[int] = None


class HeartRateBandUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None
    threshold_heartrate: Optional[int] = None
    worker_id: Optional[int] = None


class HeartRateBandDetail(BaseModel):
    threshold_heartrate: int
    worker_id: Optional[int]

    model_config = {"from_attributes": True}


class HeartRateBandOut(BaseModel):
    id: int
    device_id: str
    name: str
    process_id: int
    is_active: bool
    registered_at: datetime
    heartrate_band: Optional[HeartRateBandDetail] = None

    model_config = {"from_attributes": True}
