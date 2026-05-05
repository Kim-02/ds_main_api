from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CameraCreate(BaseModel):
    device_id: str
    name: str
    process_id: int
    rtsp_url: str


class CameraUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None
    rtsp_url: Optional[str] = None


class CameraDetail(BaseModel):
    rtsp_url: str

    model_config = {"from_attributes": True}


class CameraOut(BaseModel):
    id: int
    device_id: str
    name: str
    process_id: int
    is_active: bool
    registered_at: datetime
    camera: Optional[CameraDetail] = None

    model_config = {"from_attributes": True}
