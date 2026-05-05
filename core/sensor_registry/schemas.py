from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel


class SensorInfo(BaseModel):
    sensor_id: str
    sensor_type: Optional[str] = None
    sen_name: Optional[str] = None
    sen_locate: Optional[str] = None
    model: Optional[str] = "unknown"
    mqtt_topic: Optional[str] = None
    mdns_hostname: Optional[str] = None
    ip_addr: Optional[str] = None
    last_seen_at: Optional[Any] = None


class SensorRegisterReq(BaseModel):
    jetson_id: str
    selected_sensors: List[SensorInfo]


class SensorUnregisterReq(BaseModel):
    sensor_id: str
