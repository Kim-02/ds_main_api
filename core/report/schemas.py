import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AnomalyType(str, enum.Enum):
    temperature = "temperature"
    heartrate = "heartrate"
    cctv = "cctv"
    unknown = "unknown"


class AnomalyStatus(str, enum.Enum):
    active = "active"
    resolved = "resolved"
    stopped = "stopped"


class VLMQueryOut(BaseModel):
    id: int
    anomaly_event_id: int
    queried_at: datetime
    prompt: str
    response: Optional[str]
    frame_path: Optional[str]

    model_config = {"from_attributes": True}


class AnomalyEventOut(BaseModel):
    id: int
    anomaly_type: str
    process_id: int
    sensor_id: int
    triggered_value: Optional[float]
    start_time: datetime
    end_time: Optional[datetime]
    status: str

    model_config = {"from_attributes": True}


class AnomalyEventDetailOut(AnomalyEventOut):
    vlm_queries: list[VLMQueryOut] = []
    report: Optional["ReportOut"] = None


class ReportCreate(BaseModel):
    anomaly_event_id: int
    content: str
    action_taken: Optional[str] = None
    created_by: Optional[str] = None


class ReportUpdate(BaseModel):
    content: Optional[str] = None
    action_taken: Optional[str] = None


class ReportOut(BaseModel):
    id: int
    anomaly_event_id: int
    content: str
    action_taken: Optional[str]
    created_by: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class StopDetectionRequest(BaseModel):
    reason: Optional[str] = None
