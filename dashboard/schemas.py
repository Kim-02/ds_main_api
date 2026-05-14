from typing import List, Optional
from pydantic import BaseModel


class DashboardSummaryData(BaseModel):
    space_id: int
    space_name: Optional[str] = None
    jetson_id: Optional[int] = None
    jetson_name: Optional[str] = None
    danger_alert_count: int = 0
    sensor_total: int = 0
    cctv_total: int = 0
    worker_total: int = 0


class DashboardSummaryResponse(BaseModel):
    status: str
    data: Optional[DashboardSummaryData] = None


class RecentAlertDto(BaseModel):
    event_id: Optional[int] = None
    space_id: Optional[int] = None
    title: Optional[str] = None
    message: Optional[str] = None
    level: Optional[str] = "danger"
    source: Optional[str] = "vlm"
    camera_name: Optional[str] = None
    created_at: Optional[str] = None
    is_read: Optional[int] = 0


class RecentAlertsResponse(BaseModel):
    status: str
    data: List[RecentAlertDto] = []


class DashboardSensorDto(BaseModel):
    sen_id: Optional[int] = None
    sensor_id: Optional[str] = None
    sensor_type: Optional[str] = None
    sen_name: Optional[str] = None
    sen_locate: Optional[str] = None
    model: Optional[str] = None
    mqtt_topic: Optional[str] = None
    is_online: Optional[int] = None
    last_seen_at: Optional[str] = None
    space_id: Optional[int] = None


class DashboardSensorsResponse(BaseModel):
    status: str
    data: List[DashboardSensorDto] = []


class DashboardCctvDto(BaseModel):
    sen_id: Optional[int] = None
    ip_address: Optional[str] = None
    camera_id: Optional[str] = None
    health: Optional[int] = None
    space_id: Optional[int] = None
    sen_name: Optional[str] = None
    sensor_id: Optional[str] = None
    is_online: Optional[int] = None


class DashboardCctvsResponse(BaseModel):
    status: str
    data: List[DashboardCctvDto] = []


class DashboardWorkerDto(BaseModel):
    dept_id: Optional[int] = None
    name: Optional[str] = None
    is_manager: Optional[int] = None
    sen_id: Optional[int] = None
    sensor_id: Optional[str] = None
    sensor_name: Optional[str] = None
    sensor_type: Optional[str] = None
    space_id: Optional[int] = None


class DashboardWorkersResponse(BaseModel):
    status: str
    data: List[DashboardWorkerDto] = []

