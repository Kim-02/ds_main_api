from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, model_validator


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
    model_config = ConfigDict(extra="allow")

    sensor_id: Optional[str] = None
    dept_id: Optional[int] = None
    jetson_id: Optional[int | str] = None
    interval_ms: int = 5000
    sensor_type: Optional[str] = None
    sen_name: Optional[str] = None
    sen_locate: Optional[str] = None
    model: Optional[str] = None
    mqtt_topic: Optional[str] = None
    telemetry_topic: Optional[str] = None
    mqtt_base: Optional[str] = None
    mdns_hostname: Optional[str] = None
    ip_addr: Optional[str] = None
    is_online: Optional[bool] = True

    @model_validator(mode="before")
    @classmethod
    def normalize_app_payload(cls, data):
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        nested = normalized.get("sensor") or normalized.get("watch") or normalized.get("device")
        if isinstance(nested, dict):
            for key, value in nested.items():
                normalized.setdefault(key, value)

        aliases = {
            "sensor_id": ("sensorId", "watch_id", "watchId", "watchSensorId", "device_id", "deviceId", "band_id", "bandId"),
            "dept_id": ("deptId", "worker_id", "workerId", "employee_id", "employeeId"),
            "jetson_id": ("jetsonId",),
            "interval_ms": ("intervalMs", "interval", "intervalMillis"),
            "sensor_type": ("sensorType", "type"),
            "sen_name": ("sensor_name", "sensorName", "name", "watchName"),
            "sen_locate": ("sensor_location", "sensorLocation", "location", "watchLocation"),
            "mqtt_topic": ("mqttTopic",),
            "telemetry_topic": ("telemetryTopic",),
            "mqtt_base": ("mqttBase",),
            "mdns_hostname": ("mdnsHostname",),
            "ip_addr": ("ipAddr", "ipAddress"),
            "is_online": ("isOnline",),
        }
        for target, source_names in aliases.items():
            if normalized.get(target) not in (None, ""):
                continue
            for source_name in source_names:
                value = normalized.get(source_name)
                if value not in (None, ""):
                    normalized[target] = value
                    break

        return normalized


class AssignHeartBandResponse(BaseModel):
    success: bool
    message: str
    data: dict[str, Any]


class UnassignSensorResponse(BaseModel):
    success: bool
    message: str
