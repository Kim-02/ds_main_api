from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CameraCreate(BaseModel):
    device_id: str
    name: str
    process_id: int
    rtsp_url: str


class AppCameraRegisterReq(BaseModel):
    """앱에서 IP + 자격증명으로 카메라를 등록할 때 사용하는 요청 스키마.

    RTSP URL은 서버에서 ip_address + camera_username + camera_password +
    settings.fire_pipeline_rtsp_path 를 조합해 자동으로 생성합니다.

    camera_username: 생략 시 "admin" 사용
    name: 생략 시 "CCTV-{ip_address}" 자동 설정
    rtsp_path: 생략 시 settings.fire_pipeline_rtsp_path 사용 (기본 /stream)
    """

    ip_address: str
    camera_password: str
    process_id: int
    camera_username: str = "admin"
    name: Optional[str] = None
    rtsp_path: Optional[str] = None


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


class FirePipelineStatus(BaseModel):
    sensor_id: int
    camera_id: int
    running: bool
    latest_result: str


class FirePipelineActionResponse(BaseModel):
    """fire pipeline 수동 시작/중단 응답."""

    status: str
    sensor_id: int
    camera_id: int
