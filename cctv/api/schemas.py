from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class CameraCreate(BaseModel):
    """RTSP URL을 알고 있을 때 직접 CCTV를 등록한다.

    process_id는 기존 앱 호환 필드이며 MariaDB에서는 space_id로 취급한다.
    """

    rtsp_url: str
    device_id: Optional[str] = None
    ip_address: Optional[str] = None
    name: Optional[str] = None
    process_id: Optional[int] = None
    space_id: Optional[int] = None
    camera_username: Optional[str] = None
    camera_password: Optional[str] = None


class AppCameraRegisterReq(BaseModel):
    """앱에서 IP + 자격증명으로 카메라를 등록할 때 사용하는 요청 스키마.

    RTSP URL은 서버에서 ip_address + camera_username + camera_password +
    settings.fire_pipeline_rtsp_path 를 조합해 자동으로 생성한다.
    process_id는 기존 앱 호환 필드이며 MariaDB에서는 space_id로 취급한다.
    """

    ip_address: str
    camera_password: str
    process_id: Optional[int] = None
    space_id: Optional[int] = None
    camera_username: str = "admin"
    name: Optional[str] = None
    rtsp_path: Optional[str] = None


class CameraUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None
    health: Optional[bool] = None
    rtsp_url: Optional[str] = None
    ip_address: Optional[str] = None
    camera_username: Optional[str] = None
    camera_password: Optional[str] = None
    process_id: Optional[int] = None
    space_id: Optional[int] = None
    rtsp_path: Optional[str] = None


class CameraDetail(BaseModel):
    rtsp_url: str
    ip_address: str
    camera_id: str
    health: bool


class CameraOut(BaseModel):
    id: int
    sen_id: int
    sensor_id: Optional[str] = None
    device_id: Optional[str] = None
    name: str
    process_id: Optional[int] = None
    space_id: Optional[int] = None
    space_name: Optional[str] = None
    hazard_type: Optional[str] = None
    is_hazard: bool = False
    is_active: bool
    is_online: bool
    registered_at: Optional[datetime] = None
    camera: Optional[CameraDetail] = None

    is_demo: bool = False
    demo_video_key: Optional[str] = None

    registered_at: Optional[datetime] = None
    camera: Optional[CameraDetail] = None

class FirePipelineStatus(BaseModel):
    sensor_id: int
    camera_id: int
    running: bool
    latest_result: str
    latest_error: str = ""
    rtsp_reader: dict[str, Any] = Field(default_factory=dict)
    frame_buffer: dict[str, Any] = Field(default_factory=dict)
    fire_pipeline: dict[str, Any] = Field(default_factory=dict)


class FirePipelineActionResponse(BaseModel):
    status: str
    sensor_id: int
    camera_id: int


class VideoPipelineStartReq(BaseModel):
    """서버 실행 디렉터리의 영상 파일을 가상 CCTV로 실행한다."""

    video_name: str
    space_id: int = 1
    camera_id: Optional[int] = None
    restart: bool = True


class VideoPipelineActionResponse(BaseModel):
    status: str
    camera_id: int
    space_id: int
    video_name: str
    video_path: str
    rtsp_reader: dict[str, Any] = Field(default_factory=dict)
    frame_buffer: dict[str, Any] = Field(default_factory=dict)
    fire_pipeline: dict[str, Any] = Field(default_factory=dict)


class DemoCctvRegisterReq(BaseModel):
    """앱에서 시연용 가상 CCTV를 등록할 때 사용하는 요청 스키마."""
    space_id: int
    jetson_id: Optional[int] = None
    name: str = "시연용 화재 CCTV"
    demo_video_key: str = "scenario3_fire"


class DemoCctvRegisterResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None


class DemoAnalyzeResponse(BaseModel):
    success: bool
    message: str
    camera_id: Optional[int] = None
    scenario_id: Optional[str] = None
