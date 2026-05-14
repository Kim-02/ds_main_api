from typing import Optional
from pydantic import BaseModel


class JetsonRegisterReq(BaseModel):
    dept_id: int
    app_id: str


class JetsonRegisterRes(BaseModel):
    jetson_id: str
    register_status: str
    api_base_url: str
    ws_url: str


class JetsonAdminCreateReq(BaseModel):
    jetson_wp: str
    jetson_loc: str
    jetson_status: Optional[bool] = True
    ip_addr: str
    port: int = 8080
    space_id: int


class JetsonUpdateReq(BaseModel):
    jetson_wp: Optional[str] = None
    jetson_loc: Optional[str] = None
    jetson_status: Optional[bool] = None
    ip_addr: Optional[str] = None
    port: Optional[int] = None
    space_id: Optional[int] = None


class JetsonOut(BaseModel):
    jetson_id: int
    jetson_wp: str
    jetson_loc: str
    jetson_status: bool
    ip_addr: str
    port: int
    space_id: Optional[int] = None
    space_name: Optional[str] = None


class JetsonAppRegisterReq(BaseModel):
    """앱이 mDNS로 발견한 Jetson을 등록할 때 사용하는 요청 모델."""
    jetson_wp: str = "Jetson"
    jetson_loc: Optional[str] = ""
    jetson_status: Optional[bool] = True
    ip_addr: str
    port: int = 8080
    space_id: int


class JetsonAppRegisterRes(BaseModel):
    success: bool
    message: str
    data: Optional[JetsonOut] = None

