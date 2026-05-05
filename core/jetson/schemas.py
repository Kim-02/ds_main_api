from pydantic import BaseModel


class JetsonRegisterReq(BaseModel):
    dept_id: int
    app_id: str


class JetsonRegisterRes(BaseModel):
    jetson_id: str
    register_status: str
    api_base_url: str
    ws_url: str
