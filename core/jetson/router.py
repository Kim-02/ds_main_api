from fastapi import APIRouter, HTTPException, Request

from .schemas import JetsonRegisterReq, JetsonRegisterRes

router = APIRouter(prefix="/api/jetson", tags=["jetson"])


@router.post("/register", response_model=JetsonRegisterRes, summary="Jetson 등록 및 앱 연동")
def register_jetson(req: JetsonRegisterReq, request: Request):
    db = request.app.state.db
    jetson = db.register_jetson_connection(req.dept_id, req.app_id)
    print(jetson['ip_addr'])
    if not jetson:
        raise HTTPException(status_code=404, detail="DB에 Jetson 초기 정보가 없습니다.")

    return JetsonRegisterRes(
        jetson_id=f"jetson-{jetson['jetson_id']:02d}",
        register_status="success",
        api_base_url=f"http://{jetson['ip_addr']}:{jetson['port']}",
        ws_url=f"ws://{jetson['ip_addr']}:{jetson['port']}/ws/alerts",
    )
