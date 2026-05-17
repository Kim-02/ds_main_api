import logging
from typing import List

from fastapi import APIRouter, HTTPException, Request

from .schemas import (
    JetsonAdminCreateReq,
    JetsonAppRegisterReq,
    JetsonAppRegisterRes,
    JetsonOut,
    JetsonRegisterReq,
    JetsonRegisterRes,
    JetsonUpdateReq,
)

router = APIRouter(prefix="/api/jetson", tags=["jetson"])
logger = logging.getLogger(__name__)

# /api/v1/jetsons 에 마운트되는 앱 전용 라우터
router_v1 = APIRouter(prefix="/jetsons", tags=["jetson"])


@router_v1.post(
    "/register",
    response_model=JetsonAppRegisterRes,
    summary="앱에서 mDNS 발견 Jetson 등록 (ip_addr+port 기준 upsert)",
)
def register_jetson_from_app(req: JetsonAppRegisterReq, request: Request):
    try:
        row, is_new = request.app.state.db.upsert_jetson(req.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not row:
        raise HTTPException(status_code=500, detail="Jetson 등록에 실패했습니다.")
    msg = "Jetson 등록 성공" if is_new else "이미 등록된 Jetson 정보를 갱신했습니다."
    return JetsonAppRegisterRes(success=True, message=msg, data=row)


@router_v1.get("", response_model=List[JetsonOut], summary="Jetson 전체 목록 조회 (v1)")
def list_jetsons_v1(request: Request):
    return request.app.state.db.get_all_jetsons()


@router_v1.get("/{jetson_id}", response_model=JetsonOut, summary="Jetson 단건 조회 (v1)")
def get_jetson_v1(jetson_id: int, request: Request):
    row = request.app.state.db.get_jetson_by_id(jetson_id)
    if not row:
        raise HTTPException(status_code=404, detail="해당 Jetson이 없습니다.")
    return row


@router_v1.post("/{jetson_id}/unregister", summary="Jetson 비활성화 (status=0, 기록 유지)")
def unregister_jetson_from_app(jetson_id: int, request: Request):
    db = request.app.state.db
    if not db.get_jetson_by_id(jetson_id):
        raise HTTPException(status_code=404, detail="해당 Jetson이 없습니다.")
    if not db.deactivate_jetson(jetson_id):
        raise HTTPException(status_code=500, detail="Jetson 등록 해제에 실패했습니다.")
    return {"success": True, "message": "Jetson 등록 해제 완료"}


@router_v1.delete("/{jetson_id}", summary="Jetson 완전 삭제 (연결된 센서/CCTV runtime 포함)")
def delete_jetson_v1(jetson_id: int, request: Request):
    db = request.app.state.db
    if not db.get_jetson_by_id(jetson_id):
        raise HTTPException(status_code=404, detail="해당 Jetson이 없습니다.")

    # 연결된 CCTV runtime 먼저 중단
    camera_sen_ids = db.get_camera_sen_ids_by_jetson_id(jetson_id)
    if camera_sen_ids:
        from cctv.api.service import _stop_runtime_components
        for sen_id in camera_sen_ids:
            _stop_runtime_components(sen_id)

    if not db.delete_jetson_cascade(jetson_id):
        raise HTTPException(status_code=500, detail="Jetson 삭제에 실패했습니다.")

    return {"success": True, "message": "Jetson 삭제 완료"}


@router.get("", response_model=List[JetsonOut], summary="Jetson 전체 목록 조회")
def list_jetsons(request: Request):
    return request.app.state.db.get_all_jetsons()


@router.get("/{jetson_id}", response_model=JetsonOut, summary="Jetson 단건 조회")
def get_jetson(jetson_id: int, request: Request):
    row = request.app.state.db.get_jetson_by_id(jetson_id)
    if not row:
        raise HTTPException(status_code=404, detail="해당 Jetson이 없습니다.")
    return row


@router.post("/admin", response_model=JetsonOut, status_code=201, summary="Jetson 신규 등록 (공간 매핑)")
def create_jetson(req: JetsonAdminCreateReq, request: Request):
    try:
        row = request.app.state.db.create_jetson(req.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not row:
        raise HTTPException(status_code=500, detail="Jetson 등록에 실패했습니다.")
    return row


@router.put("/{jetson_id}", response_model=JetsonOut, summary="Jetson 수정 (공간 변경 시 센서/카메라 동기화)")
def update_jetson(jetson_id: int, req: JetsonUpdateReq, request: Request):
    db = request.app.state.db
    if not db.get_jetson_by_id(jetson_id):
        raise HTTPException(status_code=404, detail="해당 Jetson이 없습니다.")
    try:
        row = db.update_jetson(jetson_id, req.model_dump(exclude_none=True))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not row:
        raise HTTPException(status_code=500, detail="Jetson 수정에 실패했습니다.")
    return row


@router.post("/register", response_model=JetsonRegisterRes, summary="Jetson 등록 및 앱 연동")
def register_jetson(req: JetsonRegisterReq, request: Request):
    db = request.app.state.db
    jetson = db.register_jetson_connection(req.dept_id, req.app_id)
    if not jetson:
        raise HTTPException(status_code=404, detail="DB에 Jetson 초기 정보가 없습니다.")
    logger.debug("Jetson app register ip_addr=%s", jetson["ip_addr"])

    return JetsonRegisterRes(
        jetson_id=f"jetson-{jetson['jetson_id']:02d}",
        register_status="success",
        api_base_url=f"http://{jetson['ip_addr']}:{jetson['port']}",
        ws_url=f"ws://{jetson['ip_addr']}:{jetson['port']}/ws/alerts",
    )
