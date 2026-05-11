from fastapi import APIRouter, HTTPException, Request


router = APIRouter(prefix="/api/watch-vlm", tags=["watch-vlm"])


def _get_manager(request: Request):
    manager = getattr(request.app.state, "watch_camera_vlm_manager", None)
    if manager is None:
        raise HTTPException(status_code=503, detail="watch camera VLM manager is not initialized")
    return manager


@router.get("/sessions", summary="워치 연동 카메라 VLM 세션 전체 조회")
def get_watch_camera_vlm_sessions(request: Request):
    manager = _get_manager(request)
    return {"status": "success", "data": manager.get_status()}


@router.get("/spaces/{space_id}/status", summary="공간별 워치 연동 카메라 VLM 상태 조회")
def get_watch_camera_vlm_space_status(space_id: int, request: Request):
    manager = _get_manager(request)
    return {"status": "success", "data": manager.get_space_status(space_id)}


@router.post("/watch/{sensor_id}/trigger", summary="워치 센서 기준 카메라 VLM 수동 트리거")
def trigger_watch_camera_vlm(sensor_id: str, request: Request):
    manager = _get_manager(request)
    result = manager.trigger_for_watch_sensor(sensor_id)
    return {"status": "success", "data": result}
