from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/temperature-vlm", tags=["temperature-vlm"])


def _get_scheduler(request: Request):
    scheduler = getattr(request.app.state, "temperature_scheduler", None)
    if scheduler is None:
        raise HTTPException(status_code=503, detail="temperature pipeline scheduler is not initialized")
    return scheduler


def _get_manager(request: Request):
    manager = getattr(request.app.state, "temperature_camera_vlm_manager", None)
    if manager is None:
        raise HTTPException(status_code=503, detail="temperature camera VLM manager is not initialized")
    return manager


@router.get("/scheduler", summary="온도 VLM 트리거 스케줄러 상태 조회")
def get_temperature_scheduler_status(request: Request):
    return {"status": "success", "data": _get_scheduler(request).get_status()}


@router.get("/sessions", summary="온도 연동 카메라 VLM 세션 전체 조회")
def get_temperature_camera_vlm_sessions(request: Request):
    return {"status": "success", "data": _get_manager(request).get_status()}


@router.get("/spaces/{space_id}/status", summary="공간별 온도 연동 카메라 VLM 상태 조회")
def get_temperature_camera_vlm_space_status(space_id: int, request: Request):
    return {"status": "success", "data": _get_manager(request).get_space_status(space_id)}


@router.post("/sensors/{sensor_id}/trigger", summary="온도 센서 기준 카메라 VLM 수동 트리거")
def trigger_temperature_camera_vlm(sensor_id: str, request: Request):
    db = request.app.state.db
    sample = db.get_latest_th_by_sensor_id(sensor_id)
    if not sample:
        raise HTTPException(status_code=404, detail="temperature sensor or sample not found")
    result = _get_manager(request).trigger_for_temperature_sensor(sensor_id, sample)
    return {"status": "success", "data": result}


@router.post("/sensors/{sensor_id}/stop", summary="온도 센서 기준 카메라 VLM 중지")
def stop_temperature_camera_vlm(sensor_id: str, request: Request):
    return {"status": "success", "data": _get_manager(request).stop_for_temperature_sensor(sensor_id)}
