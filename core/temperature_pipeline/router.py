from fastapi import APIRouter, HTTPException, Query, Request

from core.temperature_pipeline.runner import run_temperature_camera_vlm_once

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


@router.get("/sessions", summary="온도 연동 카메라 autoregressive VLM 세션 전체 조회")
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


@router.post(
    "/sensors/{sensor_id}/debug/run-once",
    summary="실제 DB/CCTV/YOLO/autoregressive VLM 1회 실행 디버그",
)
def run_temperature_camera_vlm_once_debug(
    sensor_id: str,
    request: Request,
    camera_sen_id: int | None = Query(default=None, description="특정 CCTV sen_id만 실행"),
    publish: bool = Query(default=False, description="true면 WebSocket 앱 알림까지 발행"),
    require_hot: bool = Query(default=False, description="true면 온도가 임계치 이상일 때만 실행"),
):
    try:
        result = run_temperature_camera_vlm_once(
            request.app.state.db,
            sensor_id,
            manager=_get_manager(request),
            camera_sen_id=camera_sen_id,
            publish=publish,
            require_hot=require_hot,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")
    return {"status": "success", "data": result}


@router.post("/sensors/{sensor_id}/stop", summary="온도 센서 기준 카메라 VLM 중지")
def stop_temperature_camera_vlm(sensor_id: str, request: Request):
    return {"status": "success", "data": _get_manager(request).stop_for_temperature_sensor(sensor_id)}
