import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from .schemas import SensorPositionSaveReq

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/maps", tags=["map"])

# ──────────────────────────────────────────────────────────────
# 정적 prefix 라우트를 동적 라우트보다 위에 배치해야 한다.
# /space/{id}, /sensors/... 가 /{jetson_id} 보다 먼저 선언.
# ──────────────────────────────────────────────────────────────


@router.get("/space/{space_id}", summary="공간 기준 평면도 조회 (space_id)")
def get_floor_map_by_space(space_id: int, request: Request):
    """space_id 기준 가장 최근 평면도를 반환합니다.

    floor_map.space_id 가 NULL 이면 jetson.space_id 를 통해 fallback 조회합니다.
    """
    logger.info("[MAP] GET floor_map space_id=%s", space_id)
    floor_map = request.app.state.db.get_floor_map_by_space_id(space_id)
    if not floor_map:
        logger.warning("[MAP] floor_map not found space_id=%s", space_id)
        raise HTTPException(status_code=404, detail="해당 공간의 평면도가 없습니다.")
    logger.info(
        "[MAP] floor_map found space_id=%s map_id=%s map_space_id=%s",
        space_id, floor_map.get("map_id"), floor_map.get("space_id"),
    )
    return {"status": "success", "data": floor_map}


@router.get("/space/{space_id}/available-cctvs", summary="공간 기준 배치 가능한 CCTV 조회")
def get_available_cctvs_by_space(
    space_id: int,
    request: Request,
    map_id: Optional[int] = Query(None, description="배치 여부(placed)를 확인할 map_id"),
):
    """space_id에 속한 CCTV 목록을 반환합니다. demo_camera 포함.

    map_id를 함께 전달하면 각 CCTV의 배치 여부(placed)와 현재 좌표도 반환합니다.
    """
    logger.info("[MAP_CCTV] available-cctvs space_id=%s map_id=%s", space_id, map_id)
    cctvs = request.app.state.db.get_available_cctvs_for_map(space_id, map_id=map_id)
    logger.info(
        "[MAP_CCTV] available-cctvs result space_id=%s map_id=%s count=%s items=%s",
        space_id, map_id, len(cctvs) if cctvs else 0, cctvs,
    )
    logger.info(cctvs)
    return {"status": "success", "data": cctvs}


@router.get("/space/{space_id}/available-sensors", summary="공간 기준 배치 가능한 온습도 센서 조회")
def get_available_sensors_by_space(
    space_id: int,
    request: Request,
    map_id: Optional[int] = Query(None, description="배치 여부(placed)를 확인할 map_id"),
):
    """space_id에 속한 온습도 센서 목록을 반환합니다.

    map_id를 함께 전달하면 각 센서의 배치 여부(placed)와 현재 좌표도 반환합니다.
    """
    sensors = request.app.state.db.get_registered_sensors_by_space_id(
        space_id, map_id=map_id
    )
    return {"status": "success", "data": sensors}


@router.get("/sensors/{sensor_id}/latest", summary="온습도 센서 최신값 조회")
def get_latest_temp_sensor_value(sensor_id: str, request: Request):
    data = request.app.state.db.get_latest_th_by_sensor_id(sensor_id)
    return {"status": "success", "data": data}


@router.post("/sensors/position", summary="센서 위치 저장")
def save_sensor_position(req: SensorPositionSaveReq, request: Request):
    """평면도 위에 센서 위치(비율 좌표)를 저장합니다.

    저장 전 sensor.space_id와 floor_map.space_id가 일치하는지 검증합니다.
    """
    if not (0.0 <= req.x_ratio <= 1.0 and 0.0 <= req.y_ratio <= 1.0):
        raise HTTPException(
            status_code=400,
            detail="x_ratio, y_ratio는 0.0 ~ 1.0 범위의 비율값이어야 합니다.",
        )

    floor_map = request.app.state.db.get_floor_map_by_map_id(req.map_id)
    if not floor_map:
        raise HTTPException(status_code=404, detail=f"map_id={req.map_id} 평면도가 없습니다.")

    sensor = request.app.state.db.get_sensor_by_sensor_id(req.sensor_id)
    if not sensor:
        raise HTTPException(
            status_code=404,
            detail=f"sensor_id='{req.sensor_id}' 센서가 등록되어 있지 않습니다.",
        )

    # space_id 일치 검증
    error_msg = request.app.state.db.validate_sensor_map_space(req.map_id, req.sensor_id)
    if error_msg:
        raise HTTPException(status_code=400, detail=error_msg)

    ok = request.app.state.db.upsert_sensor_position(
        map_id=req.map_id,
        sensor_id=req.sensor_id,
        x_ratio=req.x_ratio,
        y_ratio=req.y_ratio,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="센서 위치 저장에 실패했습니다.")

    return {"status": "success", "message": "센서 위치 저장 완료"}


@router.get("/{map_id}/sensors", summary="평면도 센서 위치 조회 (map_id 기준)")
def get_map_sensor_positions(map_id: int, request: Request):
    """map_id 기준 배치된 센서 위치 목록을 반환합니다. is_demo, demo_video_key 포함."""
    logger.info("[MAP] GET sensor_positions map_id=%s", map_id)
    floor_map = request.app.state.db.get_floor_map_by_map_id(map_id)
    if not floor_map:
        raise HTTPException(status_code=404, detail="해당 map_id의 평면도가 없습니다.")

    positions = request.app.state.db.get_sensor_positions_by_map_id(map_id)

    # is_demo=True인 경우 stream_url 에 demo 식별자 주입
    for pos in positions:
        is_demo = bool(pos.get("is_demo"))
        demo_key = pos.get("demo_video_key") or ""
        pos["is_camera"] = bool(
            pos.get("sensor_type") and (
                "camera" in str(pos["sensor_type"]).lower()
                or "cctv" in str(pos["sensor_type"]).lower()
            )
        )
        pos["is_demo"] = is_demo
        if is_demo:
            pos["stream_url"] = f"demo://{demo_key}"
        else:
            pos["stream_url"] = None

    logger.info("[MAP] sensor_positions map_id=%s count=%s", map_id, len(positions))
    return {"status": "success", "data": positions}


@router.get("/{jetson_id}/available-sensors", summary="[Deprecated] jetson_id 기준 센서 조회")
def get_available_sensors_legacy(
    jetson_id: int,
    request: Request,
    map_id: Optional[int] = Query(None),
):
    """Deprecated: GET /api/maps/space/{space_id}/available-sensors 를 사용하세요."""
    sensors = request.app.state.db.get_registered_sensors_by_jetson_id(
        jetson_id, map_id=map_id
    )
    return {"status": "success", "data": sensors}


@router.get("/{jetson_id}", summary="[Deprecated] jetson_id 기준 평면도 조회")
def get_floor_map_legacy(jetson_id: int, request: Request):
    """Deprecated: GET /api/maps/space/{space_id} 를 사용하세요."""
    floor_map = request.app.state.db.get_floor_map_by_jetson_id(jetson_id)
    if not floor_map:
        raise HTTPException(status_code=404, detail="해당 Jetson의 평면도가 없습니다.")
    return {"status": "success", "data": floor_map}

