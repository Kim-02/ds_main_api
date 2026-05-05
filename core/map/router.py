from fastapi import APIRouter, HTTPException, Request

from .schemas import SensorPositionSaveReq

router = APIRouter(prefix="/api/maps", tags=["map"])


@router.get("/{jetson_id}", summary="Jetson 플로어 맵 조회")
def get_floor_map(jetson_id: int, request: Request):
    floor_map = request.app.state.db.get_floor_map_by_jetson_id(jetson_id)
    if not floor_map:
        raise HTTPException(status_code=404, detail="해당 Jetson의 플로어 맵이 없습니다.")
    return {"status": "success", "data": floor_map}


@router.get("/{map_id}/sensors", summary="플로어 맵 센서 위치 조회")
def get_map_sensor_positions(map_id: int, request: Request):
    positions = request.app.state.db.get_sensor_positions_by_map_id(map_id)
    return {"status": "success", "data": positions}


@router.get("/{jetson_id}/available-sensors", summary="배치 가능한 등록 센서 조회")
def get_available_sensors(jetson_id: int, request: Request):
    sensors = request.app.state.db.get_registered_sensors_by_jetson_id(jetson_id)
    return {"status": "success", "data": sensors}


@router.post("/sensors/position", summary="센서 위치 저장")
def save_sensor_position(req: SensorPositionSaveReq, request: Request):
    if not (0.0 <= req.x_ratio <= 1.0 and 0.0 <= req.y_ratio <= 1.0):
        raise HTTPException(status_code=400, detail="좌표는 0.0~1.0 범위의 비율값이어야 합니다.")

    ok = request.app.state.db.upsert_sensor_position(
        map_id=req.map_id, sensor_id=req.sensor_id, x_ratio=req.x_ratio, y_ratio=req.y_ratio
    )
    if not ok:
        raise HTTPException(status_code=500, detail="센서 위치 저장 실패")
    return {"status": "success", "message": "센서 위치 저장 완료"}
