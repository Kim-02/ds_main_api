from fastapi import APIRouter, HTTPException, Request

from .schemas import SensorRegisterReq, SensorUnregisterReq

router = APIRouter(prefix="/api/sensors", tags=["sensor-registry"])


@router.get("/discovered", summary="mDNS 발견 센서 목록 조회")
def get_discovered_sensors(request: Request):
    try:
        sensors = request.app.state.mdns_service.get_discovered_sensors()
        return {"status": "success", "data": sensors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", summary="등록된 센서 목록 조회")
def get_sensors(request: Request):
    try:
        return {"status": "success", "data": request.app.state.db.get_registered_sensor_rows()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/register", summary="센서 다중 등록 (mDNS 발견 → DB + MQTT)")
def register_sensors(req: SensorRegisterReq, request: Request):
    try:
        jetson_id = int(req.jetson_id.split("-")[1])
    except Exception:
        jetson_id = 1

    if not req.selected_sensors:
        raise HTTPException(status_code=400, detail="선택된 센서가 없습니다.")

    selected = []
    for s in req.selected_sensors:
        d = s.model_dump()
        if not d.get("sensor_id"):
            raise HTTPException(status_code=400, detail="sensor_id가 없는 센서가 포함되어 있습니다.")
        selected.append(d)

    db = request.app.state.db
    if not db.register_discovered_sensors(jetson_id, selected):
        raise HTTPException(status_code=500, detail="센서 DB 등록 실패")

    mqtt_svc = request.app.state.mqtt_sensor_service
    for sensor in selected:
        mqtt_svc.publish_register(
            sensor_id=sensor["sensor_id"],
            site_id=f"jetson-{jetson_id:02d}",
            interval_ms=5000,
        )

    return {"status": "success", "message": f"{len(selected)}개 센서 등록 완료"}


@router.post("/unregister", summary="센서 등록 해제")
def unregister_sensor(req: SensorUnregisterReq, request: Request):
    mqtt_svc = request.app.state.mqtt_sensor_service
    mqtt_svc.publish_unregister(req.sensor_id)

    db = request.app.state.db
    if not db.unregister_sensor_by_sensor_id(req.sensor_id):
        raise HTTPException(status_code=404, detail="해당 sensor_id를 가진 등록 센서가 없습니다.")

    return {"status": "success", "message": "센서 등록 해제 완료"}
