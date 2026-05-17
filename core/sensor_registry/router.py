from fastapi import APIRouter, HTTPException, Request

from config import settings

from .schemas import SensorRegisterReq, SensorUnregisterReq, WorkerWatchRegisterReq

router = APIRouter(prefix="/api/sensors", tags=["sensor-registry"])


def _parse_jetson_id(value) -> int:
    try:
        if isinstance(value, str) and value.startswith("jetson-"):
            return int(value.split("-")[1])
        return int(value)
    except Exception:
        return 1


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
    jetson_id = _parse_jetson_id(req.jetson_id)

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
    watch_scheduler = getattr(request.app.state, "watch_scheduler", None)
    temperature_scheduler = getattr(request.app.state, "temperature_scheduler", None)
    for sensor in selected:
        mqtt_svc.publish_register(
            sensor_id=sensor["sensor_id"],
            site_id=f"jetson-{jetson_id:02d}",
            interval_ms=settings.sensor_register_interval_ms,
        )
        sensor_type = str(sensor.get("sensor_type") or "").lower()
        if sensor_type == "heart_band" and watch_scheduler is not None:
            watch_scheduler.register(sensor["sensor_id"])
        if sensor_type in {"temp_humidity", "temperature_humidity", "th", "temperature"}:
            if temperature_scheduler is not None:
                temperature_scheduler.register(sensor["sensor_id"])

    return {"status": "success", "message": f"{len(selected)}개 센서 등록 완료"}


@router.post("/register/worker-watch", summary="작업자 워치 등록 및 휴식 파이프라인 연결")
def register_worker_watch(req: WorkerWatchRegisterReq, request: Request):
    """앱 등록 화면에서 heart_band를 특정 작업자(dept_id)에 매핑한다.

    성공하면 worker.sen_id가 채워지고, 워치 휴식 파이프라인 스레드도 즉시 시작된다.
    """
    jetson_id = _parse_jetson_id(req.jetson_id)
    sensor_info = req.sensor.model_dump()
    sensor_type = str(sensor_info.get("sensor_type") or "heart_band").lower()
    if sensor_type in {"watch", "hb", "heart_rate", "heartbeat"}:
        sensor_type = "heart_band"
    sensor_info["sensor_type"] = sensor_type

    db = request.app.state.db
    try:
        result = db.register_sensor_with_worker(sensor_info, jetson_id, req.dept_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    mqtt_svc = request.app.state.mqtt_sensor_service
    mqtt_svc.publish_register(
        sensor_id=result["sensor_id"],
        site_id=f"jetson-{jetson_id:02d}",
        interval_ms=settings.sensor_register_interval_ms,
    )

    watch_scheduler = getattr(request.app.state, "watch_scheduler", None)
    if watch_scheduler is not None:
        watch_scheduler.register(result["sensor_id"])

    return {
        "status": "success",
        "message": "작업자 워치 등록 및 휴식 파이프라인 연결 완료",
        "data": result,
    }


@router.post("/unregister", summary="센서 등록 해제")
def unregister_sensor(req: SensorUnregisterReq, request: Request):
    mqtt_svc = request.app.state.mqtt_sensor_service
    mqtt_svc.publish_unregister(req.sensor_id)

    watch_scheduler = getattr(request.app.state, "watch_scheduler", None)
    if watch_scheduler is not None:
        watch_scheduler.unregister(req.sensor_id)

    temperature_scheduler = getattr(request.app.state, "temperature_scheduler", None)
    if temperature_scheduler is not None:
        temperature_scheduler.unregister(req.sensor_id)

    db = request.app.state.db
    if not db.unregister_sensor_by_sensor_id(req.sensor_id):
        raise HTTPException(status_code=404, detail="해당 sensor_id를 가진 등록 센서가 없습니다.")

    return {"status": "success", "message": "센서 등록 해제 완료"}
