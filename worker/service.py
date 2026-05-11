from datetime import datetime

from fastapi import HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool

from .schemas import WorkerCreate, WorkerUpdate


def _row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    dept_id = row.get("dept_id")
    return {
        "id": dept_id,
        "employee_id": str(dept_id),
        "name": row.get("name", ""),
        "process_id": None,
        "health_baseline": None,
        "is_active": True,
        "created_at": datetime.now(),
    }


# ── SQLAlchemy 대체 worker CRUD ───────────────────────────────────────────────

async def list_workers(db, process_id: int | None = None) -> list[dict]:
    rows = await run_in_threadpool(db.get_workers, None)
    return [_row_to_out(r) for r in rows]


async def get_worker(db, worker_id: int) -> dict:
    row = await run_in_threadpool(db.get_worker_by_dept_id, worker_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Worker not found")
    return _row_to_out(row)


async def create_worker(db, data: WorkerCreate) -> dict:
    try:
        dept_id = int(data.employee_id)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="employee_id는 정수(사번)여야 합니다.",
        )
    existing = await run_in_threadpool(db.get_worker_by_dept_id, dept_id)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="employee_id already exists")
    row = await run_in_threadpool(db.create_worker, dept_id, data.name)
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="생성 실패")
    return _row_to_out(row)


async def update_worker(db, worker_id: int, data: WorkerUpdate) -> dict:
    await get_worker(db, worker_id)
    kwargs = data.model_dump(exclude_none=True)
    row = await run_in_threadpool(db.update_worker, worker_id, **kwargs)
    return _row_to_out(row)


async def delete_worker(db, worker_id: int) -> None:
    await get_worker(db, worker_id)
    await run_in_threadpool(db.delete_worker, worker_id)


# ── 기존 MariaDB 기반 worker 조회 (유지) ─────────────────────────────────────

async def list_db_workers(request: Request, is_manager: int | None = None) -> list[dict]:
    return await run_in_threadpool(request.app.state.db.get_workers, is_manager)


async def get_db_worker(request: Request, dept_id: int) -> dict:
    worker = await run_in_threadpool(request.app.state.db.get_worker_by_dept_id, dept_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="해당 사번의 작업자를 찾을 수 없습니다.",
        )
    return worker


async def assign_heart_band_to_worker(
    request: Request,
    dept_id: int,
    sensor_id: str,
    jetson_id: int | None,
    interval_ms: int,
) -> dict:
    db_handler = request.app.state.db
    mdns_service = getattr(request.app.state, "mdns_sensor_service", None)
    mqtt_service = getattr(request.app.state, "mqtt_sensor_service", None)

    if mdns_service is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="mDNS 센서 서비스가 app.state에 등록되어 있지 않습니다.",
        )
    if mqtt_service is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MQTT 센서 서비스가 app.state에 등록되어 있지 않습니다.",
        )

    sensor_info = mdns_service.discovered_sensors.get(sensor_id)
    if not sensor_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="mDNS로 발견된 센서를 찾을 수 없습니다.",
        )
    if sensor_info.get("sensor_type") != "heart_band":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="작업자 매핑은 heart_band 센서에만 허용됩니다.",
        )

    if jetson_id is None:
        jetson = await run_in_threadpool(db_handler.get_first_jetson)
        if not jetson:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="등록된 Jetson 정보가 없습니다.",
            )
        jetson_id = jetson["jetson_id"]

    try:
        result = await run_in_threadpool(
            db_handler.register_sensor_with_worker, sensor_info, jetson_id, dept_id
        )
        await run_in_threadpool(mqtt_service.publish_register, sensor_id, str(jetson_id), interval_ms)

        watch_scheduler = getattr(request.app.state, "watch_scheduler", None)
        if watch_scheduler is not None:
            watch_scheduler.register(sensor_id)

        return {"success": True, "message": "워치 등록 및 작업자 매핑이 완료되었습니다.", "data": result}

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"워치 매핑 처리 중 오류가 발생했습니다: {e}",
        )


async def unassign_worker_sensor(request: Request, dept_id: int) -> dict:
    db_handler = request.app.state.db
    worker = await run_in_threadpool(db_handler.get_worker_by_dept_id, dept_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="해당 사번의 작업자를 찾을 수 없습니다.",
        )

    sensor_id = worker.get("sensor_id")
    ok = await run_in_threadpool(db_handler.unassign_worker_sensor, dept_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="센서 매핑 해제에 실패했습니다.")

    mqtt_service = getattr(request.app.state, "mqtt_sensor_service", None)
    if mqtt_service is not None and sensor_id:
        await run_in_threadpool(mqtt_service.publish_unregister, sensor_id)

    watch_scheduler = getattr(request.app.state, "watch_scheduler", None)
    if watch_scheduler is not None and sensor_id:
        watch_scheduler.unregister(sensor_id)

    return {"success": True, "message": "작업자 센서 매핑이 해제되었습니다."}
