from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import worker as crud
from database.models import Worker

from .schemas import WorkerCreate, WorkerUpdate

from fastapi import HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool


async def list_workers(db: AsyncSession, process_id: int | None = None) -> list[Worker]:
    return await crud.get_all(db, process_id=process_id)


async def get_worker(db: AsyncSession, worker_id: int) -> Worker:
    w = await crud.get_by_id(db, worker_id)
    if not w:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Worker not found")
    return w


async def create_worker(db: AsyncSession, data: WorkerCreate) -> Worker:
    if await crud.get_by_employee_id(db, data.employee_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="employee_id already exists")
    return await crud.create(db, **data.model_dump())


async def update_worker(db: AsyncSession, worker_id: int, data: WorkerUpdate) -> Worker:
    w = await get_worker(db, worker_id)
    return await crud.update(db, w, **data.model_dump(exclude_none=True))


async def delete_worker(db: AsyncSession, worker_id: int) -> None:
    w = await get_worker(db, worker_id)
    await crud.delete(db, w)



#아래부터 추가
async def list_db_workers(request: Request, is_manager: int | None = None) -> list[dict]:
    db_handler = request.app.state.db

    return await run_in_threadpool(
        db_handler.get_workers,
        is_manager,
    )


async def get_db_worker(request: Request, dept_id: int) -> dict:
    db_handler = request.app.state.db

    worker = await run_in_threadpool(
        db_handler.get_worker_by_dept_id,
        dept_id,
    )

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
            db_handler.register_sensor_with_worker,
            sensor_info,
            jetson_id,
            dept_id,
        )

        await run_in_threadpool(
            mqtt_service.publish_register,
            sensor_id,
            str(jetson_id),
            interval_ms,
        )

        return {
            "success": True,
            "message": "워치 등록 및 작업자 매핑이 완료되었습니다.",
            "data": result,
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"워치 매핑 처리 중 오류가 발생했습니다: {e}",
        )


async def unassign_worker_sensor(request: Request, dept_id: int) -> dict:
    db_handler = request.app.state.db

    worker = await run_in_threadpool(
        db_handler.get_worker_by_dept_id,
        dept_id,
    )

    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="해당 사번의 작업자를 찾을 수 없습니다.",
        )

    sensor_id = worker.get("sensor_id")

    ok = await run_in_threadpool(
        db_handler.unassign_worker_sensor,
        dept_id,
    )

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="센서 매핑 해제에 실패했습니다.",
        )

    mqtt_service = getattr(request.app.state, "mqtt_sensor_service", None)

    if mqtt_service is not None and sensor_id:
        await run_in_threadpool(
            mqtt_service.publish_unregister,
            sensor_id,
        )

    return {
        "success": True,
        "message": "작업자 센서 매핑이 해제되었습니다.",
    }
