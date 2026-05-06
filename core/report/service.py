from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from database.crud import report as crud
from database.models import AnomalyEvent, AnomalyStatus, Report

from .schemas import ReportCreate, ReportUpdate, StopDetectionRequest


async def list_anomaly_events(
    db: AsyncSession,
    process_id: int | None = None,
    event_status: AnomalyStatus | None = None,
) -> list[AnomalyEvent]:
    return await crud.get_anomaly_events(db, process_id=process_id, status=event_status)


async def get_anomaly_event(db: AsyncSession, event_id: int) -> AnomalyEvent:
    event = await crud.get_anomaly_event_by_id(db, event_id)
    if not event:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anomaly event not found")
    return event


async def stop_detection(db: AsyncSession, event_id: int, req: StopDetectionRequest) -> AnomalyEvent:
    event = await get_anomaly_event(db, event_id)
    if event.status != AnomalyStatus.active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Event is not active")

    return await crud.update_anomaly_event(
        db, event, status=AnomalyStatus.stopped, end_time=datetime.now(timezone.utc)
    )


async def list_reports(db: AsyncSession) -> list[Report]:
    return await crud.get_reports(db)


async def get_report(db: AsyncSession, report_id: int) -> Report:
    r = await crud.get_report_by_id(db, report_id)
    if not r:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")
    return r


async def create_report(db: AsyncSession, data: ReportCreate) -> Report:
    await get_anomaly_event(db, data.anomaly_event_id)
    existing = await crud.get_report_by_event(db, data.anomaly_event_id)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Report already exists")
    return await crud.create_report(db, **data.model_dump())


async def update_report(db: AsyncSession, report_id: int, data: ReportUpdate) -> Report:
    r = await get_report(db, report_id)
    return await crud.update_report(db, r, **data.model_dump(exclude_none=True))
