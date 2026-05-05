from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database.models import AnomalyEvent, AnomalyStatus, Report, VLMQuery


async def get_anomaly_events(
    db: AsyncSession,
    process_id: int | None = None,
    status: AnomalyStatus | None = None,
) -> list[AnomalyEvent]:
    q = select(AnomalyEvent).order_by(AnomalyEvent.start_time.desc())
    if process_id is not None:
        q = q.where(AnomalyEvent.process_id == process_id)
    if status is not None:
        q = q.where(AnomalyEvent.status == status)
    result = await db.execute(q)
    return list(result.scalars().all())


async def get_anomaly_event_by_id(db: AsyncSession, event_id: int) -> AnomalyEvent | None:
    result = await db.execute(
        select(AnomalyEvent)
        .options(selectinload(AnomalyEvent.vlm_queries), selectinload(AnomalyEvent.report))
        .where(AnomalyEvent.id == event_id)
    )
    return result.scalar_one_or_none()


async def create_anomaly_event(db: AsyncSession, **kwargs) -> AnomalyEvent:
    event = AnomalyEvent(**kwargs)
    db.add(event)
    await db.flush()
    await db.refresh(event)
    return event


async def update_anomaly_event(db: AsyncSession, event: AnomalyEvent, **kwargs) -> AnomalyEvent:
    for key, value in kwargs.items():
        setattr(event, key, value)
    await db.flush()
    await db.refresh(event)
    return event


async def save_vlm_query(db: AsyncSession, **kwargs) -> VLMQuery:
    query = VLMQuery(**kwargs)
    db.add(query)
    await db.flush()
    await db.refresh(query)
    return query


async def get_vlm_queries(db: AsyncSession, event_id: int) -> list[VLMQuery]:
    result = await db.execute(
        select(VLMQuery)
        .where(VLMQuery.anomaly_event_id == event_id)
        .order_by(VLMQuery.queried_at)
    )
    return list(result.scalars().all())


async def get_reports(db: AsyncSession) -> list[Report]:
    result = await db.execute(
        select(Report)
        .options(selectinload(Report.anomaly_event))
        .order_by(Report.created_at.desc())
    )
    return list(result.scalars().all())


async def get_report_by_id(db: AsyncSession, report_id: int) -> Report | None:
    result = await db.execute(select(Report).where(Report.id == report_id))
    return result.scalar_one_or_none()


async def get_report_by_event(db: AsyncSession, event_id: int) -> Report | None:
    result = await db.execute(select(Report).where(Report.anomaly_event_id == event_id))
    return result.scalar_one_or_none()


async def create_report(db: AsyncSession, **kwargs) -> Report:
    report = Report(**kwargs)
    db.add(report)
    await db.flush()
    await db.refresh(report)
    return report


async def update_report(db: AsyncSession, report: Report, **kwargs) -> Report:
    for key, value in kwargs.items():
        setattr(report, key, value)
    await db.flush()
    await db.refresh(report)
    return report
