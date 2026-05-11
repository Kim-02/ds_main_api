from datetime import datetime, timezone

from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from .schemas import ReportCreate, ReportUpdate, StopDetectionRequest


def _event_row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    sen_id = row.get("sen_id") or 0
    # space_id는 sensor 테이블에 있으므로 event row에 없을 때 sen_id를 process_id로 대체
    space_id = row.get("space_id") or sen_id
    return {
        "id": row["event_id"],
        "anomaly_type": row.get("anomaly_type") or "unknown",
        "process_id": space_id,
        "sensor_id": sen_id,
        "triggered_value": _to_float(row.get("detected_value")),
        "start_time": row.get("start_time") or row.get("time"),
        "end_time": row.get("end_time"),
        "status": row.get("status") or "active",
    }


def _report_row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": row["report_id"],
        "anomaly_event_id": row["event_id"],
        "content": row["content"],
        "action_taken": row.get("action_taken"),
        "created_by": row.get("created_by"),
        "created_at": row.get("created_at") or datetime.now(),
    }


def _vlm_row_to_out(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": row["query_id"],
        "anomaly_event_id": row["event_id"],
        "queried_at": row.get("queried_at") or datetime.now(),
        "prompt": row.get("prompt") or "",
        "response": row.get("response"),
        "frame_path": row.get("frame_path"),
    }


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def list_anomaly_events(db, process_id: int | None = None, event_status=None) -> list[dict]:
    status_str = event_status.value if hasattr(event_status, "value") else event_status
    rows = await run_in_threadpool(db.get_events, process_id, status_str)
    return [_event_row_to_out(r) for r in rows]


async def get_anomaly_event(db, event_id: int) -> dict:
    row = await run_in_threadpool(db.get_event_by_id, event_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anomaly event not found")
    out = _event_row_to_out(row)

    vlm_rows = await run_in_threadpool(db.get_vlm_queries_by_event, event_id)
    out["vlm_queries"] = [_vlm_row_to_out(r) for r in vlm_rows]

    report_row = await run_in_threadpool(db.get_report_by_event_id, event_id)
    out["report"] = _report_row_to_out(report_row)

    return out


async def stop_detection(db, event_id: int, req: StopDetectionRequest) -> dict:
    row = await run_in_threadpool(db.get_event_by_id, event_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anomaly event not found")
    if (row.get("status") or "active") != "active":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Event is not active")

    updated = await run_in_threadpool(
        db.update_event,
        event_id,
        status="stopped",
        end_time=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    return _event_row_to_out(updated)


async def list_reports(db) -> list[dict]:
    rows = await run_in_threadpool(db.get_all_reports)
    return [_report_row_to_out(r) for r in rows]


async def get_report(db, report_id: int) -> dict:
    row = await run_in_threadpool(db.get_report_by_id, report_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")
    return _report_row_to_out(row)


async def create_report(db, data: ReportCreate) -> dict:
    event_row = await run_in_threadpool(db.get_event_by_id, data.anomaly_event_id)
    if not event_row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Anomaly event not found")

    existing = await run_in_threadpool(db.get_report_by_event_id, data.anomaly_event_id)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Report already exists")

    row = await run_in_threadpool(
        db.create_report,
        data.anomaly_event_id,
        data.content,
        data.action_taken,
        data.created_by,
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="생성 실패")
    return _report_row_to_out(row)


async def update_report(db, report_id: int, data: ReportUpdate) -> dict:
    await get_report(db, report_id)
    kwargs = data.model_dump(exclude_none=True)
    row = await run_in_threadpool(db.update_report, report_id, **kwargs)
    return _report_row_to_out(row)
