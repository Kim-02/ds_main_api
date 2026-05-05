from typing import Annotated, Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import AnomalyStatus
from database.session import get_db

from . import service
from .schemas import (
    AnomalyEventDetailOut,
    AnomalyEventOut,
    ReportCreate,
    ReportOut,
    ReportUpdate,
    StopDetectionRequest,
    VLMQueryOut,
)

DB = Annotated[AsyncSession, Depends(get_db)]

events_router = APIRouter(prefix="/anomaly-events", tags=["anomaly-events"])


@events_router.get("/", response_model=list[AnomalyEventOut])
async def list_anomaly_events(
    db: DB,
    process_id: Optional[int] = Query(None),
    event_status: Optional[AnomalyStatus] = Query(None),
):
    return await service.list_anomaly_events(db, process_id=process_id, event_status=event_status)


@events_router.get("/{event_id}", response_model=AnomalyEventDetailOut)
async def get_anomaly_event(event_id: int, db: DB):
    return await service.get_anomaly_event(db, event_id)


@events_router.post("/{event_id}/stop", response_model=AnomalyEventOut)
async def stop_detection(event_id: int, req: StopDetectionRequest, db: DB):
    return await service.stop_detection(db, event_id, req)


@events_router.get("/{event_id}/vlm-queries", response_model=list[VLMQueryOut])
async def get_vlm_queries(event_id: int, db: DB):
    from database.crud import report as crud
    return await crud.get_vlm_queries(db, event_id)


reports_router = APIRouter(prefix="/reports", tags=["reports"])


@reports_router.get("/", response_model=list[ReportOut])
async def list_reports(db: DB):
    return await service.list_reports(db)


@reports_router.post("/", response_model=ReportOut, status_code=status.HTTP_201_CREATED)
async def create_report(data: ReportCreate, db: DB):
    return await service.create_report(db, data)


@reports_router.get("/{report_id}", response_model=ReportOut)
async def get_report(report_id: int, db: DB):
    return await service.get_report(db, report_id)


@reports_router.put("/{report_id}", response_model=ReportOut)
async def update_report(report_id: int, data: ReportUpdate, db: DB):
    return await service.update_report(db, report_id, data)


# ------------------------------------------------------------------
# 기존 API 호환 엔드포인트
# ------------------------------------------------------------------

class EventMeasuresReq(BaseModel):
    event_id: int
    measures: str


class VlmAnalysisReq(BaseModel):
    ip_address: str
    ev_code_name: str
    message: str
    time: Optional[Any] = None


internal_router = APIRouter(prefix="/api", tags=["internal"])


@internal_router.post("/event/measures", summary="사건 조치 사항 기록")
async def post_event_measures(req: EventMeasuresReq, request: Request):
    db = request.app.state.db
    success = db.update_event_measures(req.event_id, req.measures)
    if not success:
        raise HTTPException(status_code=400, detail="조치 사항 기록 실패. event_id를 확인하세요.")
    return {"status": "success", "message": "조치 사항이 성공적으로 기록되었습니다."}


@internal_router.post("/internal/vlm-analysis", summary="VLM 분석 결과 수신 → 안전 감지 모듈 전달")
async def receive_vlm_analysis(req: VlmAnalysisReq, request: Request, background_tasks: BackgroundTasks):
    safety_core = getattr(request.app.state, "safety_core", None)
    if safety_core:
        background_tasks.add_task(safety_core.receive_ai_event, req.model_dump())
    return Response(status_code=status.HTTP_200_OK)
