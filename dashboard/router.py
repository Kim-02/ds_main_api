from fastapi import APIRouter, HTTPException, Path, Query, Request

from .schemas import (
    DashboardCctvsResponse,
    DashboardSensorsResponse,
    DashboardSummaryResponse,
    DashboardWorkersResponse,
    RecentAlertsResponse,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummaryResponse, summary="현장 대시보드 요약")
def get_dashboard_summary(
    request: Request,
    space_id: int = Query(..., description="현재 선택된 Jetson의 space_id"),
):
    if space_id <= 0:
        raise HTTPException(status_code=400, detail="유효한 space_id를 입력해주세요.")

    data = request.app.state.db.get_dashboard_summary_by_space_id(space_id)

    if data is None:
        return {
            "status": "error",
            "data": {
                "space_id": space_id,
                "space_name": None,
                "jetson_id": None,
                "jetson_name": None,
                "danger_alert_count": 0,
                "sensor_total": 0,
                "cctv_total": 0,
                "worker_total": 0,
            },
        }

    return {"status": "success", "data": data}


@router.get("/recent-alerts", response_model=RecentAlertsResponse, summary="최근 알림 목록 (space_id 기준)")
def get_recent_alerts(
    request: Request,
    space_id: int = Query(..., description="space_id"),
    limit: int = Query(20, ge=1, le=100, description="최대 조회 건수"),
):
    if space_id <= 0:
        raise HTTPException(status_code=400, detail="유효한 space_id를 입력해주세요.")

    data = request.app.state.db.get_recent_alerts_by_space_id(space_id, limit)
    return {"status": "success", "data": data}


@router.get("/sensors", response_model=DashboardSensorsResponse, summary="센서 목록 (camera 계열 제외)")
def get_dashboard_sensors(
    request: Request,
    space_id: int = Query(..., description="space_id"),
):
    if space_id <= 0:
        raise HTTPException(status_code=400, detail="유효한 space_id를 입력해주세요.")

    data = request.app.state.db.get_dashboard_sensors_by_space_id(space_id)
    return {"status": "success", "data": data}


@router.get("/cctvs", response_model=DashboardCctvsResponse, summary="CCTV 목록")
def get_dashboard_cctvs(
    request: Request,
    space_id: int = Query(..., description="space_id"),
):
    if space_id <= 0:
        raise HTTPException(status_code=400, detail="유효한 space_id를 입력해주세요.")

    data = request.app.state.db.get_dashboard_cctvs_by_space_id(space_id)
    return {"status": "success", "data": data}


@router.patch(
    "/alerts/{event_id}/read",
    summary="알림 읽음 처리",
)
def mark_alert_as_read(
    request: Request,
    event_id: int = Path(..., description="alert_event.event_id"),
    space_id: int | None = Query(None, description="space_id (선택, 검증용)"),
):
    ok = request.app.state.db.mark_alert_as_read(event_id, space_id)
    if not ok:
        raise HTTPException(status_code=404, detail="알림을 찾을 수 없습니다.")
    return {"success": True, "message": "alert marked as read"}


@router.get("/workers", response_model=DashboardWorkersResponse, summary="작업자 목록 (센서 매핑 기준)")
def get_dashboard_workers(
    request: Request,
    space_id: int = Query(..., description="space_id"),
):
    if space_id <= 0:
        raise HTTPException(status_code=400, detail="유효한 space_id를 입력해주세요.")

    data = request.app.state.db.get_dashboard_workers_by_space_id(space_id)
    return {"status": "success", "data": data}

