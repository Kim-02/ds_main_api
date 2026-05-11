"""영상 파일을 카메라 대신 입력으로 사용하는 시나리오 API.

실제 fire_pipeline 로직은 그대로 사용하며, RTSP 대신 로컬 영상 파일을 소스로 넘긴다.
manager.start_pipeline()이 rtsp_url 자리에 파일 경로를 받아도 동일하게 동작한다.
(VideoSource.is_live_source() 가 False → 파일 모드로 읽음)
"""
import threading
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

router = APIRouter(prefix="/cctv/scenarios", tags=["cctv-scenario"])

# ─── 시나리오 레지스트리 ──────────────────────────────────────────────────────
# scenario_id(str) → 메타데이터
_scenarios: dict[str, dict] = {}
_lock = threading.Lock()

# 시나리오용 가상 camera_id — 실제 카메라와 충돌하지 않도록 음수 대역 사용
_SCENARIO_ID_BASE = -10000
_scenario_counter = 0


def _next_camera_id() -> int:
    global _scenario_counter
    _scenario_counter += 1
    return _SCENARIO_ID_BASE - _scenario_counter


# ─── 스키마 ───────────────────────────────────────────────────────────────────

class ScenarioStartRequest(BaseModel):
    video_name: str          # 파일명 또는 절대경로 (예: "fire_newnew.mp4")
    space_name: str          # 작업장 이름 (메타데이터용)
    model_path: str = ""     # 비워두면 settings 기본값 사용


class ScenarioOut(BaseModel):
    scenario_id: str
    video_name: str
    space_name: str
    camera_id: int
    status: str              # running | stopped | failed
    started_at: str
    latest_result: str
    latest_error: str


# ─── 헬퍼 ────────────────────────────────────────────────────────────────────

def _to_out(scenario_id: str, meta: dict) -> dict:
    from cctv.fire_pipeline import manager
    pipeline_status = manager.get_status(meta["camera_id"])
    return {
        "scenario_id": scenario_id,
        "video_name": meta["video_name"],
        "space_name": meta["space_name"],
        "camera_id": meta["camera_id"],
        "status": "running" if pipeline_status["running"] else "stopped",
        "started_at": meta["started_at"],
        "latest_result": pipeline_status.get("latest_result", ""),
        "latest_error": pipeline_status.get("latest_error", ""),
    }


# ─── 엔드포인트 ──────────────────────────────────────────────────────────────

@router.post(
    "/",
    response_model=ScenarioOut,
    status_code=status.HTTP_201_CREATED,
    summary="영상 파일로 화재 감지 시나리오 실행",
    description=(
        "지정한 영상 파일을 카메라 대신 입력으로 사용해 fire pipeline + VLM 분석을 실행한다.\n"
        "로직은 실제 카메라와 동일하며, 분석 결과는 WebSocket /ws/alerts 로 브로드캐스트된다."
    ),
)
def start_scenario(body: ScenarioStartRequest, request: Request):
    from cctv.fire_pipeline import manager

    camera_id = _next_camera_id()
    scenario_id = str(uuid.uuid4())

    ok = manager.start_pipeline(
        camera_id=camera_id,
        rtsp_url=body.video_name,      # 파일 경로를 그대로 video_source로 사용
        model_path=body.model_path,
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="파이프라인 시작 실패 — fire_pipeline 모듈 또는 영상 파일을 확인하세요.",
        )

    meta = {
        "video_name": body.video_name,
        "space_name": body.space_name,
        "camera_id": camera_id,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    with _lock:
        _scenarios[scenario_id] = meta

    return _to_out(scenario_id, meta)


@router.get(
    "/",
    response_model=list[ScenarioOut],
    summary="실행 중인 시나리오 목록",
)
def list_scenarios():
    with _lock:
        items = list(_scenarios.items())
    return [_to_out(sid, meta) for sid, meta in items]


@router.get(
    "/{scenario_id}",
    response_model=ScenarioOut,
    summary="시나리오 상태 및 최신 분석 결과 조회",
)
def get_scenario(scenario_id: str):
    with _lock:
        meta = _scenarios.get(scenario_id)
    if meta is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="시나리오를 찾을 수 없습니다.")
    return _to_out(scenario_id, meta)


@router.delete(
    "/{scenario_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="시나리오 중단",
)
def stop_scenario(scenario_id: str):
    with _lock:
        meta = _scenarios.pop(scenario_id, None)
    if meta is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="시나리오를 찾을 수 없습니다.")

    from cctv.fire_pipeline import manager
    manager.stop_pipeline(meta["camera_id"])
