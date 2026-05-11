"""Fire pipeline 카메라별 인스턴스 관리.

카메라 등록 시 start_pipeline(), 삭제 시 stop_pipeline() 호출.
pipeline.running = False 로 스레드를 graceful shutdown 한다.
"""
import asyncio
import logging
import threading
from typing import Callable, Optional

from core.notifications import make_vlm_push_payload

logger = logging.getLogger(__name__)

# camera_id(int) → {"thread": Thread, "pipeline": ExportFinalPipeline, "latest_result": str}
_pipelines: dict[int, dict] = {}
_pipelines_lock = threading.Lock()

# FastAPI 이벤트 루프 및 WebSocket 브로드캐스트 함수 (main.py lifespan에서 주입)
_loop: Optional[asyncio.AbstractEventLoop] = None
_broadcast_fn: Optional[Callable] = None


def init_manager(loop: asyncio.AbstractEventLoop, broadcast_fn: Callable) -> None:
    """main.py lifespan에서 호출 — WebSocket 브로드캐스트 함수와 이벤트 루프를 주입."""
    global _loop, _broadcast_fn
    _loop = loop
    _broadcast_fn = broadcast_fn
    logger.info("FirePipelineManager 초기화 완료")


def _make_on_result(camera_id: int) -> Callable[[str], None]:
    """파이프라인 결과를 메모리에 저장하고 WebSocket으로 전송하는 콜백 생성."""
    def on_result(answer: str) -> None:
        with _pipelines_lock:
            entry = _pipelines.get(camera_id)
            if entry is not None:
                entry["latest_result"] = answer

        if _loop is not None and _broadcast_fn is not None:
            asyncio.run_coroutine_threadsafe(
                _broadcast_fn(make_vlm_push_payload(
                    "fire_pipeline",
                    "CCTV 화재/연기 VLM 분석 완료",
                    answer,
                    camera_id=camera_id,
                )),
                _loop,
            )

    return on_result


def start_pipeline(camera_id: int, rtsp_url: str, model_path: str = "") -> bool:
    """카메라 ID에 대한 fire pipeline 시작.

    이미 실행 중이면 False 반환.
    rtsp_url: 카메라 등록 시 생성된 RTSP 주소
    model_path: 화재 감지 YOLO 모델 경로 (비어 있으면 config 기본값 사용)
    """
    with _pipelines_lock:
        if camera_id in _pipelines:
            logger.warning("Fire pipeline already running for camera %s", camera_id)
            return False
        _pipelines[camera_id] = {
            "thread": None,
            "pipeline": None,
            "latest_result": "",
            "starting": True,
        }

    try:
        from cctv.fire_pipeline.config import FirePipelineConfig
        from cctv.fire_pipeline.pipeline import ExportFinalPipeline
    except ImportError as exc:
        with _pipelines_lock:
            _pipelines.pop(camera_id, None)
        logger.error("fire_pipeline import 실패 — 의존성 설치 여부 확인: %s", exc)
        return False

    try:
        config = FirePipelineConfig(rtsp_url=rtsp_url, model_path=model_path)
        config.camera_id = camera_id
        on_result = _make_on_result(camera_id)
        pipeline = ExportFinalPipeline(config, on_result=on_result)

        thread = threading.Thread(
            target=pipeline.start,
            daemon=True,
            name=f"fire-pipeline-cam{camera_id}",
        )
        thread.start()
    except Exception:
        with _pipelines_lock:
            _pipelines.pop(camera_id, None)
        logger.exception("Fire pipeline start failed for camera %s", camera_id)
        return False

    with _pipelines_lock:
        _pipelines[camera_id] = {
            "thread": thread,
            "pipeline": pipeline,
            "latest_result": "",
            "starting": False,
        }
    logger.info("Fire pipeline started for camera %s (rtsp=%s)", camera_id, rtsp_url)
    return True


def stop_pipeline(camera_id: int) -> bool:
    """camera_id 파이프라인 중단.

    pipeline.running = False 플래그로 capture loop를 종료시킨다.
    스레드 join은 하지 않음 (daemon=True 이므로 프로세스 종료 시 자동 정리).
    """
    with _pipelines_lock:
        entry = _pipelines.pop(camera_id, None)
    if entry is None:
        return False

    pipeline = entry["pipeline"]
    if pipeline is not None:
        pipeline.running = False
    logger.info("Fire pipeline stop requested for camera %s", camera_id)
    return True


def get_status(camera_id: int) -> dict:
    """파이프라인 실행 상태와 최근 분석 결과 반환."""
    with _pipelines_lock:
        entry = _pipelines.get(camera_id)
    if entry is None:
        return {"running": False, "latest_result": ""}
    thread = entry.get("thread")
    return {
        "running": bool(thread and thread.is_alive()),
        "starting": bool(entry.get("starting")),
        "latest_result": entry["latest_result"],
    }


def get_latest_result(camera_id: int) -> str:
    with _pipelines_lock:
        entry = _pipelines.get(camera_id)
    return entry["latest_result"] if entry else ""


def stop_all() -> None:
    """서버 종료 시 모든 파이프라인 중단."""
    with _pipelines_lock:
        camera_ids = list(_pipelines.keys())

    for camera_id in camera_ids:
        stop_pipeline(camera_id)
    logger.info("All fire pipelines stopped")
