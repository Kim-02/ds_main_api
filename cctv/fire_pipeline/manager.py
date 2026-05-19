"""Fire pipeline 카메라별 인스턴스 관리.

카메라 등록 시 start_pipeline(), 삭제 시 stop_pipeline() 호출.
pipeline.running = False 로 스레드를 graceful shutdown 한다.
"""
import asyncio
import logging
import threading
from typing import Any, Callable, Optional

from core.notifications import extract_vlm_text, make_hazard_alert_ws_payload

logger = logging.getLogger(__name__)

# camera_id(int) → {"thread": Thread, "pipeline": ExportFinalPipeline, "latest_result": str}
_pipelines: dict[int, dict] = {}
_pipelines_lock = threading.Lock()
_pipeline_generations: dict[int, int] = {}
_stopped_generations: dict[int, int] = {}

# FastAPI 이벤트 루프 및 WebSocket 브로드캐스트 함수 (main.py lifespan에서 주입)
_loop: Optional[asyncio.AbstractEventLoop] = None
_broadcast_fn: Optional[Callable] = None
_db_handler = None


def init_manager(loop: asyncio.AbstractEventLoop, broadcast_fn: Callable, db_handler=None) -> None:
    """main.py lifespan에서 호출 — WebSocket 브로드캐스트 함수와 이벤트 루프를 주입."""
    global _loop, _broadcast_fn, _db_handler
    _loop = loop
    _broadcast_fn = broadcast_fn
    _db_handler = db_handler
    logger.info("FirePipelineManager 초기화 완료 db_persistence=%s", bool(db_handler))


def _alert_level(answer: Any) -> str:
    if isinstance(answer, dict):
        risk_level = str(answer.get("risk_level") or "").lower()
        if risk_level in {"critical", "danger"}:
            return "danger"
        if risk_level in {"high"}:
            return "danger"
        if risk_level in {"medium", "warning"}:
            return "warning"
    text = str(answer or "")
    if any(word in text for word in ("화재", "연기", "대피", "위험", "긴급", "critical", "danger")):
        return "danger"
    return "warning"


def _alert_message(answer: Any) -> str:
    if isinstance(answer, dict):
        summary = str(answer.get("summary") or "").strip()
        location = answer.get("workplace_location") if isinstance(answer.get("workplace_location"), dict) else {}
        target = answer.get("target") if isinstance(answer.get("target"), dict) else {}
        space_name = str(location.get("space_name") or target.get("site_name") or "").strip()
        screen = str(answer.get("screen_analysis") or "").strip()
        cause = answer.get("fire_cause") if isinstance(answer.get("fire_cause"), dict) else {}
        spread = answer.get("spread_path") if isinstance(answer.get("spread_path"), dict) else {}
        actions = answer.get("recommended_actions") if isinstance(answer.get("recommended_actions"), list) else []
        parts = []
        if summary:
            parts.append(f"[{space_name}] {summary}" if space_name and space_name not in summary else summary)
        if screen:
            parts.append(f"화면: {screen}")
        cause_text = str(cause.get("cause") or "").strip()
        if cause_text:
            parts.append(f"원인: {cause_text}")
        spread_text = str(spread.get("route") or spread.get("direction") or "").strip()
        if spread_text:
            parts.append(f"확산: {spread_text}")
        for action in actions[:2]:
            action_text = str(action or "").strip()
            if action_text:
                parts.append(f"조치: {action_text}")
        if parts:
            return " | ".join(parts)
    return (
        extract_vlm_text(answer)
        or str(answer or "").strip()
        or "CCTV 화재/연기 autoregressive VLM 분석이 완료되었습니다."
    )


def _answer_value(answer: Any, key: str, default: Any = "") -> Any:
    if isinstance(answer, dict):
        value = answer.get(key)
        if value not in (None, ""):
            return value
    return default


def _answer_dict(answer: Any, key: str) -> dict:
    if isinstance(answer, dict) and isinstance(answer.get(key), dict):
        return answer.get(key) or {}
    return {}


def _metadata_from_db_row(row: dict | None, fallback: dict | None = None) -> dict:
    fallback = dict(fallback or {})
    if not row:
        return fallback

    metadata = dict(fallback)
    space_id = row.get("space_id")
    metadata.update({
        "sen_id": int(row.get("sen_id") or fallback.get("sen_id") or 0),
        "sensor_id": row.get("sensor_id") or fallback.get("sensor_id"),
        "sen_name": row.get("sen_name") or fallback.get("sen_name") or fallback.get("camera_name"),
        "space_id": int(space_id) if space_id is not None else fallback.get("space_id"),
        "space_name": row.get("space_name") or fallback.get("space_name"),
        "hazard_type": row.get("hazard_type") or fallback.get("hazard_type"),
        "is_hazard": bool(row.get("is_hazard", fallback.get("is_hazard", False))),
        "is_demo": bool(row.get("is_demo", fallback.get("is_demo", False))),
        "demo_video_key": row.get("demo_video_key") or fallback.get("demo_video_key"),
        "ip_address": row.get("ip_address") or fallback.get("ip_address"),
        "camera_id": row.get("camera_id") or fallback.get("camera_id"),
        "camera_pw": row.get("camera_pw") or fallback.get("camera_pw"),
    })
    return metadata


def _load_workplace_metadata(camera_id: int, fallback: dict | None = None) -> dict:
    if _db_handler is None:
        return dict(fallback or {})
    try:
        row = _db_handler.get_cctv_by_sen_id(camera_id)
        return _metadata_from_db_row(row, fallback)
    except Exception:
        logger.exception("[FirePipeline] DB workplace metadata load failed camera_id=%s", camera_id)
        return dict(fallback or {})


def _save_alert_event(camera_id: int, message: str, metadata: dict, *, level: str) -> int | None:
    if _db_handler is None:
        return None

    try:
        event_id = _db_handler.save_vlm_alert_event(
            space_id=metadata.get("space_id"),
            jetson_id=metadata.get("jetson_id"),
            camera_sen_id=metadata.get("sen_id") or camera_id,
            sensor_id=metadata.get("sensor_id"),
            title="CCTV 화재/연기 autoregressive VLM 분석",
            message=message,
            level=level,
            source="fire_pipeline",
            event_type="fire_pipeline",
        )
        logger.info(
            "[VLM_ALERT] fire_pipeline saved event_id=%s camera_id=%s space_id=%s",
            event_id,
            camera_id,
            metadata.get("space_id"),
        )
        return event_id
    except Exception as exc:
        logger.exception("[VLM_ALERT] fire_pipeline save failed camera_id=%s: %s", camera_id, exc)
        return None


async def _broadcast_payload(payload: dict, *, camera_id: int, event_id: int | None) -> None:
    if _broadcast_fn is None:
        return

    try:
        sent = await _broadcast_fn(payload)
        logger.info(
            "[WS] fire_pipeline broadcast success event_id=%s camera_id=%s sent=%s",
            event_id,
            camera_id,
            sent if sent is not None else "-",
        )
    except Exception:
        logger.exception("[WS] fire_pipeline broadcast failed event_id=%s camera_id=%s", event_id, camera_id)


def _make_on_result(camera_id: int, metadata: dict | None = None, generation: int = 0) -> Callable[[str], None]:
    """파이프라인 결과를 메모리에 저장하고 WebSocket으로 전송하는 콜백 생성."""
    metadata = _load_workplace_metadata(camera_id, metadata or {})

    def on_result(answer: str, request_number: int | None = None) -> None:
        # stop/delete 이후 늦게 도착한 VLM 결과는 과거 이벤트이므로 DB 저장과 push를 모두 하지 않는다.
        with _pipelines_lock:
            entry = _pipelines.get(camera_id)
            stopped_generation = _stopped_generations.get(camera_id, 0)
            pipeline = entry.get("pipeline") if entry is not None else None
            is_stale = (
                entry is None
                or entry.get("generation") != generation
                or generation <= stopped_generation
                or (pipeline is not None and not getattr(pipeline, "running", True))
            )

        if is_stale:
            logger.info(
                "[FirePipeline] dropped stale VLM result camera_id=%s request=%s reason=pipeline_stopped",
                camera_id,
                request_number if request_number is not None else "-",
            )
            return

        level = _alert_level(answer)
        message_text = _alert_message(answer)
        current_metadata = _load_workplace_metadata(camera_id, metadata)
        event_id = _save_alert_event(camera_id, message_text, current_metadata, level=level)

        with _pipelines_lock:
            entry = _pipelines.get(camera_id)
            if (
                entry is None
                or entry.get("generation") != generation
                or generation <= _stopped_generations.get(camera_id, 0)
            ):
                logger.info(
                    "[FirePipeline] dropped stale VLM result camera_id=%s request=%s reason=pipeline_stopped",
                    camera_id,
                    request_number if request_number is not None else "-",
                )
                return
            entry["latest_result"] = answer
            entry["latest_event_id"] = event_id

        if _loop is not None and _broadcast_fn is not None:
            # 앱은 기존 온습도/VLM 알림과 같은 hazard_alert payload를 수신한다.
            # FirePipeline도 같은 /ws/alerts registry와 같은 payload 모델을 사용한다.
            payload = make_hazard_alert_ws_payload(
                event_id=event_id,
                message=message_text,
                title="CCTV 화재/연기 감지",
                level=level,
                space_id=current_metadata.get("space_id"),
                jetson_id=current_metadata.get("jetson_id"),
                camera_sen_id=current_metadata.get("sen_id") or camera_id,
                sensor_id=current_metadata.get("sensor_id"),
                camera_name=current_metadata.get("sen_name") or current_metadata.get("camera_name") or "",
                camera_loc=current_metadata.get("space_name") or current_metadata.get("sen_locate") or "",
                ev_code_name="CCTV 화재/연기 감지",
                source="fire_pipeline",
                vibration=True,
                led=True,
                duration_ms=5000,
                reset_after_ms=15000,
                vlm_result=answer,
                hazard_material=_answer_value(answer, "hazard_material", current_metadata.get("hazard_type") or ""),
                hazard_warning=_answer_value(answer, "hazard_warning", ""),
                hazard_specific_action=_answer_value(answer, "hazard_specific_action", ""),
                evacuation_route=_answer_value(answer, "evacuation_route", ""),
                detection_info={
                    "screen_analysis": _answer_value(answer, "screen_analysis", ""),
                    "fire_cause": _answer_dict(answer, "fire_cause"),
                    "spread_path": _answer_dict(answer, "spread_path"),
                    "workplace_location": _answer_dict(answer, "workplace_location"),
                    "visible_people": _answer_value(answer, "visible_people", ""),
                },
                person_movement={
                    "summary": _answer_value(answer, "person_movement", ""),
                },
            )
            try:
                asyncio.run_coroutine_threadsafe(
                    _broadcast_payload(payload, camera_id=camera_id, event_id=event_id),
                    _loop,
                )
            except Exception:
                logger.exception("[WS] fire_pipeline broadcast schedule failed event_id=%s camera_id=%s", event_id, camera_id)
        else:
            logger.info(
                "[WS] fire_pipeline broadcast skipped event_id=%s camera_id=%s reason=no_loop_or_broadcast_fn",
                event_id,
                camera_id,
            )

    return on_result


def _run_pipeline(camera_id: int, pipeline) -> None:
    try:
        pipeline.start()
    except Exception as exc:
        with _pipelines_lock:
            entry = _pipelines.get(camera_id)
            if entry is not None:
                entry["latest_error"] = repr(exc)
        logger.exception("Fire pipeline crashed for camera %s", camera_id)


def start_pipeline(
    camera_id: int,
    rtsp_url: str,
    model_path: str = "",
    metadata: dict | None = None,
) -> bool:
    """카메라 ID에 대한 fire pipeline 시작.

    이미 실행 중이면 False 반환.
    rtsp_url: 카메라 등록 시 생성된 RTSP 주소
    model_path: 화재 감지 YOLO 모델 경로 (비어 있으면 config 기본값 사용)
    """
    with _pipelines_lock:
        current = _pipelines.get(camera_id)
        if current is not None:
            thread = current.get("thread")
            starting = bool(current.get("starting"))
            if starting or (thread is not None and thread.is_alive()):
                logger.warning(
                    "Fire pipeline already running for camera %s status={running:%s, starting:%s}",
                    camera_id,
                    bool(thread and thread.is_alive()),
                    starting,
                )
                return False
            logger.warning(
                "Fire pipeline stale entry removed for camera %s latest_error=%s",
                camera_id,
                current.get("latest_error", ""),
            )
            _pipelines.pop(camera_id, None)
        generation = _pipeline_generations.get(camera_id, 0) + 1
        _pipeline_generations[camera_id] = generation
        _pipelines[camera_id] = {
            "thread": None,
            "pipeline": None,
            "latest_result": "",
            "latest_error": "",
            "latest_event_id": None,
            "starting": True,
            "generation": generation,
            "metadata": dict(metadata or {}),
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
        initial_metadata = _load_workplace_metadata(camera_id, metadata or {})
        config.workplace_info = initial_metadata
        config.workplace_info_provider = lambda: _load_workplace_metadata(camera_id, initial_metadata)
        on_result = _make_on_result(camera_id, metadata, generation)
        pipeline = ExportFinalPipeline(config, on_result=on_result)

        thread = threading.Thread(
            target=_run_pipeline,
            args=(camera_id, pipeline),
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
            "latest_error": "",
            "latest_event_id": None,
            "starting": False,
            "generation": generation,
            "metadata": dict(metadata or {}),
        }
    logger.info("FirePipeline started camera_id=%s source=%s", camera_id, rtsp_url)
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
    with _pipelines_lock:
        _stopped_generations[camera_id] = max(
            _stopped_generations.get(camera_id, 0),
            int(entry.get("generation") or _pipeline_generations.get(camera_id, 0)),
        )

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
        "latest_error": entry.get("latest_error", ""),
        "latest_event_id": entry.get("latest_event_id"),
        "metadata": entry.get("metadata", {}),
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
