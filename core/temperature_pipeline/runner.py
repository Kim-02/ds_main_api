"""Temperature-triggered workplace autoregressive VLM monitoring.

온습도 센서별 독립 스레드가 최신 온도를 감시한다.
온도가 임계치 이상이면 같은 space_id의 CCTV autoregressive VLM 세션을 켜거나 연장하고,
온도가 내려가거나 최신 데이터가 끊기면 해당 온도계가 켠 감시를 중지한다.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Optional

from config import settings
from core.watch_pipeline.camera_vlm import (
    WatchCameraVlmSession,
)
from core.vlm_prompt_builder import (
    build_environment_health_prompt,
    calc_heat_index_c,
    hazard_response_guide as shared_hazard_response_guide,
)

logger = logging.getLogger(__name__)

TEMPERATURE_SENSOR_TYPES = {"temp_humidity", "temperature_humidity", "th", "temperature"}
DEFAULT_THRESHOLD_C = 38.0
DEFAULT_CHECK_INTERVAL_SECONDS = 10
DEFAULT_SESSION_SECONDS = 120
DEFAULT_ANALYSIS_INTERVAL_SECONDS = 30
DEFAULT_STALE_SECONDS = 60

class TemperatureCameraVlmManager:
    """온도 이상 이벤트 기준으로 같은 공간 카메라 autoregressive VLM 세션을 관리한다."""

    def __init__(
        self,
        db_handler,
        *,
        loop=None,
        broadcast_fn: Optional[Callable[[dict], Any]] = None,
        mqtt_alert_fn: Optional[Callable[..., Any]] = None,
        session_seconds: int = DEFAULT_SESSION_SECONDS,
        analysis_interval_seconds: int = DEFAULT_ANALYSIS_INTERVAL_SECONDS,
    ):
        self._db_handler = db_handler
        self._loop = loop
        self._broadcast_fn = broadcast_fn
        self._mqtt_alert_fn = mqtt_alert_fn  # callable: publish_hazard_alert_to_space_watches
        self.session_seconds = session_seconds
        self.analysis_interval_seconds = analysis_interval_seconds
        self._sessions: dict[int, WatchCameraVlmSession] = {}
        self._session_sources: dict[int, set[str]] = {}
        self._lock = threading.Lock()

    def trigger_for_temperature_sensor(self, sensor_id: str, sample: dict) -> dict:
        logger.info(
            "[TemperatureVLM] START trigger sensor_id=%s temp=%s",
            sensor_id,
            sample.get("temp"),
        )
        sensor = self._db_handler.get_sensor_space_by_sensor_id(sensor_id)
        if not sensor:
            logger.warning("[TemperatureVLM] temperature sensor not found sensor_id=%s", sensor_id)
            return {"started": 0, "extended": 0, "space_id": None, "cameras": []}

        space_id = sensor.get("space_id")
        if space_id is None:
            logger.warning("[TemperatureVLM] temperature sensor has no space_id sensor_id=%s", sensor_id)
            return {"started": 0, "extended": 0, "space_id": None, "cameras": []}

        cameras = list(self._db_handler.get_cameras_by_space_id(int(space_id)))
        cameras.extend(_get_video_runtime_cameras_by_space_id(int(space_id)))
        started = 0
        extended = 0
        camera_statuses = []
        trigger = {
            "temperature_sensor_id": sensor_id,
            "temperature_sensor_name": sensor.get("sen_name"),
            "space_id": int(space_id),
            "space_name": sensor.get("space_name"),
            "is_hazard": _as_bool(sensor.get("is_hazard")),
            "hazard_type": sensor.get("hazard_type") or "",
            "sample": sample,
            "threshold_c": float(getattr(settings, "temperature_vlm_threshold", DEFAULT_THRESHOLD_C)),
            "heat_index": calc_heat_index_c(sample.get("temp"), sample.get("humid")),
            "health_attention": _get_health_attention_summary(self._db_handler, int(space_id)),
            "triggered_at": _now_iso(),
        }

        for camera in cameras:
            action, status = self._start_or_extend(camera, sensor_id=sensor_id, trigger=trigger)
            if action == "started":
                started += 1
            elif action == "extended":
                extended += 1
            camera_statuses.append(status)

        result = {
            "started": started,
            "extended": extended,
            "space_id": int(space_id),
            "space_name": sensor.get("space_name"),
            "is_hazard": _as_bool(sensor.get("is_hazard")),
            "hazard_type": sensor.get("hazard_type") or "",
            "temperature_sensor_id": sensor_id,
            "sample": sample,
            "cameras": camera_statuses,
        }
        logger.info("[TemperatureVLM] END trigger result=%s", result)
        return result

    def stop_for_temperature_sensor(self, sensor_id: str) -> dict:
        stopped = 0
        with self._lock:
            camera_ids = [
                camera_id
                for camera_id, sources in self._session_sources.items()
                if sensor_id in sources
            ]

        for camera_id in camera_ids:
            with self._lock:
                sources = self._session_sources.get(camera_id)
                if sources is not None:
                    sources.discard(sensor_id)
                    if sources:
                        continue
                    self._session_sources.pop(camera_id, None)
                session = self._sessions.pop(camera_id, None)

            if session is not None:
                session.stop()
                stopped += 1

        if stopped:
            logger.info("[TemperatureVLM] stopped sessions sensor_id=%s count=%s", sensor_id, stopped)
        return {"sensor_id": sensor_id, "stopped": stopped}

    def get_status(self) -> list[dict]:
        with self._lock:
            sessions = list(self._sessions.values())
        return [session.status() for session in sessions]

    def get_space_status(self, space_id: int) -> dict:
        with self._lock:
            sessions = [
                session
                for session in self._sessions.values()
                if int(session.camera.get("space_id") or -1) == int(space_id)
            ]
        return {"space_id": int(space_id), "sessions": [session.status() for session in sessions]}

    def stop_all(self) -> None:
        logger.info("[TemperatureVLM] stop_all sessions=%s", len(self._sessions))
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            self._session_sources.clear()
        for session in sessions:
            session.stop()

    def publish_result(self, payload: dict) -> None:
        from core.notifications import extract_vlm_text, make_hazard_alert_ws_payload

        camera = payload.get("camera") or {}
        result = payload.get("result") or {}
        status = payload.get("status") or {}
        yolo_context = payload.get("yolo_context") or {}
        last_trigger = status.get("last_trigger") or {}
        if isinstance(result, dict):
            result = _normalize_temperature_vlm_result(result, camera, last_trigger, yolo_context)
            payload["result"] = result
            logger.info(
                "[VLM TEXT] event_type=temperature_camera_vlm_enriched camera_sen_id=%s text=%s",
                camera.get("sen_id"),
                _json_dumps(result),
            )

        # VLM 결과에서 핵심 정보 추출
        message_text = extract_vlm_text(result) or payload.get("text") or "온습도 이상 감지 - VLM 분석 완료"
        risk_level = result.get("risk_level") if isinstance(result, dict) else None
        hazard_warning = result.get("hazard_warning") if isinstance(result, dict) else None

        # level 결정: danger / warning / info
        if risk_level in {"high", "critical"}:
            level = "danger"
            title = "온습도 위험 감지"
        elif risk_level == "medium" or _has_meaningful_text(hazard_warning):
            level = "warning"
            title = "온습도 주의 감지"
        else:
            level = "info"
            title = "온습도 VLM 분석 완료"

        if isinstance(result, dict):
            message_text = _compose_temperature_alert_message(result)

        camera_sen_id_raw = camera.get("sen_id")
        camera_sen_id = int(camera_sen_id_raw) if camera_sen_id_raw is not None else None
        space_id_raw = camera.get("space_id")
        space_id = int(space_id_raw) if space_id_raw is not None else None
        sensor_id = last_trigger.get("watch_sensor_id")

        # 1. alert_event 테이블에 저장
        event_id = self._save_alert_to_db(
            space_id=space_id,
            camera_sen_id=camera_sen_id,
            sensor_id=sensor_id,
            title=title,
            message=message_text,
            level=level,
        )

        # 2. WebSocket broadcast (DB 실패해도 broadcast 시도)
        if self._loop is None or self._broadcast_fn is None:
            return
        import asyncio

        vibration = level in {"danger", "warning"}
        ws_payload = make_hazard_alert_ws_payload(
            event_id=event_id,
            message=message_text,
            title=title,
            level=level,
            space_id=space_id,
            camera_sen_id=camera_sen_id,
            sensor_id=sensor_id,
            camera_name=camera.get("sen_name") or "",
            camera_loc=camera.get("space_name") or "",
            source="temperature_vlm",
            vibration=vibration,
            vlm_result=result,
            hazard_material=result.get("hazard_material") if isinstance(result, dict) else "",
            hazard_warning=result.get("hazard_warning") if isinstance(result, dict) else "",
            hazard_specific_action=result.get("hazard_specific_action") if isinstance(result, dict) else "",
            evacuation_route=result.get("evacuation_route") if isinstance(result, dict) else "",
            abnormal_behavior=result.get("abnormal_behavior") if isinstance(result, dict) else "",
            detection_info=_as_dict(result.get("detection_info")) if isinstance(result, dict) else {},
            person_movement=_as_dict(result.get("person_movement")) if isinstance(result, dict) else {},
            environment_detections=_as_dict(result.get("environment_detections")) if isinstance(result, dict) else {},
        )
        asyncio.run_coroutine_threadsafe(self._broadcast_fn(ws_payload), self._loop)
        logger.info(
            "[VLM_ALERT] broadcast event_id=%s space_id=%s level=%s",
            event_id, space_id, level,
        )

        # 3. 워치 MQTT publish (warning / danger / emergency만 전송)
        if self._mqtt_alert_fn is not None and level in {"warning", "danger", "emergency"} and space_id is not None:
            try:
                dur = {"warning": 3000, "danger": 5000, "emergency": 8000}.get(level, 5000)
                rst = {"warning": 15000, "danger": 20000, "emergency": 30000}.get(level, 15000)
                self._mqtt_alert_fn(
                    space_id,
                    event_id=event_id,
                    level=level,
                    title=title,
                    message=message_text,
                    vibration=True,
                    duration_ms=dur,
                    reset_after_ms=rst,
                )
            except Exception as exc:
                logger.exception("[VLM_ALERT] 워치 MQTT publish 실패: %s", exc)

    def _save_alert_to_db(
        self,
        *,
        space_id: int | None,
        camera_sen_id: int | None,
        sensor_id: str | None,
        title: str,
        message: str,
        level: str,
    ) -> int | None:
        """VLM 분석 결과를 alert_event 테이블에 저장하고 event_id를 반환한다."""
        try:
            return self._db_handler.save_vlm_alert_event(
                space_id=space_id,
                camera_sen_id=camera_sen_id,
                sensor_id=sensor_id,
                title=title,
                message=message,
                level=level,
                source="temperature_vlm",
                event_type="temperature_camera_vlm",
            )
        except Exception as exc:
            logger.exception("[TemperatureVLM] DB 알림 저장 실패: %s", exc)
            return None

    def _start_or_extend(self, camera: dict, *, sensor_id: str, trigger: dict) -> tuple[str, dict]:
        sen_id = int(camera["sen_id"])
        with self._lock:
            session = self._sessions.get(sen_id)
            self._session_sources.setdefault(sen_id, set()).add(sensor_id)

            if session is not None and session.is_alive():
                session.extend(
                    self.session_seconds,
                    watch_sensor_id=sensor_id,
                    worker_id=None,
                    prediction=trigger,
                )
                return "extended", session.status()

            session = WatchCameraVlmSession(
                camera=camera,
                manager=self,
                session_seconds=self.session_seconds,
                analysis_interval_seconds=self.analysis_interval_seconds,
                prompt_builder=_build_temperature_prompt,
                event_type="temperature_camera_vlm",
            )
            self._sessions[sen_id] = session
            session.start(
                watch_sensor_id=sensor_id,
                worker_id=None,
                prediction=trigger,
            )
            return "started", session.status()


class TemperaturePipelineRunner:
    """단일 온습도 센서의 최신 온도를 감시하는 스레드."""

    def __init__(
        self,
        sensor_id: str,
        db_handler,
        camera_vlm_manager: TemperatureCameraVlmManager,
        *,
        threshold_c: float = DEFAULT_THRESHOLD_C,
        check_interval_seconds: int = DEFAULT_CHECK_INTERVAL_SECONDS,
        stale_seconds: int = DEFAULT_STALE_SECONDS,
    ):
        self.sensor_id = sensor_id
        self._db_handler = db_handler
        self._camera_vlm_manager = camera_vlm_manager
        self.threshold_c = threshold_c
        self.check_interval_seconds = check_interval_seconds
        self.stale_seconds = stale_seconds
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._hot_active = False
        self._latest_sample: dict | None = None
        self._latest_error = ""

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop,
            name=f"temperature-pipeline-{self.sensor_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info("[TemperaturePipeline] thread started sensor_id=%s", self.sensor_id)

    def stop(self) -> None:
        self._stop_event.set()
        self._camera_vlm_manager.stop_for_temperature_sensor(self.sensor_id)
        logger.info("[TemperaturePipeline] thread stop requested sensor_id=%s", self.sensor_id)

    def status(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "running": self._thread.is_alive() if self._thread else False,
            "threshold_c": self.threshold_c,
            "check_interval_seconds": self.check_interval_seconds,
            "stale_seconds": self.stale_seconds,
            "hot_active": self._hot_active,
            "latest_sample": self._latest_sample,
            "latest_error": self._latest_error,
        }

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception as exc:
                self._latest_error = str(exc)
                logger.exception("[TemperaturePipeline] failed sensor_id=%s", self.sensor_id)
            self._stop_event.wait(self.check_interval_seconds)

    def _run_once(self) -> None:
        row = self._db_handler.get_latest_th_by_sensor_id(self.sensor_id)
        sample = _normalize_sample(row)
        self._latest_sample = sample

        if sample is None:
            self._stop_hot_session("no_sample")
            return

        if _is_stale(sample.get("time"), self.stale_seconds):
            self._stop_hot_session("stale_sample")
            return

        temp = sample.get("temp")
        if temp is None:
            self._stop_hot_session("no_temp")
            return

        if float(temp) >= float(self.threshold_c):
            self._hot_active = True
            self._latest_error = ""
            self._camera_vlm_manager.trigger_for_temperature_sensor(self.sensor_id, sample)
            return

        self._stop_hot_session("temperature_normal")

    def _stop_hot_session(self, reason: str) -> None:
        if self._hot_active:
            logger.info(
                "[TemperaturePipeline] stop hot session sensor_id=%s reason=%s latest=%s",
                self.sensor_id,
                reason,
                self._latest_sample,
            )
        self._hot_active = False
        self._camera_vlm_manager.stop_for_temperature_sensor(self.sensor_id)


class TemperaturePipelineScheduler:
    """등록된 온습도 센서마다 TemperaturePipelineRunner 스레드를 관리한다."""

    def __init__(
        self,
        db_handler,
        camera_vlm_manager: TemperatureCameraVlmManager,
        *,
        threshold_c: float = DEFAULT_THRESHOLD_C,
        check_interval_seconds: int = DEFAULT_CHECK_INTERVAL_SECONDS,
        stale_seconds: int = DEFAULT_STALE_SECONDS,
    ):
        self._db_handler = db_handler
        self._camera_vlm_manager = camera_vlm_manager
        self.threshold_c = threshold_c
        self.check_interval_seconds = check_interval_seconds
        self.stale_seconds = stale_seconds
        self._runners: dict[str, TemperaturePipelineRunner] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        logger.info("[TemperatureScheduler] start")
        sensors = self._db_handler.get_registered_sensor_rows()
        for sensor in sensors:
            sensor_type = str(sensor.get("sensor_type") or "").lower()
            sensor_id = sensor.get("sensor_id")
            if sensor_id and sensor_type in TEMPERATURE_SENSOR_TYPES:
                self.register(sensor_id)

    def stop(self) -> None:
        logger.info("[TemperatureScheduler] stop")
        with self._lock:
            runners = list(self._runners.values())
            self._runners.clear()
        for runner in runners:
            runner.stop()

    def register(self, sensor_id: str) -> None:
        with self._lock:
            if sensor_id in self._runners:
                return
            runner = TemperaturePipelineRunner(
                sensor_id=sensor_id,
                db_handler=self._db_handler,
                camera_vlm_manager=self._camera_vlm_manager,
                threshold_c=self.threshold_c,
                check_interval_seconds=self.check_interval_seconds,
                stale_seconds=self.stale_seconds,
            )
            self._runners[sensor_id] = runner
        runner.start()

    def unregister(self, sensor_id: str) -> None:
        with self._lock:
            runner = self._runners.pop(sensor_id, None)
        if runner:
            runner.stop()

    def get_status(self) -> dict:
        with self._lock:
            runners = list(self._runners.values())
        return {
            "threshold_c": self.threshold_c,
            "check_interval_seconds": self.check_interval_seconds,
            "stale_seconds": self.stale_seconds,
            "runner_count": len(runners),
            "runners": [runner.status() for runner in runners],
        }


def build_temperature_pipeline_from_settings(db_handler, *, loop=None, broadcast_fn=None, mqtt_alert_fn=None):
    manager = TemperatureCameraVlmManager(
        db_handler=db_handler,
        loop=loop,
        broadcast_fn=broadcast_fn,
        mqtt_alert_fn=mqtt_alert_fn,
        session_seconds=int(getattr(settings, "temperature_vlm_session_seconds", DEFAULT_SESSION_SECONDS)),
        analysis_interval_seconds=int(
            getattr(settings, "temperature_vlm_analysis_interval_seconds", DEFAULT_ANALYSIS_INTERVAL_SECONDS)
        ),
    )
    scheduler = TemperaturePipelineScheduler(
        db_handler=db_handler,
        camera_vlm_manager=manager,
        threshold_c=float(getattr(settings, "temperature_vlm_threshold", DEFAULT_THRESHOLD_C)),
        check_interval_seconds=int(
            getattr(settings, "temperature_vlm_check_interval_seconds", DEFAULT_CHECK_INTERVAL_SECONDS)
        ),
        stale_seconds=int(getattr(settings, "temperature_vlm_stale_seconds", DEFAULT_STALE_SECONDS)),
    )
    return manager, scheduler


class _NoopVlmPublishManager:
    def publish_result(self, payload: dict) -> None:
        return None


def run_temperature_camera_vlm_once(
    db_handler,
    sensor_id: str,
    *,
    manager: TemperatureCameraVlmManager | None = None,
    camera_sen_id: int | None = None,
    publish: bool = False,
    require_hot: bool = False,
) -> dict:
    """Run the real DB → CCTV → YOLO context → autoregressive VLM path once."""
    logger.info(
        "[TemperatureVLMDebug] START run_once sensor_id=%s camera_sen_id=%s publish=%s require_hot=%s",
        sensor_id,
        camera_sen_id,
        publish,
        require_hot,
    )
    row = db_handler.get_latest_th_by_sensor_id(sensor_id)
    sample = _normalize_sample(row)
    if sample is None:
        raise LookupError(f"No temperature sample found for sensor_id={sensor_id}")

    sensor = db_handler.get_sensor_space_by_sensor_id(sensor_id)
    if not sensor:
        raise LookupError(f"Temperature sensor not found sensor_id={sensor_id}")

    space_id = sensor.get("space_id")
    if space_id is None:
        raise LookupError(f"Temperature sensor has no space_id sensor_id={sensor_id}")

    temp = sample.get("temp")
    threshold = float(getattr(settings, "temperature_vlm_threshold", DEFAULT_THRESHOLD_C))
    if require_hot and (temp is None or float(temp) < threshold):
        return {
            "sensor_id": sensor_id,
            "space_id": int(space_id),
            "space_name": sensor.get("space_name"),
            "sample": sample,
            "threshold_c": threshold,
            "skipped": True,
            "reason": "temperature_below_threshold",
            "cameras": [],
        }

    cameras = list(db_handler.get_cameras_by_space_id(int(space_id)))
    cameras.extend(_get_video_runtime_cameras_by_space_id(int(space_id)))
    if camera_sen_id is not None:
        cameras = [camera for camera in cameras if int(camera.get("sen_id") or -1) == int(camera_sen_id)]

    if not cameras:
        raise LookupError(f"No camera found for space_id={space_id} camera_sen_id={camera_sen_id}")

    trigger = {
        "temperature_sensor_id": sensor_id,
        "temperature_sensor_name": sensor.get("sen_name"),
        "space_id": int(space_id),
        "space_name": sensor.get("space_name"),
        "is_hazard": _as_bool(sensor.get("is_hazard")),
        "hazard_type": sensor.get("hazard_type") or "",
        "sample": sample,
        "threshold_c": threshold,
        "heat_index": calc_heat_index_c(sample.get("temp"), sample.get("humid")),
        "health_attention": _get_health_attention_summary(db_handler, int(space_id)),
        "triggered_at": _now_iso(),
        "debug_run_once": True,
    }
    publish_manager = manager if manager is not None else _NoopVlmPublishManager()
    results = []

    for camera in cameras:
        session = WatchCameraVlmSession(
            camera=camera,
            manager=publish_manager,
            session_seconds=int(getattr(settings, "temperature_vlm_session_seconds", DEFAULT_SESSION_SECONDS)),
            analysis_interval_seconds=int(
                getattr(settings, "temperature_vlm_analysis_interval_seconds", DEFAULT_ANALYSIS_INTERVAL_SECONDS)
            ),
            prompt_builder=_build_temperature_prompt,
            event_type="temperature_camera_vlm",
        )
        camera_result = session.analyze_once(
            watch_sensor_id=sensor_id,
            worker_id=None,
            prediction=trigger,
            publish=publish,
        )
        logger.info(
            "[VLM TEXT] event_type=temperature_camera_vlm sensor_id=%s camera_sen_id=%s text=%s",
            sensor_id,
            camera.get("sen_id"),
            camera_result.get("text", ""),
        )
        results.append(camera_result)

    result = {
        "sensor_id": sensor_id,
        "space_id": int(space_id),
        "space_name": sensor.get("space_name"),
        "is_hazard": _as_bool(sensor.get("is_hazard")),
        "hazard_type": sensor.get("hazard_type") or "",
        "sample": sample,
        "threshold_c": threshold,
        "skipped": False,
        "camera_count": len(results),
        "cameras": results,
    }
    logger.info("[TemperatureVLMDebug] END run_once result_camera_count=%s", len(results))
    return result


def _build_temperature_prompt(camera: dict, trigger: dict, yolo_context: dict | None = None) -> str:
    return build_environment_health_prompt(camera, trigger, yolo_context)


def _normalize_sample(row: Optional[dict]) -> Optional[dict]:
    if not row:
        return None
    return {
        "sen_id": row.get("sen_id"),
        "sensor_id": row.get("sensor_id"),
        "sen_name": row.get("sen_name"),
        "time": _to_iso(row.get("time")),
        "temp": _optional_float(row.get("temp")),
        "humid": _optional_float(row.get("humid")),
    }


def _is_stale(value: Any, stale_seconds: int) -> bool:
    dt = _to_datetime(value)
    if dt is None:
        return True
    return (datetime.now() - dt).total_seconds() > stale_seconds


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("T", " ").replace("Z", ""))
        except ValueError:
            return None
    return None


def _to_iso(value: Any) -> str | None:
    dt = _to_datetime(value)
    if dt is None:
        return None
    return dt.isoformat(timespec="seconds")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, default=str)


def _get_video_runtime_cameras_by_space_id(space_id: int) -> list[dict]:
    try:
        from cctv.api.service import get_video_runtime_cameras_by_space_id
        return get_video_runtime_cameras_by_space_id(space_id)
    except Exception:
        return []


def _get_health_attention_summary(db_handler, space_id: int) -> dict:
    """현장 단위 건강 취약 작업자 요약.

    온습도 판단은 특정 작업자 문제를 확정하지 않으므로 VLM에는 집계/요약 중심으로 전달한다.
    """
    query = """
        SELECT
            w.dept_id,
            w.name,
            wh.age,
            wh.elderly_flag,
            wh.heart_disease,
            wh.hypertension,
            wh.other_disease,
            wh.baseline_hr,
            h.hr AS latest_hr
        FROM worker w
        JOIN sensor s
          ON w.sen_id = s.sen_id
        JOIN worker_hr_data wh
          ON wh.dept_id = w.dept_id
        LEFT JOIN hb_trans h
          ON h.sen_id = s.sen_id
         AND h.time = (
             SELECT MAX(h2.time)
             FROM hb_trans h2
             WHERE h2.sen_id = s.sen_id
         )
        WHERE s.space_id = %s
          AND w.is_manager = 0
        ORDER BY w.dept_id
    """
    try:
        with db_handler._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (space_id,))
                rows = cursor.fetchall()
    except Exception as exc:
        logger.warning("[TemperatureVLM] health attention summary unavailable space_id=%s error=%s", space_id, exc)
        return {
            "total_worker_count": 0,
            "attention_worker_count": 0,
            "abnormal_hr_count": 0,
            "risk_factors": {},
            "summary": "건강상 주의 작업자 요약을 조회하지 못했습니다.",
        }

    total = len(rows)
    factor_counts = {
        "elderly": 0,
        "heart_disease": 0,
        "hypertension": 0,
        "other_disease": 0,
    }
    abnormal_hr_count = 0
    attention_count = 0
    attention_factors = set()
    hr_threshold = float(getattr(settings, "default_heartrate_threshold", 120) or 120)

    for row in rows:
        row_factors = []
        age = _optional_float(row.get("age"))
        elderly = _flag(row.get("elderly_flag")) or (age is not None and age >= 60)
        if elderly:
            factor_counts["elderly"] += 1
            row_factors.append("고령")
        if _flag(row.get("heart_disease")):
            factor_counts["heart_disease"] += 1
            row_factors.append("심장질환")
        if _flag(row.get("hypertension")):
            factor_counts["hypertension"] += 1
            row_factors.append("고혈압")
        if _flag(row.get("other_disease")):
            factor_counts["other_disease"] += 1
            row_factors.append("기타질환")

        latest_hr = _optional_float(row.get("latest_hr"))
        baseline_hr = _optional_float(row.get("baseline_hr"))
        hr_abnormal = False
        if latest_hr is not None:
            if latest_hr >= hr_threshold:
                hr_abnormal = True
            elif baseline_hr is not None and latest_hr - baseline_hr >= 30:
                hr_abnormal = True
        if hr_abnormal:
            abnormal_hr_count += 1
            row_factors.append("이상 심박 가능성")

        if row_factors:
            attention_count += 1
            attention_factors.update(row_factors)

    summary = (
        f"현장 작업자 {total}명 중 건강상 주의가 필요한 작업자 {attention_count}명"
        if total
        else "해당 공간에 워치가 매핑된 작업자 정보가 없습니다."
    )
    if attention_factors:
        summary += f" ({', '.join(sorted(attention_factors))})"

    return {
        "total_worker_count": total,
        "attention_worker_count": attention_count,
        "abnormal_hr_count": abnormal_hr_count,
        "risk_factors": factor_counts,
        "summary": summary,
    }


def _normalize_temperature_vlm_result(
    result: dict,
    camera: dict,
    last_trigger: dict,
    yolo_context: dict | None = None,
) -> dict:
    normalized = dict(result)
    hazard_context = _build_hazard_context(camera, last_trigger)
    hazard_type = str(hazard_context.get("hazard_type") or "").strip()
    is_hazard = _as_bool(hazard_context.get("is_hazard"))
    sample = hazard_context.get("sample") if isinstance(hazard_context.get("sample"), dict) else {}
    temp = sample.get("temp")
    space_name = hazard_context.get("space_name") or camera.get("space_name") or "작업장"
    guide = _hazard_response_guide(hazard_type)
    detection_info = _build_detection_info_from_yolo(yolo_context or {})
    health_attention = (
        hazard_context.get("health_attention")
        if isinstance(hazard_context.get("health_attention"), dict)
        else {}
    )

    if normalized.get("risk_level") is not None:
        normalized["risk_level"] = str(normalized.get("risk_level")).strip().lower()

    normalized["target"] = {
        "type": "site",
        "site_id": hazard_context.get("space_id") or camera.get("space_id"),
        "site_name": space_name,
        "worker_id": None,
        "worker_name": None,
    }
    normalized.setdefault("reason", "작업장 온습도/열지수와 현장 건강 취약 작업자 존재 여부를 함께 고려한 현장 단위 위험 안내입니다.")

    if temp is not None:
        normalized.setdefault("temperature_status", "high")
        normalized.setdefault(
            "environment_status",
            {
                "temperature_c": temp,
                "humidity_percent": sample.get("humid"),
                "heat_index_c": hazard_context.get("heat_index"),
                "status": "danger" if normalized.get("risk_level") in {"high", "critical"} else "caution",
            },
        )

    summary = str(normalized.get("summary") or "").strip()
    if (
        _summary_confuses_worker_temperature(summary)
        or _summary_confuses_hazard_distribution(summary, hazard_type)
    ):
        normalized["summary"] = _temperature_summary(space_name, temp, hazard_type if is_hazard else "")
    elif not summary:
        normalized["summary"] = _temperature_summary(space_name, temp, hazard_type if is_hazard else "")

    if detection_info:
        normalized["detection_info"] = detection_info
        normalized["detection_text"] = detection_info.get("text", "")
        normalized["environment_detections"] = detection_info.get("environment_detections", {})
        normalized["person_movement"] = detection_info.get("person_movement", {})
        if detection_info.get("visible_people"):
            normalized["visible_people"] = detection_info.get("visible_people")

    visible_risks = normalized.get("visible_risks")
    if isinstance(visible_risks, list):
        cleaned = [
            str(item)
            for item in visible_risks
            if str(item).strip().lower() not in {"heat_source", "temperature", "high_temperature"}
        ]
        normalized["visible_risks"] = cleaned or ["none"]
    if detection_info:
        yolo_risks = detection_info.get("visible_risks") or []
        if yolo_risks and yolo_risks != ["none"]:
            existing = normalized.get("visible_risks")
            existing_list = existing if isinstance(existing, list) else []
            merged = []
            for item in existing_list + yolo_risks:
                if item and item != "none" and item not in merged:
                    merged.append(item)
            normalized["visible_risks"] = merged or ["none"]

    if is_hazard and hazard_type and hazard_type not in {"none", "unknown"}:
        normalized["hazard_material"] = hazard_type
        if not _has_meaningful_text(normalized.get("hazard_warning")):
            normalized["hazard_warning"] = f"{hazard_type} 유의관리지역입니다. {guide}"
        elif hazard_type not in str(normalized.get("hazard_warning")):
            normalized["hazard_warning"] = f"{hazard_type}: {normalized.get('hazard_warning')}"

        if not _has_meaningful_text(normalized.get("hazard_specific_action")):
            normalized["hazard_specific_action"] = guide
        elif hazard_type not in str(normalized.get("hazard_specific_action")):
            normalized["hazard_specific_action"] = f"{hazard_type}: {normalized.get('hazard_specific_action')}"

        if normalized.get("risk_level") in {None, "", "low", "unknown"} and temp is not None:
            normalized["risk_level"] = "medium"
    else:
        normalized.setdefault("hazard_material", "none")

    if health_attention:
        normalized.setdefault("health_considerations", health_attention.get("summary") or "현장 건강 취약 작업자 정보 확인 필요")
    else:
        normalized.setdefault("health_considerations", "현장 건강 취약 작업자 정보 없음")

    recommended_actions = normalized.get("recommended_actions")
    if isinstance(recommended_actions, list) and recommended_actions:
        normalized.setdefault("recommended_action", str(recommended_actions[0]))

    normalized.setdefault("abnormal_behavior", "unknown")
    normalized.setdefault("evacuation_route", "unknown")
    return normalized


def _compose_temperature_alert_message(result: dict) -> str:
    parts = []
    summary = str(result.get("summary") or "").strip()
    detection_text = str(result.get("detection_text") or "").strip()
    hazard_warning = str(result.get("hazard_warning") or "").strip()
    hazard_action = str(result.get("hazard_specific_action") or "").strip()
    health = str(result.get("health_considerations") or "").strip()
    reason = str(result.get("reason") or "").strip()
    abnormal = str(result.get("abnormal_behavior") or "").strip()
    evacuation = str(result.get("evacuation_route") or "").strip()
    recommended = str(result.get("recommended_action") or "").strip()
    recommended_actions = result.get("recommended_actions")

    if summary:
        parts.append(summary)
    if reason:
        parts.append(f"이유: {reason}")
    if health:
        parts.append(f"건강 유의: {health}")
    if detection_text:
        parts.append(f"탐지: {detection_text}")
    if _has_meaningful_text(abnormal) and abnormal not in {"none", "unknown"}:
        parts.append(f"작업자 이상행동: {abnormal}")
    if _has_meaningful_text(hazard_warning):
        parts.append(f"위험물 경고: {hazard_warning}")
    if _has_meaningful_text(hazard_action):
        parts.append(f"위험물 대처: {hazard_action}")
    if _has_meaningful_text(evacuation):
        parts.append(f"대피: {evacuation}")
    if _has_meaningful_text(recommended):
        parts.append(f"조치: {recommended}")
    if isinstance(recommended_actions, list):
        for action in recommended_actions[:2]:
            if _has_meaningful_text(action):
                parts.append(f"조치: {action}")

    return _limit_text(" ".join(parts), 650) if parts else "온습도 이상 감지 - VLM 분석 완료"


def _build_detection_info_from_yolo(yolo_context: dict) -> dict:
    summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if not isinstance(summary, dict) or not summary:
        return {}

    current = summary.get("current") if isinstance(summary.get("current"), dict) else {}
    person_count = _current_count(current, "person")
    fire_count = _current_count(current, "fire")
    smoke_count = _current_count(current, "smoke")
    movement = summary.get("person_movement") if isinstance(summary.get("person_movement"), dict) else {}
    direction = str(movement.get("direction") or ("none" if person_count <= 0 else "unknown"))
    visible_risks = []
    if fire_count > 0:
        visible_risks.append("fire")
    if smoke_count > 0:
        visible_risks.append("smoke")
    if not visible_risks:
        visible_risks = ["none"]

    environment_detections = {
        "person": person_count,
        "fire": fire_count,
        "smoke": smoke_count,
    }
    text = f"person={person_count}, movement={direction}, fire={fire_count}, smoke={smoke_count}"

    return {
        "source": yolo_context.get("source"),
        "frame_count": yolo_context.get("frame_count"),
        "sampled_count": yolo_context.get("sampled_count"),
        "visible_people": summary.get("visible_people") or _visible_people_from_count(person_count),
        "visible_risks": visible_risks,
        "environment_detections": environment_detections,
        "person_movement": {
            "found": bool(movement.get("found")),
            "direction": direction,
            "delta": movement.get("delta") or [],
            "start": movement.get("start") or [],
            "end": movement.get("end") or [],
        },
        "current": current,
        "text": text,
    }


def _current_count(current: dict, class_name: str) -> int:
    item = current.get(class_name) if isinstance(current, dict) else {}
    if not isinstance(item, dict):
        return 0
    try:
        return int(item.get("count") or 0)
    except (TypeError, ValueError):
        return 0


def _visible_people_from_count(count: int) -> str:
    if count <= 0:
        return "none"
    if count == 1:
        return "one"
    return "multiple"


def _build_hazard_context(camera: dict, trigger: dict) -> dict:
    prediction = trigger.get("prediction") if isinstance(trigger, dict) else {}
    if not isinstance(prediction, dict):
        prediction = {}
    if not prediction and isinstance(trigger, dict):
        prediction = trigger

    is_hazard = _as_bool(
        prediction.get("is_hazard")
        if prediction.get("is_hazard") is not None
        else camera.get("is_hazard")
    )
    hazard_type = str(prediction.get("hazard_type") or camera.get("hazard_type") or "").strip()
    if not hazard_type:
        hazard_type = "unknown" if is_hazard else "none"

    return {
        "space_id": prediction.get("space_id") or camera.get("space_id"),
        "space_name": prediction.get("space_name") or camera.get("space_name"),
        "is_hazard": is_hazard,
        "hazard_type": hazard_type,
        "sample": prediction.get("sample") or {},
        "heat_index": prediction.get("heat_index"),
        "health_attention": prediction.get("health_attention") or {},
        "response_guide": _hazard_response_guide(hazard_type) if is_hazard else "",
        "instruction": (
            "hazard_type 유의물을 피하는 대피 경로와 경고를 포함"
            if is_hazard
            else "등록된 유의물 없음"
        ),
    }


def _hazard_response_guide(hazard_type: str) -> str:
    return shared_hazard_response_guide(hazard_type)


def _has_meaningful_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "none", "unknown", "null", "false", "0", "no", "없음"}


def _summary_confuses_worker_temperature(summary: str) -> bool:
    text = str(summary or "")
    return ("직원" in text or "작업자" in text or "근로자" in text) and "온도" in text and "작업장" not in text


def _summary_confuses_hazard_distribution(summary: str, hazard_type: str) -> bool:
    text = str(summary or "")
    hazard = str(hazard_type or "")
    if not hazard or hazard in {"none", "unknown"} or hazard not in text:
        return False
    return any(word in text for word in ("분포", "퍼져", "누출", "새고", "확산"))


def _temperature_summary(space_name: str, temp: Any, hazard_type: str) -> str:
    temp_text = f"{temp}도" if temp is not None else "기준 이상"
    if hazard_type:
        return f"{space_name}에서 온습도 센서가 {temp_text} 고온을 감지했고, 등록 위험물 {hazard_type}에 대한 주의가 필요합니다."
    return f"{space_name}에서 온습도 센서가 {temp_text} 고온을 감지했습니다."


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _flag(value: Any) -> bool:
    return _as_bool(value)


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _limit_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[: max(0, max_chars - len(marker))] + marker


def _public_camera(camera: dict) -> dict:
    return {
        "sen_id": camera.get("sen_id"),
        "sensor_id": camera.get("sensor_id"),
        "sen_name": camera.get("sen_name"),
        "space_id": camera.get("space_id"),
        "space_name": camera.get("space_name"),
        "hazard_type": camera.get("hazard_type"),
        "is_hazard": _as_bool(camera.get("is_hazard")),
        "ip_address": camera.get("ip_address"),
    }


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
