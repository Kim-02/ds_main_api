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
    build_common_autoregressive_vlm_prompt,
)

logger = logging.getLogger(__name__)

TEMPERATURE_SENSOR_TYPES = {"temp_humidity", "temperature_humidity", "th", "temperature"}
DEFAULT_THRESHOLD_C = 38.0
DEFAULT_CHECK_INTERVAL_SECONDS = 10
DEFAULT_SESSION_SECONDS = 120
DEFAULT_ANALYSIS_INTERVAL_SECONDS = 30
DEFAULT_STALE_SECONDS = 60

HAZARD_RESPONSE_GUIDES = {
    "벤젠": (
        "벤젠: 인화성·독성 증기 위험. 불꽃/스파크/고온장비를 차단하고 흡입·피부접촉을 피하며, "
        "안전이 확보될 때만 환기한다. 누출·화재 의심 시 접근하지 말고 상풍측 통로로 대피, 전문 대응을 요청한다."
    ),
    "benzene": (
        "벤젠: 인화성·독성 증기 위험. 불꽃/스파크/고온장비를 차단하고 흡입·피부접촉을 피하며, "
        "안전이 확보될 때만 환기한다. 누출·화재 의심 시 접근하지 말고 상풍측 통로로 대피, 전문 대응을 요청한다."
    ),
    "나트륨": (
        "나트륨: 물·습기와 격렬히 반응할 수 있음. 물 분사와 젖은 장비 사용을 금지하고, "
        "건조 상태로 격리한다. 화재 시 일반 물소화 대신 금속화재 대응 절차와 전문 인력을 호출한다."
    ),
    "sodium": (
        "나트륨: 물·습기와 격렬히 반응할 수 있음. 물 분사와 젖은 장비 사용을 금지하고, "
        "건조 상태로 격리한다. 화재 시 일반 물소화 대신 금속화재 대응 절차와 전문 인력을 호출한다."
    ),
    "철강": (
        "철강: 고온 표면, 중량물, 절단·용접 작업 시 화상·비산물·흄 위험. "
        "작업자를 고온 설비와 중량물 이동 경로에서 떨어뜨리고 보호구와 환기 상태를 확인한다."
    ),
}


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

        cameras = self._db_handler.get_cameras_by_space_id(int(space_id))
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
        last_trigger = status.get("last_trigger") or {}
        if isinstance(result, dict):
            result = _normalize_temperature_vlm_result(result, camera, last_trigger)
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
        if risk_level == "high":
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

    cameras = db_handler.get_cameras_by_space_id(int(space_id))
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
    hazard_context = _build_hazard_context(camera, trigger)
    trigger_json = _limit_text(_json_dumps(trigger), 260)
    hazard_json = _limit_text(_json_dumps(hazard_context), 520)
    return (
        build_common_autoregressive_vlm_prompt(camera, yolo_context)
        + "목적: 고온 감지 작업장의 현재 상황, 작업자 이상행동, 위험물별 대처를 관리자에게 전달합니다.\n"
        "중요: 온도 값은 작업자 체온이 아니라 작업장 환경 온도입니다. '직원 온도가 높다'라고 쓰지 마세요.\n"
        "현재 이미지와 YOLO정규화텍스트에 보이는 것만 시각 위험으로 확정하세요. 화재/연기/열원은 보일 때만 visible_risks에 넣으세요.\n"
        "온도센서 고온만으로 heat_source를 만들지 말고 temperature_status/high로 표현하세요.\n"
        "유의관리지역이면 hazard_material과 hazard_warning, hazard_specific_action에 hazard_type별 대처를 반드시 포함하세요.\n"
        "비틀거림, 중심 잃음, 쓰러짐, 혼란 행동이 보이면 abnormal_behavior에 명시하세요. 불확실하면 unknown 또는 none.\n"
        "JSON 하나만 답하세요. 코드블록 금지:\n"
        "{"
        "\"summary\":\"한 문장\","
        "\"risk_level\":\"low|medium|high|unknown\","
        "\"temperature_status\":\"normal|high|unknown\","
        "\"visible_people\":\"none|one|multiple|unknown\","
        "\"person_actions\":[\"working\",\"moving\",\"leaving\",\"staggering\",\"unstable_posture\",\"fallen\",\"helping\",\"unknown\"],"
        "\"abnormal_behavior\":\"none|staggering|unstable_posture|fallen|confused|unknown\","
        "\"movement\":\"none|toward_risk|away_from_risk|random|unknown\","
        "\"visible_risks\":[\"fire\",\"smoke\",\"steam\",\"spill\",\"crowding\",\"unsafe_posture\",\"none\"],"
        "\"hazard_material\":\"위험물명 또는 none\","
        "\"evacuation_route\":\"대피 경로 또는 unknown\","
        "\"hazard_warning\":\"위험물별 경고 또는 none\","
        "\"hazard_specific_action\":\"위험물별 대처 한 문장\","
        "\"recommended_action\":\"관리자/작업자 조치 한 문장\""
        "}\n"
        f"유의관리지역={hazard_json}\n"
        f"온도이벤트={trigger_json}"
    )


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


def _normalize_temperature_vlm_result(result: dict, camera: dict, last_trigger: dict) -> dict:
    normalized = dict(result)
    hazard_context = _build_hazard_context(camera, last_trigger)
    hazard_type = str(hazard_context.get("hazard_type") or "").strip()
    is_hazard = _as_bool(hazard_context.get("is_hazard"))
    sample = hazard_context.get("sample") if isinstance(hazard_context.get("sample"), dict) else {}
    temp = sample.get("temp")
    space_name = hazard_context.get("space_name") or camera.get("space_name") or "작업장"
    guide = _hazard_response_guide(hazard_type)

    if temp is not None:
        normalized.setdefault("temperature_status", "high")

    summary = str(normalized.get("summary") or "").strip()
    if _summary_confuses_worker_temperature(summary):
        normalized["summary"] = _temperature_summary(space_name, temp, hazard_type if is_hazard else "")
    elif not summary:
        normalized["summary"] = _temperature_summary(space_name, temp, hazard_type if is_hazard else "")

    visible_risks = normalized.get("visible_risks")
    if isinstance(visible_risks, list):
        cleaned = [
            str(item)
            for item in visible_risks
            if str(item).strip().lower() not in {"heat_source", "temperature", "high_temperature"}
        ]
        normalized["visible_risks"] = cleaned or ["none"]

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

    normalized.setdefault("abnormal_behavior", "unknown")
    normalized.setdefault("evacuation_route", "unknown")
    return normalized


def _compose_temperature_alert_message(result: dict) -> str:
    parts = []
    summary = str(result.get("summary") or "").strip()
    hazard_warning = str(result.get("hazard_warning") or "").strip()
    hazard_action = str(result.get("hazard_specific_action") or "").strip()
    abnormal = str(result.get("abnormal_behavior") or "").strip()
    evacuation = str(result.get("evacuation_route") or "").strip()
    recommended = str(result.get("recommended_action") or "").strip()

    if summary:
        parts.append(summary)
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

    return _limit_text(" ".join(parts), 900) if parts else "온습도 이상 감지 - VLM 분석 완료"


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
        "response_guide": _hazard_response_guide(hazard_type) if is_hazard else "",
        "instruction": (
            "hazard_type 유의물을 피하는 대피 경로와 경고를 포함"
            if is_hazard
            else "등록된 유의물 없음"
        ),
    }


def _hazard_response_guide(hazard_type: str) -> str:
    normalized = str(hazard_type or "").strip()
    if not normalized or normalized in {"none", "unknown"}:
        return "등록 위험물 정보가 없으므로 현장 안전관리자 지시에 따라 접근을 제한하고 작업자를 안전 구역으로 이동시킨다."

    lower = normalized.lower()
    for key, guide in HAZARD_RESPONSE_GUIDES.items():
        if key.lower() in lower or lower in key.lower():
            return guide

    return (
        f"{normalized}: 등록된 유의물질입니다. 직접 접촉·흡입·가열을 피하고, "
        "작업자를 위험물 반대 방향의 안전 구역으로 이동시키며 현장 안전관리자와 전문 대응팀에 즉시 알린다."
    )


def _has_meaningful_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "none", "unknown", "null", "false", "0", "no", "없음"}


def _summary_confuses_worker_temperature(summary: str) -> bool:
    text = str(summary or "")
    return ("직원" in text or "작업자" in text or "근로자" in text) and "온도" in text and "작업장" not in text


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
