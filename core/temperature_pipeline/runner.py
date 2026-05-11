"""Temperature-triggered workplace VLM monitoring.

온습도 센서별 독립 스레드가 최신 온도를 감시한다.
온도가 임계치 이상이면 같은 space_id의 CCTV VLM 세션을 켜거나 연장하고,
온도가 내려가거나 최신 데이터가 끊기면 해당 온도계가 켠 감시를 중지한다.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Optional

from config import settings
from core.watch_pipeline.camera_vlm import WatchCameraVlmSession

logger = logging.getLogger(__name__)

TEMPERATURE_SENSOR_TYPES = {"temp_humidity", "temperature_humidity", "th", "temperature"}
DEFAULT_THRESHOLD_C = 38.0
DEFAULT_CHECK_INTERVAL_SECONDS = 10
DEFAULT_SESSION_SECONDS = 120
DEFAULT_ANALYSIS_INTERVAL_SECONDS = 30
DEFAULT_STALE_SECONDS = 60


class TemperatureCameraVlmManager:
    """온도 이상 이벤트 기준으로 같은 공간 카메라 VLM 세션을 관리한다."""

    def __init__(
        self,
        db_handler,
        *,
        loop=None,
        broadcast_fn: Optional[Callable[[dict], Any]] = None,
        session_seconds: int = DEFAULT_SESSION_SECONDS,
        analysis_interval_seconds: int = DEFAULT_ANALYSIS_INTERVAL_SECONDS,
    ):
        self._db_handler = db_handler
        self._loop = loop
        self._broadcast_fn = broadcast_fn
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
        if self._loop is None or self._broadcast_fn is None:
            return
        import asyncio

        asyncio.run_coroutine_threadsafe(self._broadcast_fn(payload), self._loop)

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


def build_temperature_pipeline_from_settings(db_handler, *, loop=None, broadcast_fn=None):
    manager = TemperatureCameraVlmManager(
        db_handler=db_handler,
        loop=loop,
        broadcast_fn=broadcast_fn,
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


def _build_temperature_prompt(camera: dict, trigger: dict) -> str:
    trigger_json = _json_dumps(trigger)
    return (
        "현재 CCTV 화면을 보고 작업장 상태를 JSON 하나로만 답하세요.\n"
        "온도 센서가 고온을 감지한 공간의 관리자 모니터링 보조가 목적입니다.\n"
        "사람의 이동 방향, 행동, 밀집, 회피, 쓰러짐, 위험 원인 단서를 중점적으로 보세요.\n"
        "불확실한 내용은 추측하지 말고 unknown 또는 빈 배열로 표시하세요.\n"
        "응답 형식:\n"
        "{"
        "\"summary\":\"현재 작업장 상태 한 문장\","
        "\"risk_level\":\"low|medium|high|unknown\","
        "\"visible_people\":\"none|one|multiple|unknown\","
        "\"person_actions\":[\"working\",\"moving\",\"leaving\",\"fallen\",\"helping\",\"unknown\"],"
        "\"movement\":\"none|toward_risk|away_from_risk|random|unknown\","
        "\"visible_risks\":[\"fire\",\"smoke\",\"heat_source\",\"steam\",\"crowding\",\"none\"],"
        "\"recommended_action\":\"관리자가 확인할 조치 한 문장\""
        "}\n"
        f"카메라={_json_dumps(_public_camera(camera))}\n"
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


def _public_camera(camera: dict) -> dict:
    return {
        "sen_id": camera.get("sen_id"),
        "sensor_id": camera.get("sensor_id"),
        "sen_name": camera.get("sen_name"),
        "space_id": camera.get("space_id"),
        "space_name": camera.get("space_name"),
        "ip_address": camera.get("ip_address"),
    }


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
