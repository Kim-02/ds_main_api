"""Watch-triggered workplace autoregressive VLM sessions.

휴식 권고가 발생한 워치와 같은 space_id의 카메라를 2분 동안 분석한다.
같은 카메라가 이미 분석 중이면 종료 시간을 다시 2분 뒤로 연장한다.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Optional
from urllib.parse import quote

from config import settings
from core.cctv_vlm_context import (
    build_yolo_normalized_context_from_buffer,
    build_yolo_normalized_context_from_frames,
    public_yolo_context,
)
from core.notifications import make_vlm_push_payload
from core.vlm_prompt_builder import (
    build_common_autoregressive_vlm_prompt as shared_build_common_autoregressive_vlm_prompt,
    build_retry_prompt as shared_build_retry_prompt,
    build_worker_regression_prompt,
    compact_camera_for_prompt as shared_compact_camera_for_prompt,
    extract_hazard_type as shared_extract_hazard_type,
    format_yolo_context_for_prompt as shared_format_yolo_context_for_prompt,
    health_risk_factor_names as shared_health_risk_factor_names,
    limit_text as shared_limit_text,
)

logger = logging.getLogger(__name__)

DEFAULT_SESSION_SECONDS = 120
DEFAULT_ANALYSIS_INTERVAL_SECONDS = 30


class WatchCameraVlmManager:
    """space_id 기준 카메라 autoregressive VLM 세션을 관리한다."""

    def __init__(
        self,
        db_handler,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        broadcast_fn: Optional[Callable[[dict], Any]] = None,
        session_seconds: int = DEFAULT_SESSION_SECONDS,
        analysis_interval_seconds: int = DEFAULT_ANALYSIS_INTERVAL_SECONDS,
        prompt_builder: Optional[Callable[..., str]] = None,
        event_type: str = "watch_camera_vlm",
    ):
        self._db_handler = db_handler
        self._loop = loop
        self._broadcast_fn = broadcast_fn
        self.session_seconds = session_seconds
        self.analysis_interval_seconds = analysis_interval_seconds
        self.prompt_builder = prompt_builder or _build_prompt
        self.event_type = event_type
        self._sessions: dict[int, WatchCameraVlmSession] = {}
        self._lock = threading.Lock()

    def trigger_for_watch_sensor(
        self,
        watch_sensor_id: str,
        *,
        worker_id: str | None = None,
        prediction: dict | None = None,
    ) -> dict:
        logger.info(
            "[WatchCameraVLM] START trigger watch_sensor_id=%s worker_id=%s",
            watch_sensor_id,
            worker_id,
        )
        watch = self._db_handler.get_sensor_space_by_sensor_id(watch_sensor_id)
        if not watch:
            logger.warning("[WatchCameraVLM] watch sensor not found sensor_id=%s", watch_sensor_id)
            return {"started": 0, "extended": 0, "space_id": None, "cameras": []}

        space_id = watch.get("space_id")
        if space_id is None:
            logger.warning("[WatchCameraVLM] watch sensor has no space_id sensor_id=%s", watch_sensor_id)
            return {"started": 0, "extended": 0, "space_id": None, "cameras": []}

        cameras = list(self._db_handler.get_cameras_by_space_id(int(space_id)))
        cameras.extend(_get_video_runtime_cameras_by_space_id(int(space_id)))
        started = 0
        extended = 0
        camera_statuses = []

        for camera in cameras:
            action, status = self._start_or_extend(
                camera,
                watch_sensor_id=watch_sensor_id,
                worker_id=worker_id,
                prediction=prediction,
            )
            if action == "started":
                started += 1
            elif action == "extended":
                extended += 1
            camera_statuses.append(status)

        result = {
            "started": started,
            "extended": extended,
            "space_id": int(space_id),
            "space_name": watch.get("space_name"),
            "cameras": camera_statuses,
        }
        logger.info("[WatchCameraVLM] END trigger result=%s", result)
        return result

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
        return {
            "space_id": int(space_id),
            "sessions": [session.status() for session in sessions],
        }

    def stop_all(self) -> None:
        logger.info("[WatchCameraVLM] stop_all sessions=%s", len(self._sessions))
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            session.stop()

    def _start_or_extend(
        self,
        camera: dict,
        *,
        watch_sensor_id: str,
        worker_id: str | None,
        prediction: dict | None,
    ) -> tuple[str, dict]:
        sen_id = int(camera["sen_id"])
        with self._lock:
            session = self._sessions.get(sen_id)
            if session is not None and session.is_alive():
                session.extend(
                    self.session_seconds,
                    watch_sensor_id=watch_sensor_id,
                    worker_id=worker_id,
                    prediction=prediction,
                )
                return "extended", session.status()

            session = WatchCameraVlmSession(
                camera=camera,
                manager=self,
                session_seconds=self.session_seconds,
                analysis_interval_seconds=self.analysis_interval_seconds,
                prompt_builder=self.prompt_builder,
                event_type=self.event_type,
            )
            self._sessions[sen_id] = session
            session.start(
                watch_sensor_id=watch_sensor_id,
                worker_id=worker_id,
                prediction=prediction,
            )
            return "started", session.status()

    def publish_result(self, payload: dict) -> None:
        if self._loop is None or self._broadcast_fn is None:
            return
        asyncio.run_coroutine_threadsafe(self._broadcast_fn(payload), self._loop)


class WatchCameraVlmSession:
    """단일 카메라를 2분 동안 주기 분석하는 스레드 세션."""

    def __init__(
        self,
        *,
        camera: dict,
        manager: WatchCameraVlmManager,
        session_seconds: int,
        analysis_interval_seconds: int,
        prompt_builder: Callable[..., str],
        event_type: str,
    ):
        self.camera = camera
        self.manager = manager
        self.session_seconds = session_seconds
        self.analysis_interval_seconds = analysis_interval_seconds
        self.prompt_builder = prompt_builder
        self.event_type = event_type
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._deadline_monotonic = 0.0
        self._trigger_count = 0
        self._last_trigger: dict[str, Any] = {}
        self._latest_result: Any = None
        self._latest_error = ""
        self._latest_frame_path = ""
        self._latest_frame_source = ""
        self._latest_yolo_context: dict[str, Any] = {}
        self._started_at = ""
        self._updated_at = ""
        self._running = False

    def start(
        self,
        *,
        watch_sensor_id: str,
        worker_id: str | None,
        prediction: dict | None,
    ) -> None:
        self.extend(
            self.session_seconds,
            watch_sensor_id=watch_sensor_id,
            worker_id=worker_id,
            prediction=prediction,
        )
        self._started_at = _now_iso()
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            name=f"watch-camera-vlm-{self.camera['sen_id']}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "[WatchCameraVLM] session started camera_sen_id=%s space_id=%s",
            self.camera.get("sen_id"),
            self.camera.get("space_id"),
        )

    def extend(
        self,
        seconds: int,
        *,
        watch_sensor_id: str,
        worker_id: str | None,
        prediction: dict | None,
    ) -> None:
        with self._lock:
            self._deadline_monotonic = time.monotonic() + seconds
            self._trigger_count += 1
            self._last_trigger = {
                "watch_sensor_id": watch_sensor_id,
                "worker_id": worker_id,
                "prediction": prediction or {},
                "triggered_at": _now_iso(),
            }
            self._updated_at = _now_iso()
        self._wake_event.set()
        logger.info(
            "[WatchCameraVLM] session extended camera_sen_id=%s seconds=%s trigger_count=%s",
            self.camera.get("sen_id"),
            seconds,
            self._trigger_count,
        )

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        with self._lock:
            self._running = False
        logger.info("[WatchCameraVLM] session stop requested camera_sen_id=%s", self.camera.get("sen_id"))

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict:
        with self._lock:
            remaining = max(0.0, self._deadline_monotonic - time.monotonic())
            return {
                "running": self.is_alive() and self._running,
                "camera": _public_camera(self.camera),
                "remaining_seconds": round(remaining, 1),
                "trigger_count": self._trigger_count,
                "last_trigger": self._last_trigger,
                "latest_result": self._latest_result,
                "latest_error": self._latest_error,
                "latest_frame_path": self._latest_frame_path,
                "latest_frame_source": self._latest_frame_source,
                "latest_yolo_context": (
                    public_yolo_context(self._latest_yolo_context)
                    if self._latest_yolo_context
                    else {}
                ),
                "started_at": self._started_at,
                "updated_at": self._updated_at,
            }

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                if self._expired():
                    break

                try:
                    yolo_context = self._build_autoregressive_context()
                    frame_path = str(yolo_context.get("image_path") or "")
                    result = self._request_vlm(yolo_context)
                    self._set_result(result, frame_path, "", yolo_context)
                    self.manager.publish_result(make_vlm_push_payload(
                        self.event_type,
                        _notification_title(self.event_type),
                        result,
                        camera=_public_camera(self.camera),
                        frame_path=frame_path,
                        yolo_context=public_yolo_context(yolo_context),
                        status=self.status(),
                    ))
                except Exception as exc:
                    logger.exception(
                        "[WatchCameraVLM] analysis failed camera_sen_id=%s",
                        self.camera.get("sen_id"),
                    )
                    self._set_result(self._latest_result, self._latest_frame_path, str(exc))

                self._wait_next_interval()
        finally:
            with self._lock:
                self._running = False
                self._updated_at = _now_iso()
            logger.info("[WatchCameraVLM] session ended camera_sen_id=%s", self.camera.get("sen_id"))

    def _expired(self) -> bool:
        with self._lock:
            return time.monotonic() >= self._deadline_monotonic

    def _wait_next_interval(self) -> None:
        self._wake_event.clear()
        with self._lock:
            remaining = max(0.0, self._deadline_monotonic - time.monotonic())
        wait_seconds = min(self.analysis_interval_seconds, remaining)
        if wait_seconds > 0:
            self._wake_event.wait(wait_seconds)

    def analyze_once(
        self,
        *,
        watch_sensor_id: str,
        worker_id: str | None = None,
        prediction: dict | None = None,
        publish: bool = False,
    ) -> dict:
        """Run one real CCTV autoregressive VLM request without starting a session thread."""
        with self._lock:
            self._trigger_count += 1
            self._last_trigger = {
                "watch_sensor_id": watch_sensor_id,
                "worker_id": worker_id,
                "prediction": prediction or {},
                "triggered_at": _now_iso(),
            }
            self._updated_at = _now_iso()

        yolo_context = self._build_autoregressive_context()
        frame_path = str(yolo_context.get("image_path") or "")
        result = self._request_vlm(yolo_context)
        self._set_result(result, frame_path, "", yolo_context)

        payload = make_vlm_push_payload(
            self.event_type,
            _notification_title(self.event_type),
            result,
            camera=_public_camera(self.camera),
            frame_path=frame_path,
            yolo_context=public_yolo_context(yolo_context),
            status=self.status(),
        )
        if publish:
            self.manager.publish_result(payload)

        return {
            "camera": _public_camera(self.camera),
            "frame_path": frame_path,
            "yolo_context": public_yolo_context(yolo_context),
            "result": result,
            "text": payload.get("text", ""),
            "payload": payload,
            "status": self.status(),
        }

    def _build_autoregressive_context(self) -> dict:
        context = build_yolo_normalized_context_from_buffer(
            self.camera,
            _camera_id_candidates(self.camera),
        )
        if context is not None:
            with self._lock:
                self._latest_frame_source = context.source
            return context.to_dict()

        logger.info(
            "[WatchCameraVLM] YOLO buffer context unavailable, fallback to RTSP single-frame context camera_sen_id=%s candidates=%s",
            self.camera.get("sen_id"),
            _camera_id_candidates(self.camera),
        )
        frame = self._capture_raw_frame_from_rtsp()
        fallback_camera_id = _camera_id_candidates(self.camera)
        context = build_yolo_normalized_context_from_frames(
            self.camera,
            [{"timestamp": time.time(), "frame": frame}],
            source="rtsp:fallback_single_frame",
            source_camera_id=fallback_camera_id[0] if fallback_camera_id else None,
        )
        with self._lock:
            self._latest_frame_source = context.source
        return context.to_dict()

    def _capture_raw_frame_from_rtsp(self) -> Any:
        import cv2

        rtsp_url = _build_rtsp_url(self.camera)
        if rtsp_url.startswith("rtsp://"):
            os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|max_delay;0"
            )

        if rtsp_url.startswith("rtsp://"):
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
        else:
            cap = cv2.VideoCapture(rtsp_url)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            cap.release()
            raise RuntimeError(f"RTSP stream open failed: {rtsp_url}")

        frame = None
        try:
            for _ in range(5):
                ok, current = cap.read()
                if ok:
                    frame = current
        finally:
            cap.release()

        if frame is None:
            raise RuntimeError("RTSP frame read failed")

        return frame

    def _request_vlm(self, yolo_context: dict) -> Any:
        from ai.vlm.client import OpenAiCompatibleVlm, extract_json

        client = OpenAiCompatibleVlm(
            settings.vllm_base_url,
            settings.vllm_api_key,
            settings.vllm_model,
            timeout=settings.camera_vlm_request_timeout_seconds,
        )
        frame_path = str(yolo_context.get("image_path") or "")
        prompt = _call_prompt_builder(self.prompt_builder, self.camera, self._last_trigger, yolo_context)
        prompt = _limit_prompt_for_vlm(prompt)
        max_tokens = int(getattr(settings, "camera_vlm_max_tokens", 256) or 256)
        logger.info(
            "[WatchCameraVLM] autoregressive VLM request camera_sen_id=%s event_type=%s frame=%s prompt_chars=%s max_tokens=%s yolo_context=%s",
            self.camera.get("sen_id"),
            self.event_type,
            frame_path,
            len(prompt),
            max_tokens,
            public_yolo_context(yolo_context),
        )
        try:
            text = client.request_text(prompt, frame_path, max_tokens=max_tokens, temperature=0.2, stream=False)
        except Exception as exc:
            if not _is_prompt_too_long_error(exc):
                raise
            retry_prompt = _build_retry_prompt(self.camera, self._last_trigger, yolo_context, self.event_type)
            logger.warning(
                "[WatchCameraVLM] prompt too long, retry with compact prompt camera_sen_id=%s original_chars=%s retry_chars=%s error=%s",
                self.camera.get("sen_id"),
                len(prompt),
                len(retry_prompt),
                exc,
            )
            text = client.request_text(
                retry_prompt,
                frame_path,
                max_tokens=min(max_tokens, 192),
                temperature=0.2,
                stream=False,
            )
        logger.info(
            "[VLM TEXT] event_type=%s camera_sen_id=%s text=%s",
            self.event_type,
            self.camera.get("sen_id"),
            text,
        )
        try:
            result = extract_json(text)
            if isinstance(result, dict):
                return _normalize_worker_regression_vlm_result(result, self.camera, self._last_trigger, yolo_context)
            return result
        except Exception as exc:
            logger.warning("[WatchCameraVLM] JSON response parse failed: %s", exc)
            return {"raw_text": text}

    def _set_result(
        self,
        result: Any,
        frame_path: str,
        error: str,
        yolo_context: dict | None = None,
    ) -> None:
        with self._lock:
            self._latest_result = result
            self._latest_error = error
            self._latest_frame_path = frame_path
            if yolo_context is not None:
                self._latest_yolo_context = dict(yolo_context)
            self._updated_at = _now_iso()


def _build_rtsp_url(camera: dict) -> str:
    direct_source = camera.get("rtsp_url") or camera.get("video_path")
    if direct_source:
        return str(direct_source)

    path = settings.fire_pipeline_rtsp_path
    if not path.startswith("/"):
        path = f"/{path}"
    username = quote(str(camera.get("camera_id") or ""), safe="")
    password = quote(str(camera.get("camera_pw") or ""), safe="")
    ip_address = camera.get("ip_address")
    return f"rtsp://{username}:{password}@{ip_address}{path}"


def _get_video_runtime_cameras_by_space_id(space_id: int) -> list[dict]:
    try:
        from cctv.api.service import get_video_runtime_cameras_by_space_id
        return get_video_runtime_cameras_by_space_id(space_id)
    except Exception:
        return []


def build_common_autoregressive_vlm_prompt(camera: dict, yolo_context: dict | None) -> str:
    return shared_build_common_autoregressive_vlm_prompt(camera, yolo_context)


def _build_prompt(camera: dict, trigger: dict, yolo_context: dict | None = None) -> str:
    return build_worker_regression_prompt(camera, trigger, yolo_context)


def _compact_watch_trigger_for_prompt(trigger: dict) -> dict:
    prediction = trigger.get("prediction") if isinstance(trigger, dict) else {}
    if not isinstance(prediction, dict):
        prediction = {}
    worker = prediction.get("worker") if isinstance(prediction.get("worker"), dict) else {}
    measurements = prediction.get("measurements") if isinstance(prediction.get("measurements"), dict) else {}
    health_profile = prediction.get("health_profile") if isinstance(prediction.get("health_profile"), dict) else {}
    return {
        "watch_sensor_id": trigger.get("watch_sensor_id") if isinstance(trigger, dict) else None,
        "worker_id": trigger.get("worker_id") if isinstance(trigger, dict) else None,
        "worker": {
            "dept_id": worker.get("dept_id"),
            "name": worker.get("name"),
            "space_id": worker.get("space_id"),
            "space_name": worker.get("space_name"),
        },
        "rest_result": prediction.get("result"),
        "rest_reason": prediction.get("reason"),
        "rest_reason_detail": prediction.get("rest_reason_detail"),
        "hr": measurements.get("hr"),
        "baseline_hr": measurements.get("baseline_hr") or prediction.get("baseline_hr"),
        "hr_delta_from_baseline": prediction.get("hr_delta_from_baseline"),
        "heat_index": prediction.get("heat_index"),
        "temp_c": measurements.get("temp_c"),
        "humid": measurements.get("humid"),
        "health_profile": {
            "age": health_profile.get("age"),
            "elderly_flag": health_profile.get("elderly_flag"),
            "heart_disease": health_profile.get("heart_disease"),
            "hypertension": health_profile.get("hypertension"),
            "other_disease": health_profile.get("other_disease"),
        },
        "triggered_at": trigger.get("triggered_at") if isinstance(trigger, dict) else None,
    }


def _call_prompt_builder(
    prompt_builder: Callable[..., str],
    camera: dict,
    trigger: dict,
    yolo_context: dict,
) -> str:
    try:
        return prompt_builder(camera, trigger, yolo_context)
    except TypeError as exc:
        try:
            return prompt_builder(camera, trigger)
        except TypeError:
            raise exc


def _format_yolo_context_for_prompt(yolo_context: dict | None) -> str:
    return shared_format_yolo_context_for_prompt(yolo_context)


def _limit_text(text: str, max_chars: int) -> str:
    return shared_limit_text(text, max_chars)


def _limit_prompt_for_vlm(prompt: str) -> str:
    max_chars = int(getattr(settings, "camera_vlm_prompt_max_chars", 6000) or 6000)
    if len(prompt) <= max_chars:
        return prompt

    logger.warning(
        "[WatchCameraVLM] autoregressive VLM prompt truncated chars=%s max_chars=%s",
        len(prompt),
        max_chars,
    )
    return _limit_text(prompt, max_chars)


def _build_retry_prompt(camera: dict, trigger: dict, yolo_context: dict, event_type: str = "watch_camera_vlm") -> str:
    mode = "environment" if event_type == "temperature_camera_vlm" else "worker_regression"
    return shared_build_retry_prompt(camera, trigger, yolo_context, mode=mode)


def _extract_hazard_type(camera: dict, trigger: dict) -> str:
    return shared_extract_hazard_type(camera, trigger)


def _is_prompt_too_long_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "maximum model length" in text
        or "decoder prompt" in text
        or "prompt" in text and "longer" in text
    )


def _normalize_worker_regression_vlm_result(
    result: dict,
    camera: dict,
    last_trigger: dict,
    yolo_context: dict | None = None,
) -> dict:
    normalized = dict(result)
    prediction = last_trigger.get("prediction") if isinstance(last_trigger, dict) else {}
    if not isinstance(prediction, dict):
        prediction = {}
    worker = prediction.get("worker") if isinstance(prediction.get("worker"), dict) else {}
    measurements = prediction.get("measurements") if isinstance(prediction.get("measurements"), dict) else {}
    health_profile = prediction.get("health_profile") if isinstance(prediction.get("health_profile"), dict) else {}

    if normalized.get("risk_level") is not None:
        normalized["risk_level"] = str(normalized.get("risk_level")).strip().lower()

    normalized["target"] = {
        "type": "worker",
        "site_id": worker.get("space_id") or measurements.get("space_id") or camera.get("space_id"),
        "site_name": worker.get("space_name") or measurements.get("space_name") or camera.get("space_name"),
        "worker_id": worker.get("dept_id") or last_trigger.get("worker_id"),
        "worker_name": worker.get("name"),
    }
    normalized.setdefault(
        "reason",
        prediction.get("rest_reason_detail")
        or prediction.get("reason")
        or "워치/밴드 생체 데이터와 회귀 모델 결과를 기반으로 특정 작업자 조치 필요성을 판단했습니다.",
    )
    normalized.setdefault("rest_recommendation", prediction.get("result") or "unknown")
    normalized.setdefault(
        "environment_status",
        {
            "temperature_c": measurements.get("temp_c"),
            "humidity_percent": measurements.get("humid"),
            "heat_index_c": prediction.get("heat_index"),
            "status": "unknown",
        },
    )

    risk_factors = shared_health_risk_factor_names(health_profile)
    normalized.setdefault(
        "health_considerations",
        ", ".join(risk_factors) if risk_factors else "등록된 건강 위험 요인은 없거나 확인되지 않았습니다.",
    )

    recommended_actions = normalized.get("recommended_actions")
    if isinstance(recommended_actions, list) and recommended_actions:
        normalized.setdefault("recommended_action", str(recommended_actions[0]))

    detection_summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if detection_summary and "detection_info" not in normalized:
        normalized["detection_info"] = {
            "source": yolo_context.get("source"),
            "summary": detection_summary,
        }

    normalized.setdefault("worker_location", "same_space_unknown")
    normalized.setdefault("abnormal_behavior", "unknown")
    return normalized


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _compact_camera_for_prompt(camera: dict) -> dict:
    return shared_compact_camera_for_prompt(camera)


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
        "health": camera.get("health"),
        "is_online": camera.get("is_online"),
    }


def _notification_title(event_type: str) -> str:
    if event_type == "temperature_camera_vlm":
        return "고온 구역 CCTV autoregressive VLM 분석 완료"
    if event_type == "watch_camera_vlm":
        return "휴식 권고 구역 CCTV autoregressive VLM 분석 완료"
    return "CCTV autoregressive VLM 분석 완료"


def _camera_id_candidates(camera: dict) -> list[int]:
    candidates = []
    for key in (
        "buffer_camera_id",
        "runtime_camera_id",
        "camera_table_id",
        "camera_pk",
        "cam_id",
        "id",
        "sen_id",
        "sensor_id",
        "camera_id",
    ):
        value = camera.get(key)
        try:
            if value is None or str(value).strip() == "":
                continue
            candidate = int(value)
        except (TypeError, ValueError):
            continue
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
