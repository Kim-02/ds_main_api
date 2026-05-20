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
from core.notifications import extract_vlm_text, make_vlm_push_payload
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

DEFAULT_SESSION_SECONDS = 30


class WatchCameraVlmManager:
    """space_id 기준 카메라 autoregressive VLM 세션을 관리한다."""

    def __init__(
        self,
        db_handler,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        broadcast_fn: Optional[Callable[[dict], Any]] = None,
        session_seconds: int = DEFAULT_SESSION_SECONDS,
        prompt_builder: Optional[Callable[..., str]] = None,
        event_type: str = "watch_camera_vlm",
    ):
        self._db_handler = db_handler
        self._loop = loop
        self._broadcast_fn = broadcast_fn
        self.session_seconds = session_seconds
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
        prompt_builder: Callable[..., str],
        event_type: str,
    ):
        self.camera = camera
        self.manager = manager
        self.session_seconds = session_seconds
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
            self._latest_result = None
            self._latest_error = ""
            self._latest_frame_path = ""
            self._latest_frame_source = ""
            self._latest_yolo_context = {}
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
        """트리거가 올 때만 VLM을 한 번 실행한다.

        고정 인터벌 반복 대신, extend()가 _wake_event를 set할 때마다
        분석을 한 번 수행하고 다시 대기한다. VLM이 바쁘면 timeshare 큐에서
        자연스럽게 대기 후 실행된다.
        """
        analyzed_count = 0
        try:
            while not self._stop_event.is_set():
                if self._expired():
                    break

                with self._lock:
                    current_count = self._trigger_count

                if current_count > analyzed_count:
                    analyzed_count = current_count
                    try:
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
                        self.manager.publish_result(payload)
                        logger.info(
                            "[VLM TEXT] event_type=%s camera_sen_id=%s app_text=%s",
                            self.event_type,
                            self.camera.get("sen_id"),
                            payload.get("text") or payload.get("body") or "",
                        )
                    except Exception as exc:
                        logger.exception(
                            "[WatchCameraVLM] analysis failed camera_sen_id=%s",
                            self.camera.get("sen_id"),
                        )
                        self._set_result(self._latest_result, self._latest_frame_path, str(exc))
                else:
                    # 새 트리거가 올 때까지 대기 (세션 만료 시간 내에서)
                    with self._lock:
                        remaining = max(0.0, self._deadline_monotonic - time.monotonic())
                    self._wake_event.clear()
                    self._wake_event.wait(timeout=remaining if remaining > 0 else 0.1)
        finally:
            with self._lock:
                self._running = False
                self._updated_at = _now_iso()
            logger.info("[WatchCameraVLM] session ended camera_sen_id=%s", self.camera.get("sen_id"))

    def _expired(self) -> bool:
        with self._lock:
            return time.monotonic() >= self._deadline_monotonic

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
            logger.info(
                "[VLM TEXT] event_type=%s camera_sen_id=%s app_text=%s",
                self.event_type,
                self.camera.get("sen_id"),
                payload.get("text") or payload.get("body") or "",
            )
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
        json_system_prompt = (
            "You are a JSON-only industrial safety assistant. "
            "Return exactly one valid JSON object. No markdown, no headings, no code fences, no explanation outside JSON."
        )
        try:
            text = client.request_text(
                prompt,
                frame_path,
                max_tokens=max_tokens,
                temperature=0.0,
                stream=False,
                system_prompt=json_system_prompt,
            )
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
                temperature=0.0,
                stream=False,
                system_prompt=json_system_prompt,
            )
        logger.debug(
            "[VLM RAW] event_type=%s camera_sen_id=%s text=%s",
            self.event_type,
            self.camera.get("sen_id"),
            text,
        )
        try:
            result = extract_json(text)
            if isinstance(result, dict):
                if self.event_type == "temperature_camera_vlm":
                    return result
                return _normalize_worker_regression_vlm_result(result, self.camera, self._last_trigger, yolo_context)
            return result
        except Exception as exc:
            logger.warning("[WatchCameraVLM] JSON response parse failed: %s", exc)
            return _fallback_structured_vlm_result(
                text,
                self.camera,
                self._last_trigger,
                yolo_context,
                event_type=self.event_type,
            )

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
    rest_result = str(prediction.get("result") or "").strip()
    worker_name = str(worker.get("name") or "").strip()

    if normalized.get("risk_level") is not None:
        normalized["risk_level"] = str(normalized.get("risk_level")).strip().lower()
    rest_risk_level = _risk_level_from_rest_result(rest_result)
    if rest_risk_level and _risk_rank(normalized.get("risk_level")) < _risk_rank(rest_risk_level):
        normalized["risk_level"] = rest_risk_level

    normalized["target"] = {
        "type": "worker",
        "site_id": worker.get("space_id") or measurements.get("space_id") or camera.get("space_id"),
        "site_name": worker.get("space_name") or measurements.get("space_name") or camera.get("space_name"),
        "worker_id": worker.get("dept_id") or last_trigger.get("worker_id"),
        "worker_name": worker_name or None,
    }
    rest_reason = (
        prediction.get("rest_reason_detail")
        or prediction.get("reason")
        or "워치/밴드 생체 데이터와 회귀 모델 결과를 기반으로 특정 작업자 조치 필요성을 판단했습니다."
    )
    vlm_reason = str(normalized.get("reason") or "").strip()
    if rest_result:
        if _reason_conflicts_with_rest(vlm_reason):
            normalized["reason"] = str(rest_reason)
        elif vlm_reason and str(rest_reason) not in vlm_reason:
            normalized["reason"] = f"{rest_reason} / CCTV 참고: {vlm_reason}"
        else:
            normalized["reason"] = str(rest_reason)
    else:
        normalized.setdefault("reason", str(rest_reason))

    normalized["rest_recommendation"] = rest_result or "unknown"
    normalized["environment_status"] = {
        "temperature_c": measurements.get("temp_c"),
        "humidity_percent": measurements.get("humid"),
        "heat_index_c": prediction.get("heat_index"),
        "status": (
            "danger"
            if rest_risk_level in {"high", "critical"}
            else ("caution" if rest_risk_level == "medium" else "unknown")
        ),
    }

    risk_factors = shared_health_risk_factor_names(health_profile)
    health_text = (
        "등록 건강 유의요인: " + ", ".join(risk_factors)
        if risk_factors
        else "등록된 건강 위험 요인은 없거나 확인되지 않았습니다."
    )
    if not _has_meaningful_worker_text(normalized.get("health_considerations")) or risk_factors:
        normalized["health_considerations"] = health_text

    summary = str(normalized.get("summary") or "").strip()
    if rest_result and _summary_conflicts_with_rest(summary, worker_name):
        normalized["summary"] = _worker_rest_summary(worker_name, rest_result)

    recommended_actions = _as_action_list(normalized.get("recommended_actions"))
    if rest_result:
        recommended_actions = _merge_actions(
            _default_worker_actions(rest_result, worker_name),
            recommended_actions,
        )
        normalized["recommended_actions"] = recommended_actions
        normalized["recommended_action"] = recommended_actions[0]
    elif recommended_actions:
        normalized["recommended_actions"] = recommended_actions
        normalized.setdefault("recommended_action", recommended_actions[0])

    detection_summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if detection_summary and "detection_info" not in normalized:
        normalized["detection_info"] = {
            "source": yolo_context.get("source"),
            "summary": detection_summary,
        }

    normalized.setdefault("worker_location", "same_space_unknown")
    normalized.setdefault("abnormal_behavior", "not_visible")
    normalized.setdefault("behavior_observation", {
        "posture": "unknown",
        "movement_pattern": "unknown",
        "confidence": "low",
        "detail": "화면에서 자세·이동 패턴을 확인할 수 없습니다.",
    })
    normalized.setdefault(
        "person_identification",
        _default_person_identification(yolo_context, target_worker_match="unknown"),
    )

    # 비틀거림·낙상 탐지 시 risk_level 강제 상향
    abnormal = str(normalized.get("abnormal_behavior") or "").strip().lower()
    if abnormal in {"staggering", "falling"}:
        if _risk_rank(normalized.get("risk_level")) < _risk_rank("high"):
            normalized["risk_level"] = "high"
        actions = normalized.get("recommended_actions")
        if not isinstance(actions, list):
            actions = []
        fall_action = "즉시 현장 확인 — 작업자 비틀거림·낙상 가능성이 감지되었습니다."
        if fall_action not in actions:
            normalized["recommended_actions"] = [fall_action] + actions

    return normalized


def _risk_level_from_rest_result(rest_result: str) -> str:
    text = str(rest_result or "")
    if "반드시" in text:
        return "critical"
    if "강한" in text:
        return "high"
    if "약한" in text:
        return "medium"
    return ""


def _risk_rank(level: Any) -> int:
    return {
        "": 0,
        "unknown": 0,
        "low": 1,
        "medium": 2,
        "high": 3,
        "critical": 4,
    }.get(str(level or "").strip().lower(), 0)


def _has_meaningful_worker_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text not in {"", "none", "unknown", "null", "false", "0", "no", "no_action", "없음"}


def _reason_conflicts_with_rest(reason: str) -> bool:
    text = str(reason or "")
    return (
        not _has_meaningful_worker_text(text)
        or "위험이 없" in text
        or "위험도가 낮" in text
        or "조치 없음" in text
        or "no_action" in text
    )


def _summary_conflicts_with_rest(summary: str, worker_name: str) -> bool:
    text = str(summary or "")
    if not _has_meaningful_worker_text(text):
        return True
    if worker_name and worker_name in text:
        return False
    if "작업자" in text or "근로자" in text:
        return False
    return any(phrase in text for phrase in ("위험이 없습니다", "위험도가 낮", "사람이나 물체가 보이지"))


def _worker_rest_summary(worker_name: str, rest_result: str) -> str:
    subject = f"{worker_name} 작업자" if worker_name else "해당 작업자"
    return f"{subject}에게 {rest_result}가 발생했으며, CCTV에서 직접 특정되지 않더라도 관리자 확인과 휴식 조치가 필요합니다."


def _default_worker_actions(rest_result: str, worker_name: str) -> list[str]:
    subject = f"{worker_name} 작업자" if worker_name else "해당 작업자"
    if "반드시" in str(rest_result):
        return [
            f"{subject}는 즉시 작업을 중단하고 안전한 장소에서 휴식합니다.",
            "관리자는 상태를 직접 확인하고 필요 시 응급 조치 또는 보호자/담당자 연락을 진행합니다.",
        ]
    return [
        f"{subject}는 작업 강도를 낮추고 물 섭취와 휴식을 취합니다.",
        "관리자는 작업자 위치와 상태를 확인하고 필요 시 작업에서 일시 제외합니다.",
    ]


def _as_action_list(value: Any) -> list[str]:
    if isinstance(value, list):
        items = value
    elif value:
        items = [value]
    else:
        items = []
    cleaned = []
    for item in items:
        text = str(item or "").strip()
        if not _has_meaningful_worker_text(text):
            continue
        if text not in cleaned:
            cleaned.append(text)
    return cleaned


def _merge_actions(primary: list[str], secondary: list[str]) -> list[str]:
    merged = []
    for item in primary + secondary:
        text = str(item or "").strip()
        if text and text not in merged:
            merged.append(text)
    return merged[:4]


def _fallback_structured_vlm_result(
    raw_text: str,
    camera: dict,
    last_trigger: dict,
    yolo_context: dict | None,
    *,
    event_type: str,
) -> dict:
    """VLM이 마크다운/자연어를 반환해도 앱에는 구조화된 JSON만 보낸다."""
    if event_type == "temperature_camera_vlm":
        return _fallback_environment_vlm_result(raw_text, camera, last_trigger, yolo_context)
    return _normalize_worker_regression_vlm_result(
        _fallback_worker_vlm_result(raw_text, camera, last_trigger, yolo_context),
        camera,
        last_trigger,
        yolo_context,
    )


def _fallback_worker_vlm_result(
    raw_text: str,
    camera: dict,
    last_trigger: dict,
    yolo_context: dict | None,
) -> dict:
    prediction = last_trigger.get("prediction") if isinstance(last_trigger, dict) else {}
    if not isinstance(prediction, dict):
        prediction = {}
    worker = prediction.get("worker") if isinstance(prediction.get("worker"), dict) else {}
    measurements = prediction.get("measurements") if isinstance(prediction.get("measurements"), dict) else {}
    rest_result = str(prediction.get("result") or "unknown").strip()
    worker_name = str(worker.get("name") or "").strip()
    subject = f"{worker_name} 작업자" if worker_name else "해당 작업자"
    detection_text = _detection_text_from_yolo(yolo_context)
    return {
        "risk_level": _risk_level_from_rest_result(rest_result) or "unknown",
        "summary": _worker_rest_summary(worker_name, rest_result) if rest_result != "unknown" else "워치/밴드 기반 작업자 상태 확인이 필요합니다.",
        "reason": prediction.get("rest_reason_detail") or prediction.get("reason") or "VLM 응답이 JSON이 아니어서 회귀 판단 결과를 기준으로 구조화했습니다.",
        "health_considerations": "",
        "recommended_actions": _default_worker_actions(rest_result, worker_name),
        "target": {
            "type": "worker",
            "site_id": worker.get("space_id") or measurements.get("space_id") or camera.get("space_id"),
            "site_name": worker.get("space_name") or measurements.get("space_name") or camera.get("space_name"),
            "worker_id": worker.get("dept_id") or last_trigger.get("worker_id"),
            "worker_name": worker_name or None,
        },
        "visible_people": _visible_people_from_yolo(yolo_context),
        "person_actions": ["unknown"],
        "worker_location": "same_space_unknown",
        "person_identification": _default_person_identification(
            yolo_context,
            target_worker_match="unknown",
        ),
        "abnormal_behavior": "unknown",
        "visible_risks": _visible_risks_from_yolo(yolo_context),
        "environment_status": {
            "temperature_c": measurements.get("temp_c"),
            "humidity_percent": measurements.get("humid"),
            "heat_index_c": prediction.get("heat_index"),
            "status": "unknown",
        },
        "detection_info": detection_text,
        "hazard_material": camera.get("hazard_type") or "none",
        "hazard_warning": "등록 위험물은 공간 속성이며 화면상 누출/화재가 확인되지 않으면 단정하지 않습니다.",
        "hazard_specific_action": "관리자는 현장 상태와 작업자 위치를 확인합니다.",
        "evacuation_route": "unknown",
        "recommended_action": f"{subject} 상태를 확인하고 휴식 조치를 진행합니다.",
        "vlm_parse_error": True,
        "raw_text_preview": _limit_text(str(raw_text or ""), 220),
    }


def _fallback_environment_vlm_result(
    raw_text: str,
    camera: dict,
    last_trigger: dict,
    yolo_context: dict | None,
) -> dict:
    prediction = last_trigger.get("prediction") if isinstance(last_trigger, dict) else {}
    if not isinstance(prediction, dict):
        prediction = {}
    sample = prediction.get("sample") if isinstance(prediction.get("sample"), dict) else {}
    health_attention = (
        prediction.get("health_attention")
        if isinstance(prediction.get("health_attention"), dict)
        else {}
    )
    return {
        "risk_level": "medium" if prediction.get("temperature_sensor_id") else "unknown",
        "summary": f"{prediction.get('space_name') or camera.get('space_name') or '작업장'} 현장 상태 확인이 필요합니다.",
        "reason": "VLM 응답이 JSON이 아니어서 온습도 이벤트와 YOLO 탐지 요약을 기준으로 구조화했습니다.",
        "health_considerations": health_attention.get("summary") or "현장 건강 취약 작업자 정보 확인 필요",
        "recommended_actions": ["관리자는 현장 온습도와 작업자 상태를 확인합니다.", "작업자는 이상 증상이 있으면 즉시 보고하고 안전한 위치에서 대기합니다."],
        "target": {
            "type": "site",
            "site_id": prediction.get("space_id") or camera.get("space_id"),
            "site_name": prediction.get("space_name") or camera.get("space_name"),
            "worker_id": None,
            "worker_name": None,
        },
        "visible_people": _visible_people_from_yolo(yolo_context),
        "worker_movements": _detection_text_from_yolo(yolo_context),
        "person_identification": _default_person_identification(yolo_context),
        "abnormal_behavior": "not_visible",
        "field_status": "VLM 응답 오류로 현장 상태를 자동 확인하세요.",
        "health_risk_summary": health_attention.get("summary") or "현장 건강 취약 작업자 정보 확인 필요",
        "worker_risks": [],
        "visible_risks": _visible_risks_from_yolo(yolo_context),
        "environment_status": {
            "temperature_c": sample.get("temp"),
            "humidity_percent": sample.get("humid"),
            "heat_index_c": prediction.get("heat_index"),
            "status": "caution",
        },
        "detection_info": _detection_text_from_yolo(yolo_context),
        "hazard_material": prediction.get("hazard_type") or camera.get("hazard_type") or "none",
        "hazard_warning": "등록 위험물은 공간 속성이며 화면상 누출/화재가 확인되지 않으면 단정하지 않습니다.",
        "hazard_specific_action": "관리자는 현장 접근을 제한하고 필요 시 환기와 대피 경로를 확인합니다.",
        "evacuation_route": "unknown",
        "recommended_action": "관리자는 현장 상태와 작업자 이상 여부를 확인합니다.",
        "vlm_parse_error": True,
        "raw_text_preview": _limit_text(str(raw_text or ""), 220),
    }


def _visible_people_from_yolo(yolo_context: dict | None) -> str:
    summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if isinstance(summary, dict) and summary.get("visible_people"):
        return str(summary.get("visible_people"))
    return "unknown"


def _visible_risks_from_yolo(yolo_context: dict | None) -> list[str]:
    summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    risks = summary.get("visible_risks") if isinstance(summary, dict) else None
    if isinstance(risks, list) and risks:
        return [str(item) for item in risks]
    return ["unknown"]


def _detection_text_from_yolo(yolo_context: dict | None) -> str:
    summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if isinstance(summary, dict) and summary.get("text"):
        return str(summary.get("text"))
    public = public_yolo_context(yolo_context or {})
    if public.get("normalized_text_preview"):
        return str(public.get("normalized_text_preview"))
    return "YOLO 탐지 요약 없음"


def _default_person_identification(
    yolo_context: dict | None,
    *,
    target_worker_match: str | None = None,
) -> dict:
    visible_people = _visible_people_from_yolo(yolo_context)
    movement = "unknown"
    position = "unknown"
    summary = yolo_context.get("detection_summary") if isinstance(yolo_context, dict) else {}
    if isinstance(summary, dict):
        person_movement = summary.get("person_movement") if isinstance(summary.get("person_movement"), dict) else {}
        movement = str(person_movement.get("direction") or ("none" if visible_people == "none" else "unknown"))
        current = summary.get("current") if isinstance(summary.get("current"), dict) else {}
        person = current.get("person") if isinstance(current.get("person"), dict) else {}
        position = str(person.get("position") or ("none" if visible_people == "none" else "unknown"))

    result = {
        "moving_people": [],
        "note": "화면에서 사람의 옷 색/보호구 구분 단서를 확인하지 못했습니다.",
    }
    if target_worker_match is not None:
        result["target_worker_match"] = target_worker_match

    if visible_people != "none":
        result["moving_people"].append({
            "label": "사람1",
            "clothing": "unknown",
            "ppe": "unknown",
            "position": position,
            "movement": movement,
            "confidence": "low",
        })
        result["note"] = "YOLO는 사람 위치/이동만 제공하며 옷 색은 VLM 화면 분석이 필요합니다."

    return result


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
