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

        cameras = self._db_handler.get_cameras_by_space_id(int(space_id))
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
            retry_prompt = _build_retry_prompt(self.camera, self._last_trigger, yolo_context)
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
            return extract_json(text)
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
    path = settings.fire_pipeline_rtsp_path
    if not path.startswith("/"):
        path = f"/{path}"
    username = quote(str(camera.get("camera_id") or ""), safe="")
    password = quote(str(camera.get("camera_pw") or ""), safe="")
    ip_address = camera.get("ip_address")
    return f"rtsp://{username}:{password}@{ip_address}{path}"


def build_common_autoregressive_vlm_prompt(camera: dict, yolo_context: dict | None) -> str:
    return (
        "분석: autoregressive VLM. 현재 CCTV 이미지가 최우선, YOLO 10초 정규화는 이동/위험 흐름 보조입니다.\n"
        "현재 이미지에 안 보이는 사람/객체를 확정하지 마세요.\n"
        "화재/연기/열원은 현재 이미지 또는 YOLO정규화텍스트에 근거가 있을 때만 확정하세요.\n"
        f"카메라={json.dumps(_compact_camera_for_prompt(camera), ensure_ascii=False, default=str)}\n"
        f"{_format_yolo_context_for_prompt(yolo_context)}\n"
    )


def _build_prompt(camera: dict, trigger: dict, yolo_context: dict | None = None) -> str:
    trigger_json = _limit_text(json.dumps(trigger, ensure_ascii=False, default=str), 500)
    return (
        build_common_autoregressive_vlm_prompt(camera, yolo_context)
        + "이벤트별 목적: 휴식 권고가 발생한 작업자와 같은 공간의 작업장 상태 확인입니다.\n"
        "현재 CCTV 화면을 보고 작업장 상태를 JSON 하나로만 답하세요.\n"
        "휴식 권고가 발생한 작업자와 같은 공간의 작업장 상태 확인이 목적입니다.\n"
        "불확실한 내용은 추측하지 말고 unknown 또는 빈 배열로 표시하세요.\n"
        "응답 형식:\n"
        "{"
        "\"summary\":\"현재 작업장 상태 한 문장\","
        "\"risk_level\":\"low|medium|high|unknown\","
        "\"visible_people\":\"none|one|multiple|unknown\","
        "\"visible_risks\":[\"fire\",\"smoke\",\"fall\",\"crowding\",\"unsafe_posture\",\"none\"],"
        "\"recommended_action\":\"필요 조치 한 문장\""
        "}\n"
        f"휴식권고={trigger_json}"
    )


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
    if not yolo_context:
        return "YOLO정규화메타={}\nYOLO정규화텍스트=\n[YOLO compact history]\nmissing"

    public = public_yolo_context(yolo_context)
    metadata = {
        "source": public.get("source"),
        "camera_id": public.get("camera_id"),
        "frame_count": public.get("frame_count"),
        "sampled_count": public.get("sampled_count"),
        "generated_at": public.get("generated_at"),
        "normalized_text_length": public.get("normalized_text_length"),
    }
    text = str(yolo_context.get("normalized_text") or "")
    max_chars = int(getattr(settings, "cctv_vlm_yolo_context_prompt_max_chars", 2600) or 2600)
    return (
        f"YOLO정규화메타={json.dumps(metadata, ensure_ascii=False, default=str)}\n"
        "YOLO정규화텍스트=\n"
        f"{_limit_text(text, max_chars)}"
    )


def _limit_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[: max(0, max_chars - len(marker))] + marker


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


def _build_retry_prompt(camera: dict, trigger: dict, yolo_context: dict) -> str:
    max_chars = int(getattr(settings, "camera_vlm_retry_prompt_max_chars", 900) or 900)
    camera_text = json.dumps(_compact_camera_for_prompt(camera), ensure_ascii=False, default=str)
    trigger_text = _limit_text(json.dumps(trigger, ensure_ascii=False, default=str), 260)
    yolo_text = _limit_text(str(yolo_context.get("normalized_text") or ""), 360)
    hazard_type = _extract_hazard_type(camera, trigger)
    prompt = (
        "분석 명칭: autoregressive VLM.\n"
        "현재 CCTV 이미지가 최우선입니다. YOLO 이력은 보조 근거입니다.\n"
        "화재/연기/열원은 보일 때만 확정하세요. 온도센서 값은 작업자 체온이 아니라 작업장 환경 온도입니다.\n"
        "등록위험물이 있으면 위험물별 경고와 대처를 반드시 포함하세요. 코드블록 금지.\n"
        "JSON 하나만 답하세요: "
        "{\"summary\":\"한 문장\",\"risk_level\":\"low|medium|high|unknown\","
        "\"visible_people\":\"none|one|multiple|unknown\","
        "\"visible_risks\":[\"fire\",\"smoke\",\"unsafe_posture\",\"none\"],"
        "\"hazard_material\":\"위험물명 또는 none\","
        "\"evacuation_route\":\"대피 경로 또는 unknown\","
        "\"hazard_warning\":\"위험물 경고 또는 none\","
        "\"hazard_specific_action\":\"위험물별 대처 한 문장\","
        "\"recommended_action\":\"조치 한 문장\"}\n"
        f"카메라={camera_text}\n"
        f"등록위험물={hazard_type}\n"
        f"이벤트={trigger_text}\n"
        f"YOLO={yolo_text}"
    )
    return _limit_text(prompt, max_chars)


def _extract_hazard_type(camera: dict, trigger: dict) -> str:
    if isinstance(trigger, dict):
        prediction = trigger.get("prediction")
        if isinstance(prediction, dict) and prediction.get("hazard_type"):
            return str(prediction.get("hazard_type"))
        if trigger.get("hazard_type"):
            return str(trigger.get("hazard_type"))
    return str(camera.get("hazard_type") or "none")


def _is_prompt_too_long_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "maximum model length" in text
        or "decoder prompt" in text
        or "prompt" in text and "longer" in text
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _compact_camera_for_prompt(camera: dict) -> dict:
    return {
        "sen_id": camera.get("sen_id"),
        "space_id": camera.get("space_id"),
        "space_name": camera.get("space_name"),
        "hazard_type": camera.get("hazard_type"),
        "is_hazard": _as_bool(camera.get("is_hazard")),
    }


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
