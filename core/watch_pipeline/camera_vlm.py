"""Watch-triggered workplace VLM sessions.

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
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

from config import settings
from core.notifications import make_vlm_push_payload

logger = logging.getLogger(__name__)

DEFAULT_SESSION_SECONDS = 120
DEFAULT_ANALYSIS_INTERVAL_SECONDS = 30
DEFAULT_FRAME_DIR = Path("watch_vlm_frames")


class WatchCameraVlmManager:
    """space_id 기준 카메라 VLM 세션을 관리한다."""

    def __init__(
        self,
        db_handler,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        broadcast_fn: Optional[Callable[[dict], Any]] = None,
        session_seconds: int = DEFAULT_SESSION_SECONDS,
        analysis_interval_seconds: int = DEFAULT_ANALYSIS_INTERVAL_SECONDS,
        prompt_builder: Optional[Callable[[dict, dict], str]] = None,
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
        prompt_builder: Callable[[dict, dict], str],
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
                "started_at": self._started_at,
                "updated_at": self._updated_at,
            }

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                if self._expired():
                    break

                try:
                    frame_path = self._capture_frame()
                    result = self._request_vlm(frame_path)
                    self._set_result(result, frame_path, "")
                    self.manager.publish_result(make_vlm_push_payload(
                        self.event_type,
                        _notification_title(self.event_type),
                        result,
                        camera=_public_camera(self.camera),
                        frame_path=frame_path,
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

    def _capture_frame(self) -> str:
        frame_path = self._capture_frame_from_buffer()
        if frame_path:
            return frame_path

        logger.info(
            "[WatchCameraVLM] buffer frame unavailable, fallback to RTSP camera_sen_id=%s candidates=%s",
            self.camera.get("sen_id"),
            _camera_id_candidates(self.camera),
        )
        return self._capture_frame_from_rtsp()

    def _capture_frame_from_buffer(self) -> str | None:
        try:
            from cctv.buffer import get_recent_frames
        except Exception as exc:
            logger.warning("[WatchCameraVLM] cctv.buffer unavailable: %s", exc)
            return None

        buffer_seconds = float(getattr(settings, "frame_buffer_seconds", 10) or 10)
        for camera_id in _camera_id_candidates(self.camera):
            frames = get_recent_frames(camera_id, seconds=buffer_seconds)
            if not frames:
                continue

            latest = frames[-1]
            frame_path = self._write_frame(latest["frame"], source=f"buffer_{camera_id}")
            with self._lock:
                self._latest_frame_source = f"buffer:{camera_id}"
            logger.info(
                "[WatchCameraVLM] captured frame from buffer camera_sen_id=%s buffer_camera_id=%s frame_count=%s",
                self.camera.get("sen_id"),
                camera_id,
                len(frames),
            )
            return frame_path

        return None

    def _capture_frame_from_rtsp(self) -> str:
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

        frame_path = self._write_frame(frame, source="rtsp")
        with self._lock:
            self._latest_frame_source = "rtsp"
        return frame_path

    def _write_frame(self, frame: Any, *, source: str) -> str:
        import cv2

        DEFAULT_FRAME_DIR.mkdir(parents=True, exist_ok=True)
        filename = (
            f"space_{self.camera.get('space_id')}_cam_{self.camera.get('sen_id')}_"
            f"{source}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.jpg"
        )
        frame_path = DEFAULT_FRAME_DIR / filename
        if not cv2.imwrite(str(frame_path), frame):
            raise RuntimeError(f"Frame write failed: {frame_path}")
        return str(frame_path)

    def _request_vlm(self, frame_path: str) -> Any:
        from ai.vlm.client import OpenAiCompatibleVlm, extract_json

        client = OpenAiCompatibleVlm(
            settings.vllm_base_url,
            settings.vllm_api_key,
            settings.vllm_model,
            timeout=settings.camera_vlm_request_timeout_seconds,
        )
        prompt = self.prompt_builder(self.camera, self._last_trigger)
        text = client.request_text(prompt, frame_path, max_tokens=512, temperature=0.2, stream=False)
        try:
            return extract_json(text)
        except Exception as exc:
            logger.warning("[WatchCameraVLM] JSON response parse failed: %s", exc)
            return {"raw_text": text}

    def _set_result(self, result: Any, frame_path: str, error: str) -> None:
        with self._lock:
            self._latest_result = result
            self._latest_error = error
            self._latest_frame_path = frame_path
            self._updated_at = _now_iso()


def _build_rtsp_url(camera: dict) -> str:
    path = settings.fire_pipeline_rtsp_path
    if not path.startswith("/"):
        path = f"/{path}"
    username = quote(str(camera.get("camera_id") or ""), safe="")
    password = quote(str(camera.get("camera_pw") or ""), safe="")
    ip_address = camera.get("ip_address")
    return f"rtsp://{username}:{password}@{ip_address}{path}"


def _build_prompt(camera: dict, trigger: dict) -> str:
    trigger_json = json.dumps(trigger, ensure_ascii=False)
    return (
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
        f"카메라={_public_camera(camera)}\n"
        f"휴식권고={trigger_json}"
    )


def _public_camera(camera: dict) -> dict:
    return {
        "sen_id": camera.get("sen_id"),
        "sensor_id": camera.get("sensor_id"),
        "sen_name": camera.get("sen_name"),
        "space_id": camera.get("space_id"),
        "space_name": camera.get("space_name"),
        "ip_address": camera.get("ip_address"),
        "health": camera.get("health"),
        "is_online": camera.get("is_online"),
    }


def _notification_title(event_type: str) -> str:
    if event_type == "temperature_camera_vlm":
        return "고온 구역 CCTV VLM 분석 완료"
    if event_type == "watch_camera_vlm":
        return "휴식 권고 구역 CCTV VLM 분석 완료"
    return "CCTV VLM 분석 완료"


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
