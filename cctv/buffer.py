"""Per-camera rolling frame buffer.

각 카메라가 켜진 뒤 항상 최근 10초 프레임을 보관한다.
위험상황이 발생하면 get_recent_frames(camera_id)로 분석용 프레임을 꺼낼 수 있다.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Any

from cctv.rtsp import get_latest_frame

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SECONDS = 10.0
DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.5


@dataclass
class CameraFrameBuffer:
    camera_id: int
    process_id: int | None = None
    buffer_seconds: float = DEFAULT_BUFFER_SECONDS
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS
    running: bool = False
    thread: threading.Thread | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    frames: deque = field(default_factory=deque)
    last_source_timestamp: float = 0.0
    latest_error: str = ""

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return

        self.running = True
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"frame-buffer-cam{self.camera_id}",
        )
        self.thread.start()
        logger.info(
            "Frame buffer started camera_id=%s process_id=%s seconds=%s",
            self.camera_id,
            self.process_id,
            self.buffer_seconds,
        )

    def stop(self) -> None:
        self.running = False
        logger.info("Frame buffer stop requested camera_id=%s", self.camera_id)

    def status(self) -> dict:
        with self.lock:
            self._prune_locked(time.time())
            latest_age = None

            if self.frames:
                latest_age = time.time() - self.frames[-1]["timestamp"]

            return {
                "camera_id": self.camera_id,
                "process_id": self.process_id,
                "running": self.running,
                "thread_alive": self.thread.is_alive() if self.thread else False,
                "buffer_seconds": self.buffer_seconds,
                "frame_count": len(self.frames),
                "latest_age_seconds": round(latest_age, 3) if latest_age is not None else None,
                "latest_error": self.latest_error,
            }

    def get_frames(self, seconds: float | None = None) -> list[dict]:
        now = time.time()
        limit = seconds or self.buffer_seconds

        with self.lock:
            self._prune_locked(now)
            selected = [
                item
                for item in self.frames
                if now - item["timestamp"] <= limit
            ]

            return [
                {
                    "camera_id": self.camera_id,
                    "process_id": self.process_id,
                    "timestamp": item["timestamp"],
                    "frame": item["frame"].copy(),
                }
                for item in selected
            ]

    def _run(self) -> None:
        while self.running:
            try:
                latest = get_latest_frame(self.camera_id)

                if latest is None:
                    self._set_error("latest_frame_not_ready")
                    time.sleep(self.sample_interval)
                    continue

                source_timestamp, frame = latest

                if source_timestamp <= self.last_source_timestamp:
                    time.sleep(self.sample_interval)
                    continue

                self.last_source_timestamp = source_timestamp

                with self.lock:
                    self.frames.append({
                        "timestamp": source_timestamp,
                        "frame": frame.copy(),
                    })
                    self._prune_locked(time.time())
                    self.latest_error = ""
            except Exception as exc:
                self._set_error(str(exc))
                logger.exception("Frame buffer failed camera_id=%s", self.camera_id)

            time.sleep(self.sample_interval)

    def _set_error(self, message: str) -> None:
        with self.lock:
            self.latest_error = message

    def _prune_locked(self, now: float) -> None:
        while self.frames and now - self.frames[0]["timestamp"] > self.buffer_seconds:
            self.frames.popleft()


_buffers: dict[int, CameraFrameBuffer] = {}
_buffers_lock = threading.Lock()


def start_buffer(
    camera_id: int,
    process_id: int | None = None,
    buffer_seconds: float = DEFAULT_BUFFER_SECONDS,
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
) -> bool:
    """Start or replace a camera rolling buffer.

    Returns True when a new buffer was started, False when one was already
    running with the same process id.
    """
    with _buffers_lock:
        current = _buffers.get(camera_id)

        if current is not None and current.running and current.process_id == process_id:
            logger.info("Frame buffer already running camera_id=%s", camera_id)
            return False

        if current is not None:
            current.stop()

        buffer = CameraFrameBuffer(
            camera_id=camera_id,
            process_id=process_id,
            buffer_seconds=buffer_seconds,
            sample_interval=sample_interval,
        )
        _buffers[camera_id] = buffer
        buffer.start()
        return True


def stop_buffer(camera_id: int) -> bool:
    with _buffers_lock:
        buffer = _buffers.pop(camera_id, None)

    if buffer is None:
        return False

    buffer.stop()
    return True


def get_recent_frames(camera_id: int, seconds: float = DEFAULT_BUFFER_SECONDS) -> list[dict]:
    with _buffers_lock:
        buffer = _buffers.get(camera_id)

    if buffer is None:
        return []

    return buffer.get_frames(seconds)


def get_buffer_status(camera_id: int) -> dict:
    with _buffers_lock:
        buffer = _buffers.get(camera_id)

    if buffer is None:
        return {
            "camera_id": camera_id,
            "process_id": None,
            "running": False,
            "thread_alive": False,
            "buffer_seconds": DEFAULT_BUFFER_SECONDS,
            "frame_count": 0,
            "latest_age_seconds": None,
            "latest_error": "buffer_not_started",
        }

    return buffer.status()


def get_all_buffer_ids() -> list[int]:
    """진단용: 현재 등록된 frame buffer의 camera_id 목록을 반환."""
    with _buffers_lock:
        return list(_buffers.keys())


def stop_all_buffers() -> None:
    with _buffers_lock:
        camera_ids = list(_buffers.keys())

    for camera_id in camera_ids:
        stop_buffer(camera_id)
