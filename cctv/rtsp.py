"""RTSP reader registry.

카메라가 등록되면 카메라별 reader thread가 계속 최신 프레임을 갱신한다.
buffer 모듈은 여기서 최신 프레임을 읽어 최근 10초 롤링 버퍼를 만든다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
import threading
import time
from typing import Any
from urllib.parse import urlparse, unquote

logger = logging.getLogger(__name__)

# 여러 카메라가 동시에 열릴 때 OPENCV_FFMPEG_CAPTURE_OPTIONS 환경변수 경쟁 방지
_cap_open_lock = threading.Lock()


def _split_rtsp_credentials(rtsp_url: str) -> tuple[str, str, str]:
    """RTSP URL에서 자격증명을 분리해 (clean_url, username, password)를 반환한다.

    URL에 포함된 percent-encoded 문자(%40 등)를 디코딩해서 raw 값을 반환한다.
    일부 Jetson FFMPEG 버전이 %40을 @으로 디코딩하지 않아 401이 발생하는 문제를 우회한다.
    """
    parsed = urlparse(rtsp_url)
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")

    if not username and not password:
        return rtsp_url, "", ""

    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    clean_url = f"{parsed.scheme}://{host}{parsed.path}"
    if parsed.query:
        clean_url += f"?{parsed.query}"

    return clean_url, username, password


@dataclass
class RtspReader:
    camera_id: int
    rtsp_url: str
    reconnect_delay: float = 2.0
    running: bool = False
    thread: threading.Thread | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    latest_frame: Any = None
    latest_timestamp: float = 0.0
    latest_error: str = ""
    opened: bool = False

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return

        self.running = True
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"rtsp-reader-cam{self.camera_id}",
        )
        self.thread.start()
        logger.info("RTSP reader started camera_id=%s", self.camera_id)

    def stop(self) -> None:
        self.running = False
        logger.info("RTSP reader stop requested camera_id=%s", self.camera_id)

    def get_latest(self) -> tuple[float, Any] | None:
        with self.lock:
            if self.latest_frame is None:
                return None

            return self.latest_timestamp, self.latest_frame.copy()

    def status(self) -> dict:
        with self.lock:
            has_frame = self.latest_frame is not None
            latest_age = time.time() - self.latest_timestamp if has_frame else None
            return {
                "camera_id": self.camera_id,
                "running": self.running,
                "thread_alive": self.thread.is_alive() if self.thread else False,
                "opened": self.opened,
                "has_frame": has_frame,
                "latest_age_seconds": round(latest_age, 3) if latest_age is not None else None,
                "latest_error": self.latest_error,
            }

    def _set_error(self, message: str) -> None:
        with self.lock:
            self.latest_error = message

    def _set_opened(self, opened: bool) -> None:
        with self.lock:
            self.opened = opened

    def _set_frame(self, frame: Any) -> None:
        with self.lock:
            self.latest_frame = frame.copy()
            self.latest_timestamp = time.time()
            self.latest_error = ""

    def _run(self) -> None:
        try:
            import cv2
        except Exception as exc:
            self._set_error(f"opencv_import_failed: {exc}")
            logger.exception("OpenCV import failed for RTSP reader camera_id=%s", self.camera_id)
            self.running = False
            return

        is_rtsp = str(self.rtsp_url).startswith("rtsp://")
        if is_rtsp:
            cap_url, username, password = _split_rtsp_credentials(self.rtsp_url)
        else:
            cap_url, username, password = self.rtsp_url, "", ""

        while self.running:
            cap = None

            try:
                if is_rtsp:
                    base_opts = "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|max_delay;0"
                    if username or password:
                        # %40 등 인코딩 문자를 raw 값으로 전달해 FFMPEG 인증 오류 방지
                        base_opts += f"|user;{username}|password;{password}"
                    with _cap_open_lock:
                        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = base_opts
                        cap = cv2.VideoCapture(cap_url, cv2.CAP_FFMPEG)
                else:
                    cap = cv2.VideoCapture(cap_url)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                frame_interval = self._frame_interval(cap)

                if not cap.isOpened():
                    self._set_opened(False)
                    self._set_error("rtsp_open_failed")
                    logger.warning(
                        "RTSP open failed camera_id=%s url=%s",
                        self.camera_id, cap_url,
                    )
                    time.sleep(self.reconnect_delay)
                    continue

                self._set_opened(True)
                logger.info("RTSP opened camera_id=%s", self.camera_id)

                while self.running:
                    ok, frame = cap.read()

                    if not ok:
                        self._set_error("rtsp_read_failed")
                        logger.warning("RTSP read failed camera_id=%s", self.camera_id)
                        break

                    self._set_frame(frame)

                    if frame_interval > 0:
                        time.sleep(frame_interval)
            except Exception as exc:
                self._set_error(str(exc))
                logger.exception("RTSP reader failed camera_id=%s", self.camera_id)
            finally:
                self._set_opened(False)

                if cap is not None:
                    cap.release()

            if self.running:
                time.sleep(self.reconnect_delay)

    def _frame_interval(self, cap: Any) -> float:
        """로컬 영상 파일은 FPS에 맞춰 공급해서 실제 영상처럼 재생한다."""
        if str(self.rtsp_url).startswith("rtsp://"):
            return 0.0
        if not os.path.isfile(str(self.rtsp_url)):
            return 0.0

        try:
            fps = float(cap.get(5))  # cv2.CAP_PROP_FPS
        except Exception:
            fps = 0.0
        if fps <= 0:
            fps = 30.0
        return min(max(1.0 / fps, 0.001), 0.2)


_readers: dict[int, RtspReader] = {}
_readers_lock = threading.Lock()


def register_reader(camera_id: int, rtsp_url: str, reconnect_delay: float = 2.0) -> bool:
    """Start or replace the reader for a camera.

    Returns True when a new reader was started, False when an existing reader
    for the same URL was already running.
    """
    with _readers_lock:
        current = _readers.get(camera_id)

        if current is not None and current.rtsp_url == rtsp_url and current.running:
            logger.info("RTSP reader already running camera_id=%s", camera_id)
            return False

        if current is not None:
            current.stop()

        reader = RtspReader(
            camera_id=camera_id,
            rtsp_url=rtsp_url,
            reconnect_delay=reconnect_delay,
        )
        _readers[camera_id] = reader
        reader.start()
        return True


def stop_reader(camera_id: int) -> bool:
    with _readers_lock:
        reader = _readers.pop(camera_id, None)

    if reader is None:
        return False

    reader.stop()
    return True


def get_latest_frame(camera_id: int) -> tuple[float, Any] | None:
    with _readers_lock:
        reader = _readers.get(camera_id)

    if reader is None:
        return None

    return reader.get_latest()


def get_reader_status(camera_id: int) -> dict:
    with _readers_lock:
        reader = _readers.get(camera_id)

    if reader is None:
        return {
            "camera_id": camera_id,
            "running": False,
            "thread_alive": False,
            "opened": False,
            "has_frame": False,
            "latest_age_seconds": None,
            "latest_error": "reader_not_started",
        }

    return reader.status()


def get_all_reader_ids() -> list[int]:
    """진단용: 현재 등록된 RTSP reader의 camera_id 목록을 반환."""
    with _readers_lock:
        return list(_readers.keys())


def stop_all_readers() -> None:
    with _readers_lock:
        camera_ids = list(_readers.keys())

    for camera_id in camera_ids:
        stop_reader(camera_id)
