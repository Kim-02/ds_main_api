import cv2
import os
import threading
import time
from urllib.parse import urlparse, unquote

_cap_open_lock = threading.Lock()


def _split_rtsp_credentials(url: str) -> tuple[str, str, str]:
    parsed = urlparse(url)
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    if not username and not password:
        return url, "", ""
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    clean = f"{parsed.scheme}://{host}{parsed.path}"
    if parsed.query:
        clean += f"?{parsed.query}"
    return clean, username, password


class VideoSource:
    def __init__(self, source):
        self.source = source
        self.cap = None
        self.start_time = 0

    def is_live_source(self):
        return str(self.source).startswith("rtsp://")

    def open(self):
        if self.is_live_source():
            cap_url, username, password = _split_rtsp_credentials(self.source)
            base_opts = "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|max_delay;0"
            if username or password:
                base_opts += f"|user;{username}|password;{password}"
            with _cap_open_lock:
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = base_opts
                self.cap = cv2.VideoCapture(cap_url, cv2.CAP_FFMPEG)
        else:
            self.cap = cv2.VideoCapture(self.source)
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self.start_time = time.monotonic()
        return self.cap.isOpened()

    def read(self):
        return self.cap.read()

    def get_fps(self):
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        return fps if fps != 0 else 30

    def get_time(self):
        if self.is_live_source():
            return time.monotonic() - self.start_time
        current_time = self.cap.get(cv2.CAP_PROP_POS_MSEC) / 1000
        if current_time == 0:
            return time.monotonic() - self.start_time
        return current_time

    def release(self):
        if self.cap is not None:
            self.cap.release()
