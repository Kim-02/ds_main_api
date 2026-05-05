"""RTSP 스트림 리더."""
import asyncio
import logging

import cv2
import numpy as np

logger = logging.getLogger(__name__)


class RTSPReader:
    def __init__(self, camera_id: int, rtsp_url: str):
        self.camera_id = camera_id
        self.rtsp_url = rtsp_url
        self._cap: cv2.VideoCapture | None = None
        self._running = False

    def _open(self) -> bool:
        self._cap = cv2.VideoCapture(self.rtsp_url)
        self._cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        return self._cap.isOpened()

    def read_frame_sync(self) -> np.ndarray | None:
        if self._cap is None or not self._cap.isOpened():
            return None
        ret, frame = self._cap.read()
        return frame if ret else None

    async def read_frame(self) -> np.ndarray | None:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.read_frame_sync)

    async def save_frame(self, frame: np.ndarray, filename: str) -> str:
        from cctv.frame_processing import save_frame

        return await save_frame(frame, filename)

    def start(self) -> None:
        if not self._open():
            logger.error("Cannot open RTSP stream: %s", self.rtsp_url)
            return
        self._running = True

    def stop(self) -> None:
        self._running = False
        if self._cap:
            self._cap.release()

    @property
    def is_running(self) -> bool:
        return self._running


_registry: dict[int, RTSPReader] = {}


def get_reader(camera_id: int) -> RTSPReader | None:
    return _registry.get(camera_id)


def register_reader(camera_id: int, rtsp_url: str) -> RTSPReader:
    if camera_id in _registry:
        return _registry[camera_id]
    reader = RTSPReader(camera_id, rtsp_url)
    reader.start()
    _registry[camera_id] = reader
    return reader


def stop_reader(camera_id: int) -> None:
    reader = _registry.pop(camera_id, None)
    if reader:
        reader.stop()
