"""Runtime preview window for images sent to the autoregressive VLM."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import queue
import threading
from typing import Any, Iterator
from uuid import uuid4

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class _PreviewHandle:
    request_id: str

    def close(self) -> None:
        _manager().close(self.request_id)


class _NullPreviewHandle:
    def close(self) -> None:
        return


class _VlmPreviewManager:
    def __init__(self) -> None:
        self._commands: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._disabled = False

    def show(self, image_path: str, label: str) -> _PreviewHandle | _NullPreviewHandle:
        if self._disabled or not _preview_enabled(image_path):
            return _NullPreviewHandle()

        request_id = uuid4().hex
        self._ensure_thread()
        self._commands.put(("show", {
            "request_id": request_id,
            "image_path": image_path,
            "label": label,
        }))
        return _PreviewHandle(request_id)

    def close(self, request_id: str) -> None:
        if not request_id or self._disabled:
            return
        self._commands.put(("close", {"request_id": request_id}))

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="vlm-preview-window",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        try:
            import cv2
        except Exception as exc:
            self._disabled = True
            logger.warning("[VLM_PREVIEW] OpenCV GUI unavailable: %s", exc)
            return

        window_name = str(getattr(settings, "vlm_preview_window_name", "autoregressive VLM preview"))
        active: dict[str, dict[str, Any]] = {}
        current_id = ""
        window_open = False

        while True:
            try:
                command, payload = self._commands.get(timeout=0.05)
            except queue.Empty:
                command = ""
                payload = {}

            if command == "show":
                request_id = str(payload.get("request_id") or "")
                image_path = str(payload.get("image_path") or "")
                label = str(payload.get("label") or "autoregressive VLM")
                image = self._read_preview_image(cv2, image_path, label)
                if image is not None:
                    active[request_id] = {
                        "image": image,
                        "label": label,
                        "image_path": image_path,
                    }
                    current_id = request_id
                    window_open = self._show_image(cv2, window_name, image, window_open)
                    logger.info("[VLM_PREVIEW] show label=%s image=%s", label, image_path)
                self._commands.task_done()

            elif command == "close":
                request_id = str(payload.get("request_id") or "")
                active.pop(request_id, None)
                if current_id == request_id:
                    current_id = next(reversed(active), "") if active else ""
                if current_id:
                    window_open = self._show_image(cv2, window_name, active[current_id]["image"], window_open)
                elif window_open:
                    self._destroy_window(cv2, window_name)
                    window_open = False
                    logger.info("[VLM_PREVIEW] close")
                self._commands.task_done()

            if window_open:
                try:
                    cv2.waitKey(50)
                except Exception as exc:
                    self._disabled = True
                    logger.warning("[VLM_PREVIEW] waitKey failed, disabling preview: %s", exc)
                    self._destroy_window(cv2, window_name)
                    return

    def _read_preview_image(self, cv2: Any, image_path: str, label: str):
        path = Path(image_path)
        if not path.exists():
            logger.warning("[VLM_PREVIEW] image not found path=%s", image_path)
            return None

        image = cv2.imread(str(path))
        if image is None:
            logger.warning("[VLM_PREVIEW] image read failed path=%s", image_path)
            return None

        image = self._fit_window(cv2, image)
        cv2.putText(
            image,
            "autoregressive VLM analyzing...",
            (20, 34),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (0, 255, 255),
            2,
            cv2.LINE_AA,
        )
        cv2.putText(
            image,
            label[:80],
            (20, 68),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.55,
            (255, 255, 255),
            1,
            cv2.LINE_AA,
        )
        return image

    def _fit_window(self, cv2: Any, image):
        max_width = int(getattr(settings, "vlm_preview_max_width", 960) or 960)
        max_height = int(getattr(settings, "vlm_preview_max_height", 720) or 720)
        height, width = image.shape[:2]
        scale = min(max_width / float(width), max_height / float(height), 1.0)
        if scale >= 1.0:
            return image
        return cv2.resize(
            image,
            (max(1, int(width * scale)), max(1, int(height * scale))),
            interpolation=cv2.INTER_AREA,
        )

    def _show_image(self, cv2: Any, window_name: str, image, window_open: bool) -> bool:
        try:
            if not window_open:
                cv2.namedWindow(window_name, cv2.WINDOW_NORMAL)
            cv2.imshow(window_name, image)
            cv2.waitKey(1)
            return True
        except Exception as exc:
            self._disabled = True
            logger.warning("[VLM_PREVIEW] imshow failed, disabling preview: %s", exc)
            self._destroy_window(cv2, window_name)
            return False

    def _destroy_window(self, cv2: Any, window_name: str) -> None:
        try:
            cv2.destroyWindow(window_name)
            cv2.waitKey(1)
        except Exception:
            logger.debug("[VLM_PREVIEW] destroyWindow failed", exc_info=True)


_manager_instance: _VlmPreviewManager | None = None
_manager_lock = threading.Lock()


def _manager() -> _VlmPreviewManager:
    global _manager_instance
    with _manager_lock:
        if _manager_instance is None:
            _manager_instance = _VlmPreviewManager()
        return _manager_instance


def _preview_enabled(image_path: str) -> bool:
    if not bool(getattr(settings, "vlm_preview_enabled", True)):
        return False
    if not image_path:
        return False
    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        logger.debug("[VLM_PREVIEW] skipped: DISPLAY/WAYLAND_DISPLAY not set")
        return False
    return True


@contextmanager
def vlm_preview(image_path: str, label: str) -> Iterator[None]:
    handle = _manager().show(image_path, label)
    try:
        yield
    finally:
        handle.close()
