"""Frame persistence and YOLO analysis helpers."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

DEFAULT_FRAMES_DIR = Path("data/frames")
ANOMALY_LABELS = {"fire", "flame", "smoke", "spark"}


@dataclass(frozen=True)
class ProcessedFrame:
    frame_path: str
    yolo_detections: dict
    is_anomaly: bool


def build_frame_filename(
    camera_id: int,
    *,
    captured_at: datetime | None = None,
    prefix: str = "cam",
) -> str:
    captured_at = captured_at or datetime.now(timezone.utc)
    timestamp = captured_at.strftime("%Y%m%d_%H%M%S_%f")
    return f"{prefix}{camera_id}_{timestamp}.jpg"


async def save_frame(
    frame: np.ndarray,
    filename: str,
    *,
    frames_dir: Path = DEFAULT_FRAMES_DIR,
) -> str:
    frames_dir.mkdir(parents=True, exist_ok=True)
    path = frames_dir / filename
    _write_frame(path, frame)
    return str(path)


async def save_and_analyze_frame(
    frame: np.ndarray,
    camera_id: int,
    *,
    captured_at: datetime | None = None,
    frames_dir: Path = DEFAULT_FRAMES_DIR,
) -> ProcessedFrame:
    from ai.yolo.inference import analyze_frame

    filename = build_frame_filename(camera_id, captured_at=captured_at)
    frame_path = await save_frame(frame, filename, frames_dir=frames_dir)
    detections = await analyze_frame(frame)
    return ProcessedFrame(
        frame_path=frame_path,
        yolo_detections=detections,
        is_anomaly=has_anomaly_label(detections),
    )


def has_anomaly_label(detections: dict | None) -> bool:
    if not detections:
        return False
    labels = {d.get("label", "").lower() for d in detections.get("objects", [])}
    return bool(labels & ANOMALY_LABELS)


def _write_frame(path: Path, frame: np.ndarray) -> None:
    from PIL import Image

    image_data = _to_image_data(frame)
    image = Image.fromarray(image_data)
    save_kwargs = {"quality": 90} if path.suffix.lower() in {".jpg", ".jpeg"} else {}
    image.save(path, **save_kwargs)


def _to_image_data(frame: np.ndarray) -> np.ndarray:
    if frame.dtype != np.uint8:
        frame = np.clip(frame, 0, 255).astype(np.uint8)

    if frame.ndim == 2:
        return np.ascontiguousarray(frame)

    if frame.ndim != 3:
        raise ValueError(f"Unsupported frame shape: {frame.shape}")

    channels = frame.shape[2]
    if channels == 3:
        return np.ascontiguousarray(frame[:, :, ::-1])
    if channels == 4:
        return np.ascontiguousarray(frame[:, :, [2, 1, 0, 3]])

    raise ValueError(f"Unsupported frame channel count: {channels}")
