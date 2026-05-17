"""Shared YOLO-normalized context for CCTV autoregressive VLM requests."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import logging
from pathlib import Path
import threading
import time
from typing import Any, Iterable

from config import settings

logger = logging.getLogger(__name__)

DETECT_CLASSES = ["person", "fire", "smoke"]
DANGER_CLASSES = ["fire", "smoke"]

_detector_lock = threading.Lock()
_detector = None
_detector_key: tuple[str, float | None] | None = None


@dataclass
class YoloNormalizedContext:
    """Normalized YOLO history attached to every CCTV autoregressive VLM call."""

    normalized_text: str
    image_path: str
    source: str
    camera_id: int | None
    frame_count: int
    sampled_count: int
    saved_frame_count: int
    yolo_model_path: str
    generated_at: str
    detection_summary: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def public(self, *, include_text: bool = False) -> dict:
        data = {
            "source": self.source,
            "camera_id": self.camera_id,
            "frame_count": self.frame_count,
            "sampled_count": self.sampled_count,
            "saved_frame_count": self.saved_frame_count,
            "image_path": self.image_path,
            "generated_at": self.generated_at,
            "normalized_text_length": len(self.normalized_text or ""),
            "normalized_text_preview": _limit_text(self.normalized_text or "", 300),
            "detection_summary": self.detection_summary,
            "yolo_model_path": self.yolo_model_path,
            "error": self.error,
        }
        if include_text:
            data["normalized_text"] = self.normalized_text
        return data


def build_yolo_normalized_context_from_buffer(
    camera: dict,
    camera_id_candidates: Iterable[int],
) -> YoloNormalizedContext | None:
    """Read the last configured seconds from the rolling buffer and normalize it."""
    try:
        from cctv.buffer import get_recent_frames
    except Exception as exc:
        logger.warning("[CCTV_YOLO_CONTEXT] cctv.buffer unavailable: %s", exc)
        return None

    seconds = _context_seconds()
    for camera_id in camera_id_candidates:
        frames = get_recent_frames(camera_id, seconds=seconds)
        if not frames:
            continue

        logger.info(
            "[CCTV_YOLO_CONTEXT] buffer frames selected camera_sen_id=%s buffer_camera_id=%s frames=%s seconds=%s",
            camera.get("sen_id"),
            camera_id,
            len(frames),
            seconds,
        )
        return build_yolo_normalized_context_from_frames(
            camera,
            frames,
            source=f"buffer:{camera_id}",
            source_camera_id=camera_id,
        )

    return None


def build_yolo_normalized_context_from_frames(
    camera: dict,
    frames: list[dict],
    *,
    source: str,
    source_camera_id: int | None = None,
) -> YoloNormalizedContext:
    """Run YOLO on sampled frames and convert the result into compact text."""
    if not frames:
        raise RuntimeError("YOLO normalized context requires at least one frame")

    from ai.vlm.fire_pipeline.normalizer import WindowNormalizer
    from ai.vlm.fire_pipeline.prompt_builder import make_compact_vlm_text

    ordered = sorted(frames, key=lambda item: float(item.get("timestamp") or time.time()))
    sampled = _sample_frames(ordered, _max_context_frames())
    if not sampled:
        raise RuntimeError("YOLO normalized context sampling produced no frames")

    normalizer = WindowNormalizer(
        window_size=max(1, len(sampled)),
        keep_count=int(getattr(settings, "fire_pipeline_normalized_keep_count", 3) or 3),
        danger_classes=DANGER_CLASSES,
    )
    start_timestamp = float(sampled[0].get("timestamp") or time.time())
    completed_summary = None
    saved_frame_paths: list[str] = []

    logger.info(
        "[CCTV_YOLO_CONTEXT] START normalize camera_sen_id=%s source=%s sampled=%s/%s",
        camera.get("sen_id"),
        source,
        len(sampled),
        len(ordered),
    )

    for index, item in enumerate(sampled, start=1):
        frame = item.get("frame")
        if frame is None:
            continue

        detections, _analyzed_frame = _detect_frame(frame)
        frame_path = _save_context_frame(camera, frame, source=source, index=index)
        saved_frame_paths.append(frame_path)

        timestamp = float(item.get("timestamp") or start_timestamp)
        frame_data = {
            "frame_number": index,
            "time": round(timestamp - start_timestamp, 2),
            "image_path": frame_path,
            "detections": detections,
        }
        completed_summary = normalizer.add_frame(frame_data)

    summary, history = normalizer.make_history_with_live_summary(completed_summary)
    if summary is None:
        raise RuntimeError("YOLO normalized context has no summary")

    normalized_text = make_compact_vlm_text(history)
    image_path = _latest_image_path(summary, saved_frame_paths)

    context = YoloNormalizedContext(
        normalized_text=normalized_text,
        image_path=image_path,
        source=source,
        camera_id=source_camera_id,
        frame_count=len(ordered),
        sampled_count=len(sampled),
        saved_frame_count=len(saved_frame_paths),
        yolo_model_path=_model_path(),
        generated_at=datetime.now().isoformat(timespec="seconds"),
        detection_summary=_build_detection_summary(summary),
    )
    logger.info(
        "[CCTV_YOLO_CONTEXT] END normalize camera_sen_id=%s source=%s sampled=%s image=%s text_len=%s",
        camera.get("sen_id"),
        source,
        context.sampled_count,
        context.image_path,
        len(context.normalized_text),
    )
    return context


def public_yolo_context(context: dict | YoloNormalizedContext | None, *, include_text: bool = False) -> dict:
    if context is None:
        return {}
    if isinstance(context, YoloNormalizedContext):
        return context.public(include_text=include_text)

    text = str(context.get("normalized_text") or "")
    data = {
        "source": context.get("source"),
        "camera_id": context.get("camera_id"),
        "frame_count": context.get("frame_count"),
        "sampled_count": context.get("sampled_count"),
        "saved_frame_count": context.get("saved_frame_count"),
        "image_path": context.get("image_path"),
        "generated_at": context.get("generated_at"),
        "normalized_text_length": len(text),
        "normalized_text_preview": _limit_text(text, 300),
        "detection_summary": context.get("detection_summary") or {},
        "yolo_model_path": context.get("yolo_model_path"),
        "error": context.get("error", ""),
    }
    if include_text:
        data["normalized_text"] = text
    return data


def _detect_frame(frame: Any):
    from ai.vlm.fire_pipeline.detector import YoloDetector

    global _detector, _detector_key
    model_path = _model_path()
    confidence = _confidence()
    key = (model_path, confidence)

    with _detector_lock:
        if _detector is None or _detector_key != key:
            logger.info(
                "[CCTV_YOLO_CONTEXT] detector init model=%s classes=%s confidence=%s",
                model_path,
                DETECT_CLASSES,
                confidence,
            )
            _detector = YoloDetector(
                model_path=model_path,
                detect_classes=DETECT_CLASSES,
                confidence=confidence,
            )
            _detector_key = key
        return _detector.detect(frame)


def _save_context_frame(camera: dict, frame: Any, *, source: str, index: int) -> str:
    import cv2

    folder = Path(getattr(settings, "cctv_vlm_yolo_context_frame_dir", "cctv_vlm_yolo_frames"))
    folder.mkdir(parents=True, exist_ok=True)

    camera_key = _safe_token(camera.get("sen_id") or camera.get("sensor_id") or "unknown")
    source_key = _safe_token(source)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    prefix = f"cctv_vlm_cam_{camera_key}_"
    frame_path = folder / f"{prefix}{source_key}_{timestamp}_{index:03d}.jpg"
    write_frame = _resize_for_vlm(frame)
    if not cv2.imwrite(str(frame_path), write_frame):
        raise RuntimeError(f"YOLO context frame write failed: {frame_path}")

    _prune_context_frames(folder, prefix)
    return str(frame_path)


def _resize_for_vlm(frame: Any) -> Any:
    import cv2

    max_side = int(getattr(settings, "cctv_vlm_request_image_max_side", 512) or 512)
    if max_side <= 0:
        return frame

    height, width = frame.shape[:2]
    current_max = max(width, height)
    if current_max <= max_side:
        return frame

    scale = max_side / float(current_max)
    resized_width = max(1, int(width * scale))
    resized_height = max(1, int(height * scale))
    return cv2.resize(frame, (resized_width, resized_height), interpolation=cv2.INTER_AREA)


def _prune_context_frames(folder: Path, prefix: str) -> None:
    keep_count = int(getattr(settings, "cctv_vlm_yolo_context_saved_frames_keep_count", 240) or 240)
    files = sorted(
        (path for path in folder.glob(f"{prefix}*.jpg") if path.is_file()),
        key=lambda path: path.stat().st_mtime,
    )
    overflow = len(files) - keep_count
    if overflow <= 0:
        return

    for path in files[:overflow]:
        try:
            path.unlink()
        except OSError:
            logger.debug("[CCTV_YOLO_CONTEXT] old frame remove failed path=%s", path, exc_info=True)


def _sample_frames(frames: list[dict], max_frames: int) -> list[dict]:
    if max_frames <= 0 or len(frames) <= max_frames:
        return frames
    if max_frames == 1:
        return [frames[-1]]

    step = (len(frames) - 1) / float(max_frames - 1)
    indexes = []
    for i in range(max_frames):
        index = round(i * step)
        if index not in indexes:
            indexes.append(index)
    return [frames[index] for index in indexes]


def _latest_image_path(summary: dict, fallback_paths: list[str]) -> str:
    for frame in reversed(summary.get("frames", [])):
        image_path = frame.get("image_path")
        if image_path:
            return image_path
    if fallback_paths:
        return fallback_paths[-1]
    return ""


def _build_detection_summary(summary: dict) -> dict[str, Any]:
    current_state = summary.get("current_state") or {}
    person_movement = summary.get("person_movement") or {}
    person_tracks = summary.get("person_tracks") or []

    current: dict[str, dict[str, Any]] = {}
    for class_name in DETECT_CLASSES:
        item = current_state.get(class_name) or {}
        count = int(item.get("count") or 0)
        current[class_name] = {
            "count": count,
            "position": item.get("position") or "none",
            "confidence": item.get("average_confidence"),
            "center": item.get("center") or [],
        }

    people_count = current["person"]["count"]
    if people_count <= 0:
        visible_people = "none"
    elif people_count == 1:
        visible_people = "one"
    else:
        visible_people = "multiple"

    visible_risks = [
        class_name
        for class_name in ("fire", "smoke")
        if current[class_name]["count"] > 0
    ] or ["none"]

    movement_found = person_movement.get("found") is True
    movement = {
        "found": movement_found,
        "direction": person_movement.get("direction", "none" if not movement_found else "unknown"),
        "delta": person_movement.get("delta") or [],
        "start": person_movement.get("start") or [],
        "end": person_movement.get("end") or [],
    }
    track_items = []
    for track in person_tracks[:3]:
        track_items.append({
            "id": track.get("id"),
            "direction": track.get("direction", "unknown"),
            "visible": bool(track.get("visible_now", True)),
            "relation": (track.get("relation") or {}).get("type", "unknown"),
        })

    text = (
        f"person={people_count}({current['person']['position']}), "
        f"movement={movement['direction']}, "
        f"fire={current['fire']['count']}({current['fire']['position']}), "
        f"smoke={current['smoke']['count']}({current['smoke']['position']})"
    )

    return {
        "window": {
            "start_time": summary.get("start_time"),
            "end_time": summary.get("end_time"),
            "frame_count": summary.get("frame_count"),
        },
        "visible_people": visible_people,
        "people_count": people_count,
        "visible_risks": visible_risks,
        "current": current,
        "person_movement": movement,
        "person_tracks": track_items,
        "text": text,
    }


def _model_path() -> str:
    return (
        str(getattr(settings, "fire_pipeline_yolo_model_path", "") or "")
        or str(getattr(settings, "yolo_model_path", "") or "")
        or "0507_best.engine"
    )


def _confidence() -> float | None:
    value = getattr(settings, "yolo_confidence", None)
    if value is None:
        return None
    return float(value)


def _context_seconds() -> float:
    value = getattr(settings, "cctv_vlm_yolo_context_seconds", None)
    if value is None or float(value) <= 0:
        value = getattr(settings, "frame_buffer_seconds", 10)
    return float(value)


def _max_context_frames() -> int:
    value = int(getattr(settings, "cctv_vlm_yolo_context_max_frames", 0) or 0)
    if value > 0:
        return value

    seconds = _context_seconds()
    interval = float(getattr(settings, "frame_buffer_sample_interval_seconds", 0.5) or 0.5)
    return max(1, int(seconds / interval))


def _safe_token(value: Any) -> str:
    text = str(value)
    return "".join(char if char.isalnum() else "_" for char in text)[:80] or "unknown"


def _limit_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[: max(0, max_chars - len(marker))] + marker
