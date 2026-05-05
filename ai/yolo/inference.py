"""YOLO 추론 래퍼 — 프레임 분석 및 탐지 결과 정규화."""
import asyncio
import logging
from functools import lru_cache

import numpy as np

from config import settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load_model():
    from ultralytics import YOLO
    model = YOLO(settings.yolo_model_path)
    logger.info("YOLO model loaded: %s", settings.yolo_model_path)
    return model


def _infer(frame: np.ndarray) -> dict:
    model = _load_model()
    results = model(frame, conf=settings.yolo_confidence, verbose=False)
    objects = []
    for result in results:
        for box in result.boxes:
            label = result.names[int(box.cls[0])]
            conf = float(box.conf[0])
            x1, y1, x2, y2 = [int(v) for v in box.xyxy[0]]
            objects.append({"label": label, "confidence": round(conf, 3), "bbox": [x1, y1, x2, y2]})
    return {"objects": objects, "count": len(objects)}


async def analyze_frame(frame: np.ndarray) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _infer, frame)


async def get_recent_detections(camera_id: int, limit: int = 10) -> list[dict]:
    """VLM 컨텍스트 구성용 최근 N개 프레임 탐지 이력."""
    from database import AsyncSessionLocal
    from database.models import FrameRecord
    from sqlalchemy import select

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(FrameRecord)
            .where(FrameRecord.camera_id == camera_id)
            .order_by(FrameRecord.timestamp.desc())
            .limit(limit)
        )
        records = result.scalars().all()
        return [
            {
                "timestamp": r.timestamp.isoformat(),
                "detections": r.yolo_detections,
                "is_anomaly": r.is_anomaly,
            }
            for r in reversed(records)
        ]
