"""N분 롤링 프레임 버퍼 — 상시 작동 프로세스 (2 FPS)."""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import settings
from database import AsyncSessionLocal
from database.models import FrameRecord

logger = logging.getLogger(__name__)

CAPTURE_INTERVAL = 0.5  # 2 FPS


class FrameBuffer:
    def __init__(self, camera_id: int, process_id: int):
        self.camera_id = camera_id
        self.process_id = process_id
        self._task: asyncio.Task | None = None

    async def _capture_loop(self) -> None:
        from cctv.rtsp import get_reader
        from cctv.frame_processing import save_and_analyze_frame

        while True:
            try:
                reader = get_reader(self.camera_id)
                if reader is None:
                    await asyncio.sleep(CAPTURE_INTERVAL)
                    continue

                frame = await reader.read_frame()
                if frame is None:
                    await asyncio.sleep(CAPTURE_INTERVAL)
                    continue

                processed = await save_and_analyze_frame(frame, self.camera_id)

                async with AsyncSessionLocal() as db:
                    db.add(FrameRecord(
                        camera_id=self.camera_id,
                        process_id=self.process_id,
                        frame_path=processed.frame_path,
                        yolo_detections=processed.yolo_detections,
                        is_anomaly=processed.is_anomaly,
                    ))
                    await db.commit()

                await self._purge_old_frames()

                if processed.is_anomaly:
                    from cctv.detection.detector import check
                    asyncio.create_task(
                        check(
                            camera_id=self.camera_id,
                            detections=processed.yolo_detections,
                            frame_path=processed.frame_path,
                        )
                    )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Frame capture error for camera %s", self.camera_id)

            await asyncio.sleep(CAPTURE_INTERVAL)

    async def _purge_old_frames(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.frame_buffer_minutes)
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            result = await db.execute(
                select(FrameRecord)
                .where(FrameRecord.camera_id == self.camera_id, FrameRecord.timestamp < cutoff)
            )
            old = result.scalars().all()
            for rec in old:
                Path(rec.frame_path).unlink(missing_ok=True)
                await db.delete(rec)
            if old:
                await db.commit()

    def start(self) -> None:
        self._task = asyncio.create_task(self._capture_loop())
        logger.info("FrameBuffer started for camera %s (2 FPS)", self.camera_id)

    def stop(self) -> None:
        if self._task:
            self._task.cancel()


_buffers: dict[int, FrameBuffer] = {}


def get_buffer(camera_id: int) -> FrameBuffer | None:
    return _buffers.get(camera_id)


def start_buffer(camera_id: int, process_id: int) -> FrameBuffer:
    if camera_id in _buffers:
        return _buffers[camera_id]
    buf = FrameBuffer(camera_id, process_id)
    buf.start()
    _buffers[camera_id] = buf
    return buf


def stop_buffer(camera_id: int) -> None:
    buf = _buffers.pop(camera_id, None)
    if buf:
        buf.stop()
