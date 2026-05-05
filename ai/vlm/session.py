"""이상 감지 VLM 루프 기반 클래스 — 각 도메인 핸들러가 상속."""
import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from database import AsyncSessionLocal
from database.crud import report as report_crud
from database.models import AnomalyStatus, AnomalyType

logger = logging.getLogger(__name__)


class AnomalySession(ABC):
    def __init__(self, event_id: int, process_id: int, anomaly_type: AnomalyType):
        self.event_id = event_id
        self.process_id = process_id
        self.anomaly_type = anomaly_type
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        try:
            await self.run_loop(self._stop_event)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Unhandled error in anomaly session event_id=%s", self.event_id)
        finally:
            await self._mark_resolved()
            from core.detection_manager import manager
            manager.remove(self.event_id)

    @abstractmethod
    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """5초 간격 VLM 호출 루프를 구현."""

    async def _mark_resolved(self) -> None:
        async with AsyncSessionLocal() as db:
            event = await report_crud.get_anomaly_event_by_id(db, self.event_id)
            if event and event.status == AnomalyStatus.active:
                await report_crud.update_anomaly_event(
                    db,
                    event,
                    status=AnomalyStatus.resolved,
                    end_time=datetime.now(timezone.utc),
                )
                await db.commit()
