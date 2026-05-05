"""활성 이상 감지 세션 전역 관리자."""
import asyncio
import logging

logger = logging.getLogger(__name__)


class DetectionManager:
    def __init__(self):
        self._sessions: dict[int, "AnomalySession"] = {}

    def add(self, session: "AnomalySession") -> None:
        self._sessions[session.event_id] = session
        session.start()

    async def stop(self, event_id: int) -> None:
        session = self._sessions.pop(event_id, None)
        if session:
            await session.stop()

    def remove(self, event_id: int) -> None:
        self._sessions.pop(event_id, None)

    def get_active_sessions(self) -> list[dict]:
        return [
            {
                "event_id": s.event_id,
                "process_id": s.process_id,
                "anomaly_type": s.anomaly_type.value,
            }
            for s in self._sessions.values()
        ]

    def is_active(self, event_id: int) -> bool:
        return event_id in self._sessions


manager = DetectionManager()
