"""CCTV 이상 감지 세션 — 5초마다 VLM 호출 + 모든 밴드 진동·LED 경보."""
import asyncio
import logging

from ai.vlm.session import AnomalySession
from database.models import AnomalyType

logger = logging.getLogger(__name__)

VLM_INTERVAL = 5
ANOMALY_LABELS = {"fire", "flame", "smoke", "spark"}


class CCTVAnomalySession(AnomalySession):
    def __init__(
        self, event_id: int, process_id: int, sensor_id: int,
        camera_id: int, initial_labels: list[str], initial_frame_path: str,
    ):
        super().__init__(event_id, process_id, AnomalyType.cctv)
        self.sensor_id = sensor_id
        self.camera_id = camera_id
        self.initial_labels = initial_labels
        self.initial_frame_path = initial_frame_path

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        from database import AsyncSessionLocal
        from database.crud import report as report_crud
        from database.crud import sensor as sensor_crud
        from ai.vlm import client as vlm_client
        from ai.prompts import cctv as cctv_prompt
        from ai.yolo.inference import get_recent_detections
        from cctv.rtsp import get_reader
        from core.notification import send_anomaly_alert
        import datetime

        await self._alert_all_bands()

        frame_path = self.initial_frame_path
        first = True

        while not stop_event.is_set():
            try:
                async with AsyncSessionLocal() as db:
                    sensor = await sensor_crud.get_by_id(db, self.sensor_id)
                    process_name = sensor.process.name if sensor and sensor.process else "알 수 없음"
                    precautions = (sensor.process.precautions or "") if sensor and sensor.process else ""

                if not first:
                    reader = get_reader(self.camera_id)
                    if reader:
                        frame = await reader.read_frame()
                        if frame is not None:
                            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                            frame_path = await reader.save_frame(frame, f"cctv_{self.event_id}_{ts}.jpg")

                first = False
                recent = await get_recent_detections(self.camera_id, limit=10)

                detected_labels = self.initial_labels
                if recent:
                    latest = recent[-1].get("detections") or {}
                    fresh = [
                        obj["label"] for obj in latest.get("objects", [])
                        if obj.get("label", "").lower() in ANOMALY_LABELS
                    ]
                    if fresh:
                        detected_labels = fresh

                prompt = cctv_prompt.build_prompt(
                    detected_labels=detected_labels,
                    process_name=process_name,
                    precautions=precautions,
                    recent_detections=recent,
                )

                response = (
                    await vlm_client.query_with_image(prompt, frame_path)
                    if frame_path
                    else await vlm_client.query_text_only(prompt)
                )

                async with AsyncSessionLocal() as db:
                    await report_crud.save_vlm_query(
                        db, anomaly_event_id=self.event_id,
                        prompt=prompt, response=response, frame_path=frame_path,
                    )
                    await db.commit()

                await send_anomaly_alert(
                    process_id=self.process_id, anomaly_type="cctv",
                    message=response, event_id=self.event_id,
                )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in CCTV session event=%s", self.event_id)

            await asyncio.sleep(VLM_INTERVAL)

        from cctv.detection.detector import clear_active_event
        clear_active_event(self.camera_id)

    async def _alert_all_bands(self) -> None:
        """공정 내 모든 심박 밴드에 진동+LED 긴급 경보."""
        from database import AsyncSessionLocal
        from database.crud import sensor as sensor_crud
        from core.mqtt.client import publish
        from core.mqtt.topics import Topics

        async with AsyncSessionLocal() as db:
            bands = await sensor_crud.get_bands_by_process(db, self.process_id)

        for band in bands:
            await publish(
                Topics.band_alert(band.sensor.device_id),
                {"type": "emergency", "vibrate": True, "led": True, "event_id": self.event_id},
            )
        logger.info("Band alerts sent to %d bands in process %s", len(bands), self.process_id)
