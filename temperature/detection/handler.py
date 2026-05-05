"""온도 이상 감지 세션 — 5초마다 VLM 호출."""
import asyncio
import logging

from ai.vlm.session import AnomalySession
from database.models import AnomalyType

logger = logging.getLogger(__name__)

VLM_INTERVAL = 5


class TemperatureAnomalySession(AnomalySession):
    def __init__(self, event_id: int, process_id: int, sensor_id: int, device_id: str, threshold: float):
        super().__init__(event_id, process_id, AnomalyType.temperature)
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.threshold = threshold

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        from database import AsyncSessionLocal
        from database.crud import report as report_crud
        from database.crud import sensor as sensor_crud
        from database.models import SensorReading
        from sqlalchemy import select
        from ai.vlm import client as vlm_client
        from ai.prompts import temperature as temp_prompt
        from ai.yolo.inference import get_recent_detections
        from cctv.rtsp import get_reader
        from core.notification import send_anomaly_alert
        import datetime

        while not stop_event.is_set():
            try:
                # 최신 온도/습도
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(SensorReading)
                        .where(SensorReading.sensor_id == self.sensor_id)
                        .order_by(SensorReading.timestamp.desc())
                        .limit(1)
                    )
                    reading = result.scalar_one_or_none()
                    current_temperature = reading.data.get("temperature", 0) if reading else 0
                    current_humidity = reading.data.get("humidity", 0) if reading else 0

                # 공정 정보 + 카메라 목록
                async with AsyncSessionLocal() as db:
                    sensor = await sensor_crud.get_by_id(db, self.sensor_id)
                    process_name = sensor.process.name if sensor and sensor.process else "알 수 없음"
                    precautions = (sensor.process.precautions or "") if sensor and sensor.process else ""
                    cameras = await sensor_crud.get_cameras_by_process(db, self.process_id)

                # 현재 프레임 캡처
                frame_path = None
                recent = []
                if cameras:
                    cam = cameras[0]
                    reader = get_reader(cam.id)
                    if reader:
                        frame = await reader.read_frame()
                        if frame is not None:
                            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                            frame_path = await reader.save_frame(frame, f"temp_{self.event_id}_{ts}.jpg")
                    recent = await get_recent_detections(cam.id, limit=10)

                prompt = temp_prompt.build_prompt(
                    current_temperature=current_temperature,
                    current_humidity=current_humidity,
                    threshold=self.threshold,
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
                    process_id=self.process_id, anomaly_type="temperature",
                    message=response, event_id=self.event_id,
                )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in temperature session event=%s", self.event_id)

            await asyncio.sleep(VLM_INTERVAL)

        from temperature.detection.detector import clear_active_event
        clear_active_event(self.sensor_id)
