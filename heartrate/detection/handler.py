"""심박 이상 감지 세션 — 5초마다 VLM 호출 + 회귀 분석."""
import asyncio
import logging
from typing import Optional

from ai.vlm.session import AnomalySession
from database.models import AnomalyType

logger = logging.getLogger(__name__)

VLM_INTERVAL = 5


class HeartrateAnomalySession(AnomalySession):
    def __init__(
        self, event_id: int, process_id: int, sensor_id: int,
        device_id: str, threshold: int, worker_id: Optional[int],
    ):
        super().__init__(event_id, process_id, AnomalyType.heartrate)
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.threshold = threshold
        self.worker_id = worker_id

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        from database import AsyncSessionLocal
        from database.crud import report as report_crud
        from database.crud import sensor as sensor_crud
        from database.crud import worker as worker_crud
        from database.models import SensorReading, Sensor, SensorType
        from sqlalchemy import select
        from ai.vlm import client as vlm_client
        from ai.prompts import heartrate as hr_prompt
        from ai.yolo.inference import get_recent_detections
        from cctv.rtsp import get_reader
        from core.notification import send_anomaly_alert
        from heartrate.regression import predict_rest_needed
        import datetime

        while not stop_event.is_set():
            try:
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(SensorReading)
                        .where(SensorReading.sensor_id == self.sensor_id)
                        .order_by(SensorReading.timestamp.desc())
                        .limit(1)
                    )
                    reading = result.scalar_one_or_none()
                    current_hr = reading.data.get("heartrate", 0) if reading else 0

                worker_name, health_baseline = "알 수 없음", None
                if self.worker_id:
                    async with AsyncSessionLocal() as db:
                        w = await worker_crud.get_by_id(db, self.worker_id)
                        if w:
                            worker_name, health_baseline = w.name, w.health_baseline

                async with AsyncSessionLocal() as db:
                    sensor = await sensor_crud.get_by_id(db, self.sensor_id)
                    process_name = sensor.process.name if sensor and sensor.process else "알 수 없음"
                    precautions = (sensor.process.precautions or "") if sensor and sensor.process else ""
                    cameras = await sensor_crud.get_cameras_by_process(db, self.process_id)

                # 공정 내 최신 온습도
                current_temperature, current_humidity = 0.0, 0.0
                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(SensorReading)
                        .join(Sensor, SensorReading.sensor_id == Sensor.id)
                        .where(Sensor.process_id == self.process_id, Sensor.sensor_type == SensorType.temperature)
                        .order_by(SensorReading.timestamp.desc())
                        .limit(1)
                    )
                    tr = result.scalar_one_or_none()
                    if tr:
                        current_temperature = tr.data.get("temperature", 0)
                        current_humidity = tr.data.get("humidity", 0)

                frame_path, recent = None, []
                if cameras:
                    cam = cameras[0]
                    reader = get_reader(cam.id)
                    if reader:
                        frame = await reader.read_frame()
                        if frame is not None:
                            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                            frame_path = await reader.save_frame(frame, f"hr_{self.event_id}_{ts}.jpg")
                    recent = await get_recent_detections(cam.id, limit=10)

                rest_rec = predict_rest_needed(
                    heartrate=current_hr,
                    temperature=current_temperature,
                    humidity=current_humidity,
                    health_baseline=health_baseline,
                )

                prompt = hr_prompt.build_prompt(
                    current_heartrate=current_hr,
                    threshold=self.threshold,
                    worker_name=worker_name,
                    process_name=process_name,
                    precautions=precautions,
                    health_baseline=health_baseline,
                    recent_detections=recent,
                    rest_recommendation=rest_rec,
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
                    process_id=self.process_id, anomaly_type="heartrate",
                    message=response, event_id=self.event_id,
                )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in heartrate session event=%s", self.event_id)

            await asyncio.sleep(VLM_INTERVAL)

        from heartrate.detection.detector import clear_active_event
        clear_active_event(self.sensor_id)
