"""등록된 heart_band 센서마다 스레드를 열어 주기적으로 휴식 권고 파이프라인을 실행한다.

제어 신호 흐름:
  1) 휴식 권고 alert_on 발행  (약한 → yellow / 강한·반드시 → red, 5초, 진동)
  2) duration_ms(5초) 대기
  3) alert_off 발행  (진동 해제)
"""
import json
import logging
import threading
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SEC = 30
STALE_THRESHOLD_SEC = 15

_ALERT_OFF_PAYLOAD = json.dumps({"command": "alert_off"})


class WatchPipelineRunner:
    """단일 heart_band 센서에 대한 주기적 파이프라인 실행기."""

    def __init__(
        self,
        sensor_id: str,
        repository,
        rest_service,
        mqtt_publish_fn: Callable[[str, str], None],
        interval_sec: int = DEFAULT_INTERVAL_SEC,
    ):
        self.sensor_id = sensor_id
        self._repository = repository
        self._rest_service = rest_service
        self._mqtt_publish_fn = mqtt_publish_fn
        self.interval_sec = interval_sec
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(
            target=self._loop,
            name=f"watch-pipeline-{self.sensor_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info("[WatchPipeline] 스레드 시작 sensor_id=%s", self.sensor_id)

    def stop(self):
        self._stop_event.set()
        logger.info("[WatchPipeline] 스레드 종료 sensor_id=%s", self.sensor_id)

    def _loop(self):
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception as exc:
                logger.error(
                    "[WatchPipeline] 파이프라인 오류 sensor_id=%s: %s",
                    self.sensor_id,
                    exc,
                )
            self._stop_event.wait(self.interval_sec)

    def _run_once(self):
        # 최근 15초 이내 갱신이 없으면 워치 오프라인으로 간주하고 스킵
        last_seen = self._repository.fetch_sensor_last_seen(self.sensor_id)
        if last_seen is None:
            return
        if (datetime.now() - last_seen).total_seconds() > STALE_THRESHOLD_SEC:
            logger.info("[WatchPipeline] 워치 응답 없음 — 스킵 sensor_id=%s", self.sensor_id)
            return

        worker_id = self._repository.find_worker_id_by_sensor_id(self.sensor_id)
        if not worker_id:
            return

        result = self._rest_service.evaluate_worker(worker_id)
        if result.command is None:
            return

        topic, on_payload = result.command.to_topic_and_payload()

        # 1) 제어 신호 ON  (약한→yellow 5초 진동 / 강한·반드시→red 5초 진동)
        self._mqtt_publish_fn(topic, on_payload)
        logger.info("[WatchPipeline] 휴식 권고 발행 sensor_id=%s", self.sensor_id)

        # 2) duration 동안 대기 (stop 요청 시 즉시 탈출)
        duration_sec = result.command.duration_ms / 1000
        stopped = self._stop_event.wait(duration_sec)

        # 3) 제어 신호 OFF  (진동 해제) — stop 요청이 아닐 때만
        if not stopped:
            self._mqtt_publish_fn(topic, _ALERT_OFF_PAYLOAD)
            logger.info("[WatchPipeline] 진동 해제 발행 sensor_id=%s", self.sensor_id)


class WatchPipelineScheduler:
    """DB에 등록된 heart_band 센서마다 WatchPipelineRunner 스레드를 관리한다."""

    def __init__(
        self,
        db_handler,
        mqtt_publish_fn: Callable[[str, str], None],
        interval_sec: int = DEFAULT_INTERVAL_SEC,
    ):
        self._db_handler = db_handler
        self._mqtt_publish_fn = mqtt_publish_fn
        self.interval_sec = interval_sec
        self._runners: dict[str, WatchPipelineRunner] = {}
        self._repository = None
        self._rest_service = None

    def start(self):
        logger.info("[WatchScheduler] 스케줄러 시작")
        try:
            self._init_service()
        except Exception as exc:
            logger.error("[WatchScheduler] 서비스 초기화 실패: %s", exc)
            return

        sensors = self._db_handler.get_registered_sensor_rows()
        heart_bands = [s for s in sensors if s.get("sensor_type") == "heart_band"]
        for sensor in heart_bands:
            sensor_id = sensor.get("sensor_id")
            if sensor_id:
                self._start_runner(sensor_id)

    def stop(self):
        logger.info("[WatchScheduler] 스케줄러 종료")
        for runner in self._runners.values():
            runner.stop()
        self._runners.clear()

    def register(self, sensor_id: str) -> None:
        """새 워치 등록 시 호출 — 즉시 스레드를 시작한다."""
        if self._rest_service is None:
            logger.warning("[WatchScheduler] 서비스 미초기화 — sensor_id=%s 시작 불가", sensor_id)
            return
        self._start_runner(sensor_id)

    def unregister(self, sensor_id: str) -> None:
        """워치 매핑 해제 시 호출 — 해당 스레드를 중지한다."""
        runner = self._runners.pop(sensor_id, None)
        if runner:
            runner.stop()

    def _init_service(self):
        from ai.rest import DatabaseHandlerRestDataRepository, RestRuntimeService

        self._repository = DatabaseHandlerRestDataRepository(self._db_handler)
        self._rest_service = RestRuntimeService.from_model_path(repository=self._repository)
        logger.info("[WatchScheduler] 서비스 초기화 완료")

    def _start_runner(self, sensor_id: str):
        if sensor_id in self._runners:
            return
        runner = WatchPipelineRunner(
            sensor_id=sensor_id,
            repository=self._repository,
            rest_service=self._rest_service,
            mqtt_publish_fn=self._mqtt_publish_fn,
            interval_sec=self.interval_sec,
        )
        runner.start()
        self._runners[sensor_id] = runner
