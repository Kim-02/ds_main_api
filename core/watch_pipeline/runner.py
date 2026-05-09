"""등록된 heart_band 센서마다 스레드를 열어 주기적으로 휴식 권고 파이프라인을 실행한다."""
import logging
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SEC = 30


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
        worker_id = self._repository.find_worker_id_by_sensor_id(self.sensor_id)
        if not worker_id:
            return

        result = self._rest_service.evaluate_worker(worker_id)
        if result.command is None:
            return

        topic, payload = result.command.to_topic_and_payload()
        self._mqtt_publish_fn(topic, payload)
        logger.info("[WatchPipeline] 휴식 권고 발행 sensor_id=%s", self.sensor_id)


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
