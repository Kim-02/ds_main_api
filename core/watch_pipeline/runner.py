"""등록된 heart_band 센서마다 스레드를 열어 주기적으로 휴식 권고 파이프라인을 실행한다.

흐름 (스레드 1개 = 워치 1개):
  loop {
    STEP1  sensor_id → worker_id  (DB)
    STEP2  온습도 + 심박 + 작업자 정보 조회 → 회귀모델 예측  (RestRuntimeService)
    STEP3  휴식 권고가 필요하면 MQTT publish
    sleep(interval_sec)
  }
"""
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

    # ── 라이프사이클 ──────────────────────────────────────────────────────────

    def start(self):
        self._thread = threading.Thread(
            target=self._loop,
            name=f"watch-pipeline-{self.sensor_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "[WatchPipeline] 스레드 시작 sensor_id=%s interval_sec=%d",
            self.sensor_id,
            self.interval_sec,
        )

    def stop(self):
        self._stop_event.set()
        logger.info("[WatchPipeline] 종료 요청 sensor_id=%s", self.sensor_id)

    # ── 내부 루프 ─────────────────────────────────────────────────────────────

    def _loop(self):
        logger.info(
            "[WatchPipeline] ─── 루프 진입 sensor_id=%s interval_sec=%d",
            self.sensor_id,
            self.interval_sec,
        )
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception as exc:
                logger.error(
                    "[WatchPipeline] 파이프라인 오류 sensor_id=%s: %s: %s",
                    self.sensor_id,
                    type(exc).__name__,
                    exc,
                )
            self._stop_event.wait(self.interval_sec)
        logger.info("[WatchPipeline] ─── 루프 종료 sensor_id=%s", self.sensor_id)

    def _run_once(self):
        sid = self.sensor_id
        logger.info("[WatchPipeline] >>> START _run_once sensor_id=%s", sid)

        # STEP1: sensor_id → worker_id
        logger.info("[WatchPipeline] STEP1 START sensor_id→worker_id sensor_id=%s", sid)
        worker_id = self._repository.find_worker_id_by_sensor_id(sid)
        logger.info(
            "[WatchPipeline] STEP1 END sensor_id=%s worker_id=%s",
            sid,
            worker_id,
        )
        if not worker_id:
            logger.info(
                "[WatchPipeline] STEP1 SKIP sensor_id=%s: 연결된 작업자 없음 → 파이프라인 종료",
                sid,
            )
            return

        # STEP2: 온습도 + 심박 + 작업자 정보 조회 → 모델 예측
        logger.info(
            "[WatchPipeline] STEP2 START evaluate_worker sensor_id=%s worker_id=%s",
            sid,
            worker_id,
        )
        result = self._rest_service.evaluate_worker(worker_id)
        logger.info(
            "[WatchPipeline] STEP2 END sensor_id=%s worker_id=%s "
            "prediction_result=%s should_rest=%s",
            sid,
            worker_id,
            result.prediction.get("result"),
            result.should_rest,
        )

        # STEP3: 휴식 권고 MQTT 발행
        if result.command is None:
            logger.info(
                "[WatchPipeline] STEP3 SKIP sensor_id=%s: 휴식 불필요",
                sid,
            )
        else:
            topic, payload = result.command.to_topic_and_payload()
            logger.info(
                "[WatchPipeline] STEP3 START MQTT publish sensor_id=%s topic=%s payload=%s",
                sid,
                topic,
                payload,
            )
            self._mqtt_publish_fn(topic, payload)
            logger.info(
                "[WatchPipeline] STEP3 END MQTT publish 완료 sensor_id=%s",
                sid,
            )

        logger.info("[WatchPipeline] <<< END _run_once sensor_id=%s", sid)


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

    # ── 라이프사이클 ──────────────────────────────────────────────────────────

    def start(self):
        logger.info("[WatchScheduler] 스케줄러 시작")

        try:
            self._init_service()
        except Exception as exc:
            logger.error("[WatchScheduler] 서비스 초기화 실패 (스케줄러 비활성화): %s", exc)
            return

        sensors = self._db_handler.get_registered_sensor_rows()
        heart_bands = [s for s in sensors if s.get("sensor_type") == "heart_band"]
        logger.info(
            "[WatchScheduler] 등록된 heart_band 센서 수=%d 전체 센서 수=%d",
            len(heart_bands),
            len(sensors),
        )

        if not heart_bands:
            logger.info("[WatchScheduler] heart_band 센서 없음 → 스레드 없이 종료")
            return

        for sensor in heart_bands:
            sensor_id = sensor.get("sensor_id")
            if not sensor_id:
                logger.warning("[WatchScheduler] sensor_id 없는 센서 행 스킵: %s", sensor)
                continue
            self._start_runner(sensor_id)

        logger.info("[WatchScheduler] 스레드 등록 완료 count=%d", len(self._runners))

    def stop(self):
        logger.info("[WatchScheduler] 모든 스레드 종료 요청 count=%d", len(self._runners))
        for runner in self._runners.values():
            runner.stop()
        self._runners.clear()

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────────

    def _init_service(self):
        logger.info("[WatchScheduler] 휴식 예측 서비스 초기화 시작")
        from ai.rest import DatabaseHandlerRestDataRepository, RestRuntimeService

        self._repository = DatabaseHandlerRestDataRepository(self._db_handler)
        self._rest_service = RestRuntimeService.from_model_path(
            repository=self._repository,
        )
        logger.info("[WatchScheduler] 휴식 예측 서비스 초기화 완료")

    def _start_runner(self, sensor_id: str):
        if sensor_id in self._runners:
            logger.warning("[WatchScheduler] 이미 실행 중 sensor_id=%s", sensor_id)
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
        logger.info("[WatchScheduler] sensor_id=%s 파이프라인 스레드 등록 완료", sensor_id)
