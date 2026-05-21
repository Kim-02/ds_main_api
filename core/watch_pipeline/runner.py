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
        camera_vlm_manager=None,
        interval_sec: int = DEFAULT_INTERVAL_SEC,
        stale_threshold_sec: int = STALE_THRESHOLD_SEC,
    ):
        self.sensor_id = sensor_id
        self._repository = repository
        self._rest_service = rest_service
        self._mqtt_publish_fn = mqtt_publish_fn
        self._camera_vlm_manager = camera_vlm_manager
        self.interval_sec = interval_sec
        self.stale_threshold_sec = stale_threshold_sec
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
        # 설정된 시간 이내 심박 데이터가 없으면 워치 미착용으로 간주하고 스킵.
        # sensor.last_seen_at 은 status ping 만으로도 갱신되므로
        # hb_trans 의 실제 심박 수신 시각을 기준으로 한다.
        last_hr = self._repository.fetch_last_hr_time(self.sensor_id)
        if last_hr is None:
            return
        if (datetime.now() - last_hr).total_seconds() > self.stale_threshold_sec:
            logger.info("[WatchPipeline] 심박 미수신 (미착용 추정) — 스킵 sensor_id=%s", self.sensor_id)
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
        self._trigger_same_space_camera_vlm(worker_id, result.prediction)

        # 2) duration 동안 대기 (stop 요청 시 즉시 탈출)
        duration_sec = result.command.duration_ms / 1000
        stopped = self._stop_event.wait(duration_sec)

        # 3) 제어 신호 OFF  (진동 해제) — stop 요청이 아닐 때만
        if not stopped:
            self._mqtt_publish_fn(topic, _ALERT_OFF_PAYLOAD)
            logger.info("[WatchPipeline] 진동 해제 발행 sensor_id=%s", self.sensor_id)

    def _trigger_same_space_camera_vlm(self, worker_id: str, prediction: dict) -> None:
        if self._camera_vlm_manager is None:
            return

        # 새 repository 메서드로 LEFT JOIN 기반 통합 컨텍스트 조회
        # (worker_hr_data 없어도 진행, is_manager 포함, hazard_type 포함)
        enriched_prediction = prediction
        try:
            trigger_data = self._repository.build_worker_vlm_trigger(
                worker_id,
                regression_result=prediction,
            )
            worker_info = trigger_data.get("prediction", {}).get("worker", {})
            if worker_info.get("is_manager"):
                logger.info("[WatchPipeline] 관리자 VLM 트리거 스킵 worker_id=%s", worker_id)
                return
            # compact_worker_regression_trigger_for_prompt 호환 형태(inner prediction dict) 사용
            enriched_prediction = trigger_data.get("prediction", prediction)
        except Exception as exc:
            logger.warning(
                "[WatchPipeline] build_worker_vlm_trigger 실패, 기존 prediction 사용 worker_id=%s: %s",
                worker_id, exc,
            )

        try:
            result = self._camera_vlm_manager.trigger_for_watch_sensor(
                self.sensor_id,
                worker_id=worker_id,
                prediction=enriched_prediction,
            )
            logger.info("[WatchPipeline] same-space camera VLM trigger result=%s", result)
        except Exception as exc:
            logger.error("[WatchPipeline] same-space camera VLM trigger failed: %s", exc)


class WatchPipelineScheduler:
    """DB에 등록된 heart_band 센서마다 WatchPipelineRunner 스레드를 관리한다."""

    def __init__(
        self,
        db_handler,
        mqtt_publish_fn: Callable[[str, str], None],
        camera_vlm_manager=None,
        interval_sec: int = DEFAULT_INTERVAL_SEC,
        stale_threshold_sec: int = STALE_THRESHOLD_SEC,
        alert_duration_ms: int = 5000,
        alert_reset_after_ms: int = 5000,
        forced_rest_work_min: int = 120,
        rest_repository_defaults: dict | None = None,
    ):
        self._db_handler = db_handler
        self._mqtt_publish_fn = mqtt_publish_fn
        self._camera_vlm_manager = camera_vlm_manager
        self.interval_sec = interval_sec
        self.stale_threshold_sec = stale_threshold_sec
        self.alert_duration_ms = alert_duration_ms
        self.alert_reset_after_ms = alert_reset_after_ms
        self.forced_rest_work_min = forced_rest_work_min
        self.rest_repository_defaults = rest_repository_defaults or {}
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
        from ai.rest import BandControlCommandBuilder, DatabaseHandlerRestDataRepository, RestRuntimeService

        self._repository = DatabaseHandlerRestDataRepository(
            self._db_handler,
            **self.rest_repository_defaults,
        )
        command_builder = BandControlCommandBuilder(
            duration_ms=self.alert_duration_ms,
            reset_after_ms=self.alert_reset_after_ms,
        )
        self._rest_service = RestRuntimeService.from_model_path(
            repository=self._repository,
            forced_rest_work_min=self.forced_rest_work_min,
            command_builder=command_builder,
        )
        logger.info("[WatchScheduler] 서비스 초기화 완료")

    def _start_runner(self, sensor_id: str):
        if sensor_id in self._runners:
            return
        runner = WatchPipelineRunner(
            sensor_id=sensor_id,
            repository=self._repository,
            rest_service=self._rest_service,
            mqtt_publish_fn=self._mqtt_publish_fn,
            camera_vlm_manager=self._camera_vlm_manager,
            interval_sec=self.interval_sec,
            stale_threshold_sec=self.stale_threshold_sec,
        )
        runner.start()
        self._runners[sensor_id] = runner
