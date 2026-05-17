from pathlib import Path
from typing import Any, Dict, Optional, Protocol

from .models import (
    BandControlCommand,
    EnvironmentSample,
    RestRuntimeResult,
    WatchSample,
    WorkerProfile,
)
from .rest_calculator import WorkerRawInput
from .rest_model_engine import (
    DEFAULT_FORCED_REST_WORK_MIN,
    FINAL_FORCE_REST,
    FINAL_STRONG_REST,
    FINAL_WEAK_REST,
    RestModelEngine,
)


DEFAULT_MODEL_PATH = Path(__file__).with_name("rest_recommendation_model.pkl")

REST_REQUIRED_RESULTS = {
    FINAL_FORCE_REST,
    FINAL_STRONG_REST,
    FINAL_WEAK_REST,
}

REST_COMMAND_COLORS = {
    FINAL_WEAK_REST: "yellow",
    FINAL_STRONG_REST: "red",
    FINAL_FORCE_REST: "red",
}


class RestDataRepository(Protocol):
    def fetch_environment(self, worker_id: str) -> EnvironmentSample:
        ...

    def fetch_watch(self, worker_id: str) -> WatchSample:
        ...

    def fetch_worker_profile(self, worker_id: str) -> WorkerProfile:
        ...


class BandControlCommandBuilder:
    def __init__(
        self,
        color: str = "yellow",
        vibration: bool = True,
        led: bool = True,
        duration_ms: int = 5000,   # alert_on 유지 시간 (ms) — 이후 runner가 alert_off 발행
        reset_after_ms: int = 5000,
    ):
        self.color = color
        self.vibration = vibration
        self.led = led
        self.duration_ms = duration_ms
        self.reset_after_ms = reset_after_ms

    def build(self, target_topic: str) -> BandControlCommand:
        return BandControlCommand(
            target_topic=target_topic,
            color=self.color,
            vibration=self.vibration,
            led=self.led,
            duration_ms=self.duration_ms,
            reset_after_ms=self.reset_after_ms,
        )

    def build_for_prediction(self, target_topic: str, prediction: Dict[str, Any]) -> BandControlCommand:
        result = prediction.get("result")
        color = REST_COMMAND_COLORS.get(result, self.color)
        return BandControlCommand(
            target_topic=target_topic,
            color=color,
            vibration=self.vibration,
            led=self.led,
            duration_ms=self.duration_ms,
            reset_after_ms=self.reset_after_ms,
        )


class RestRuntimeService:
    def __init__(
        self,
        repository: RestDataRepository,
        engine: RestModelEngine,
        command_builder: Optional[BandControlCommandBuilder] = None,
    ):
        self.repository = repository
        self.engine = engine
        self.command_builder = command_builder or BandControlCommandBuilder()

    @classmethod
    def from_model_path(
        cls,
        repository: RestDataRepository,
        model_path: str | Path | None = None,
        forced_rest_work_min: int = DEFAULT_FORCED_REST_WORK_MIN,
        command_builder: Optional[BandControlCommandBuilder] = None,
    ) -> "RestRuntimeService":
        resolved_model_path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
        engine = RestModelEngine(
            model_path=str(resolved_model_path),
            forced_rest_work_min=forced_rest_work_min,
        )
        return cls(
            repository=repository,
            engine=engine,
            command_builder=command_builder,
        )

    def evaluate_worker(self, worker_id: str) -> RestRuntimeResult:
        environment = self.repository.fetch_environment(worker_id)
        watch = self.repository.fetch_watch(worker_id)
        profile = self.repository.fetch_worker_profile(worker_id)
        baseline_hr = watch.baseline_hr if watch.baseline_hr is not None else profile.baseline_hr

        raw = WorkerRawInput(
            worker_id=profile.worker_id,
            hr=watch.hr,
            temp_c=environment.temp_c,
            humid=environment.humid,
            age=profile.age,
            gender=profile.gender,
            height_cm=profile.height_cm,
            weight_kg=profile.weight_kg,
            work_duration_min=profile.work_duration_min,
            elderly_flag=profile.elderly_flag,
            heart_disease=profile.heart_disease,
            hypertension=profile.hypertension,
            other_disease=profile.other_disease,
            baseline_hr=baseline_hr,
        )

        prediction = self.engine.predict(raw)
        prediction = self._enrich_prediction(
            prediction,
            environment=environment,
            watch=watch,
            profile=profile,
        )

        command = None
        if self.should_send_rest_command(prediction):
            command = self.command_builder.build_for_prediction(profile.target_topic, prediction)

        return RestRuntimeResult(
            worker_id=profile.worker_id,
            prediction=prediction,
            command=command,
        )

    @staticmethod
    def should_send_rest_command(prediction: Dict[str, Any]) -> bool:
        return prediction.get("result") in REST_REQUIRED_RESULTS

    @staticmethod
    def _enrich_prediction(
        prediction: Dict[str, Any],
        *,
        environment: EnvironmentSample,
        watch: WatchSample,
        profile: WorkerProfile,
    ) -> Dict[str, Any]:
        enriched = dict(prediction)
        enriched["worker"] = {
            "dept_id": profile.worker_id,
            "name": profile.name,
            "watch_sensor_id": profile.sensor_id,
            "watch_sensor_name": profile.sensor_name,
            "space_id": profile.space_id,
            "space_name": profile.space_name,
        }
        enriched["measurements"] = {
            "hr": watch.hr,
            "baseline_hr": watch.baseline_hr if watch.baseline_hr is not None else profile.baseline_hr,
            "temp_c": environment.temp_c,
            "humid": environment.humid,
            "temperature_sensor_id": environment.sensor_id,
            "temperature_sensor_name": environment.sensor_name,
            "space_id": environment.space_id,
            "space_name": environment.space_name,
        }
        enriched["health_profile"] = {
            "age": profile.age,
            "gender": profile.gender,
            "height_cm": profile.height_cm,
            "weight_kg": profile.weight_kg,
            "elderly_flag": profile.elderly_flag,
            "heart_disease": profile.heart_disease,
            "hypertension": profile.hypertension,
            "other_disease": profile.other_disease,
        }
        enriched["rest_reason_detail"] = _compose_rest_reason(enriched)
        return enriched


def _compose_rest_reason(prediction: Dict[str, Any]) -> str:
    measurements = prediction.get("measurements") or {}
    health = prediction.get("health_profile") or {}
    parts = [
        f"판단={prediction.get('result', 'unknown')}",
        f"심박={measurements.get('hr', 'unknown')}",
        f"기준심박={measurements.get('baseline_hr', 'unknown')}",
        f"심박변화={prediction.get('hr_delta_from_baseline', 'unknown')}",
        f"열지수={prediction.get('heat_index', 'unknown')}",
        f"온도={measurements.get('temp_c', 'unknown')}",
        f"습도={measurements.get('humid', 'unknown')}",
    ]
    risk_flags = [
        name
        for name, value in (
            ("고령", health.get("elderly_flag")),
            ("심장질환", health.get("heart_disease")),
            ("고혈압", health.get("hypertension")),
            ("기타질환", health.get("other_disease")),
        )
        if int(value or 0) == 1
    ]
    if risk_flags:
        parts.append("개인위험=" + ",".join(risk_flags))
    return " / ".join(parts)
