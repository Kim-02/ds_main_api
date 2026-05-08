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
        duration_ms: int = 5000,
        reset_after_ms: int = 15000,
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
            baseline_hr=watch.baseline_hr,
        )

        prediction = self.engine.predict(raw)
        command = None
        if self.should_send_rest_command(prediction):
            command = self.command_builder.build(profile.target_topic)

        return RestRuntimeResult(
            worker_id=profile.worker_id,
            prediction=prediction,
            command=command,
        )

    @staticmethod
    def should_send_rest_command(prediction: Dict[str, Any]) -> bool:
        return prediction.get("result") in REST_REQUIRED_RESULTS
