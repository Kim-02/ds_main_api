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
        duration_ms: int = 5000,
        reset_after_ms: int = 5000,
    ):
        self.color = color
        self.vibration = vibration
        self.led = led
        self.duration_ms = duration_ms
        self.reset_after_ms = reset_after_ms

    def build(self, target_topic: str) -> BandControlCommand:
        print(f"[RestCommandBuilder] START build target_topic={target_topic}")
        command = BandControlCommand(
            target_topic=target_topic,
            color=self.color,
            vibration=self.vibration,
            led=self.led,
            duration_ms=self.duration_ms,
            reset_after_ms=self.reset_after_ms,
        )
        print(f"[RestCommandBuilder] END build command={command.to_dict()}")
        return command

    def build_for_prediction(self, target_topic: str, prediction: Dict[str, Any]) -> BandControlCommand:
        result = prediction.get("result")
        color = REST_COMMAND_COLORS.get(result, self.color)
        print(
            "[RestCommandBuilder] START build_for_prediction "
            f"target_topic={target_topic}, result={result}, color={color}, "
            f"duration_ms={self.duration_ms}, reset_after_ms={self.reset_after_ms}"
        )
        command = BandControlCommand(
            target_topic=target_topic,
            color=color,
            vibration=self.vibration,
            led=self.led,
            duration_ms=self.duration_ms,
            reset_after_ms=self.reset_after_ms,
        )
        print(f"[RestCommandBuilder] END build_for_prediction command={command.to_dict()}")
        return command


class RestRuntimeService:
    def __init__(
        self,
        repository: RestDataRepository,
        engine: RestModelEngine,
        command_builder: Optional[BandControlCommandBuilder] = None,
    ):
        print(
            "[RestRuntimeService] INIT "
            f"repository={repository.__class__.__name__}, "
            f"engine={engine.__class__.__name__}, "
            f"command_builder={(command_builder or BandControlCommandBuilder()).__class__.__name__}"
        )
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
        print(
            "[RestRuntimeService] START from_model_path "
            f"model_path={resolved_model_path}, "
            f"forced_rest_work_min={forced_rest_work_min}"
        )
        engine = RestModelEngine(
            model_path=str(resolved_model_path),
            forced_rest_work_min=forced_rest_work_min,
        )
        service = cls(
            repository=repository,
            engine=engine,
            command_builder=command_builder,
        )
        print("[RestRuntimeService] END from_model_path")
        return service

    def evaluate_worker(self, worker_id: str) -> RestRuntimeResult:
        print(f"[RestRuntimeService] START evaluate_worker worker_id={worker_id}")
        print("[RestRuntimeService] -> repository.fetch_environment START")
        environment = self.repository.fetch_environment(worker_id)
        print(f"[RestRuntimeService] <- repository.fetch_environment END data={environment}")

        print("[RestRuntimeService] -> repository.fetch_watch START")
        watch = self.repository.fetch_watch(worker_id)
        print(f"[RestRuntimeService] <- repository.fetch_watch END data={watch}")

        print("[RestRuntimeService] -> repository.fetch_worker_profile START")
        profile = self.repository.fetch_worker_profile(worker_id)
        print(f"[RestRuntimeService] <- repository.fetch_worker_profile END data={profile}")

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
        print(f"[RestRuntimeService] raw input built={raw}")

        print("[RestRuntimeService] -> engine.predict START")
        prediction = self.engine.predict(raw)
        print(f"[RestRuntimeService] <- engine.predict END prediction={prediction}")

        command = None
        should_send = self.should_send_rest_command(prediction)
        print(
            "[RestRuntimeService] command decision "
            f"result={prediction.get('result')}, should_send={should_send}"
        )
        if should_send:
            command = self.command_builder.build_for_prediction(profile.target_topic, prediction)

        result = RestRuntimeResult(
            worker_id=profile.worker_id,
            prediction=prediction,
            command=command,
        )
        print(
            "[RestRuntimeService] END evaluate_worker "
            f"worker_id={worker_id}, should_rest={result.should_rest}, "
            f"command={result.command.to_dict() if result.command else None}"
        )
        return result

    @staticmethod
    def should_send_rest_command(prediction: Dict[str, Any]) -> bool:
        result = prediction.get("result")
        should_send = result in REST_REQUIRED_RESULTS
        print(
            "[RestRuntimeService] should_send_rest_command "
            f"result={result}, required={REST_REQUIRED_RESULTS}, should_send={should_send}"
        )
        return should_send
