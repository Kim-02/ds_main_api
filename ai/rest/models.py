import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class EnvironmentSample:
    temp_c: float
    humid: float


@dataclass(frozen=True)
class WatchSample:
    hr: float
    baseline_hr: Optional[float] = None


@dataclass(frozen=True)
class WorkerProfile:
    worker_id: str
    age: int
    gender: int
    height_cm: float
    weight_kg: float
    work_duration_min: int
    elderly_flag: int
    heart_disease: int
    hypertension: int
    other_disease: int
    target_topic: str


@dataclass(frozen=True)
class BandControlCommand:
    target_topic: str
    command: str = "alert_on"
    color: str = "yellow"
    vibration: bool = True
    led: bool = True
    duration_ms: int = 5000
    reset_after_ms: int = 15000

    def to_dict(self, include_target_topic: bool = True) -> Dict[str, Any]:
        data = asdict(self)
        if not include_target_topic:
            data.pop("target_topic", None)
        return data

    def to_json(self, include_target_topic: bool = True) -> str:
        return json.dumps(
            self.to_dict(include_target_topic=include_target_topic),
            ensure_ascii=False,
        )

    def to_topic_and_payload(self) -> tuple[str, str]:
        return self.target_topic, self.to_json(include_target_topic=False)


@dataclass(frozen=True)
class RestRuntimeResult:
    worker_id: str
    prediction: Dict[str, Any]
    command: Optional[BandControlCommand]

    @property
    def should_rest(self) -> bool:
        return self.command is not None

    def command_json(self, include_target_topic: bool = True) -> Optional[str]:
        if self.command is None:
            return None
        return self.command.to_json(include_target_topic=include_target_topic)
