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
    reset_after_ms: int = 5000

    def to_dict(self, include_target_topic: bool = True) -> Dict[str, Any]:
        print(
            "[BandControlCommand] START to_dict "
            f"include_target_topic={include_target_topic}, target_topic={self.target_topic}"
        )
        data = asdict(self)
        if not include_target_topic:
            data.pop("target_topic", None)
        print(f"[BandControlCommand] END to_dict data={data}")
        return data

    def to_json(self, include_target_topic: bool = True) -> str:
        print(
            "[BandControlCommand] START to_json "
            f"include_target_topic={include_target_topic}"
        )
        payload = json.dumps(
            self.to_dict(include_target_topic=include_target_topic),
            ensure_ascii=False,
        )
        print(f"[BandControlCommand] END to_json payload={payload}")
        return payload

    def to_topic_and_payload(self) -> tuple[str, str]:
        print(f"[BandControlCommand] START to_topic_and_payload target_topic={self.target_topic}")
        payload = self.to_json(include_target_topic=False)
        print(
            "[BandControlCommand] END to_topic_and_payload "
            f"topic={self.target_topic}, payload={payload}"
        )
        return self.target_topic, payload


@dataclass(frozen=True)
class RestRuntimeResult:
    worker_id: str
    prediction: Dict[str, Any]
    command: Optional[BandControlCommand]

    @property
    def should_rest(self) -> bool:
        result = self.command is not None
        print(f"[RestRuntimeResult] should_rest worker_id={self.worker_id}, result={result}")
        return result

    def command_json(self, include_target_topic: bool = True) -> Optional[str]:
        print(
            "[RestRuntimeResult] START command_json "
            f"worker_id={self.worker_id}, include_target_topic={include_target_topic}"
        )
        if self.command is None:
            print("[RestRuntimeResult] END command_json payload=None")
            return None
        payload = self.command.to_json(include_target_topic=include_target_topic)
        print(f"[RestRuntimeResult] END command_json payload={payload}")
        return payload
