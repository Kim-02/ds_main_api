from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from database.db_handler import DatabaseHandler

from .models import EnvironmentSample, WatchSample, WorkerProfile


DEFAULT_AGE = 40
DEFAULT_GENDER = 1
DEFAULT_HEIGHT_CM = 170.0
DEFAULT_WEIGHT_KG = 70.0
DEFAULT_WORK_DURATION_MIN = 0


class DatabaseHandlerRestDataRepository:
    """Rest runtime repository backed by the project's existing MariaDB schema."""

    def __init__(
        self,
        db_handler: "DatabaseHandler",
        *,
        default_age: int = DEFAULT_AGE,
        default_gender: int = DEFAULT_GENDER,
        default_height_cm: float = DEFAULT_HEIGHT_CM,
        default_weight_kg: float = DEFAULT_WEIGHT_KG,
        default_work_duration_min: int = DEFAULT_WORK_DURATION_MIN,
    ):
        self.db_handler = db_handler
        self.default_age = default_age
        self.default_gender = default_gender
        self.default_height_cm = default_height_cm
        self.default_weight_kg = default_weight_kg
        self.default_work_duration_min = default_work_duration_min

    def fetch_environment(self, worker_id: str) -> EnvironmentSample:
        print(f"[RestRepository] START fetch_environment worker_id={worker_id}")
        row = self._fetch_one(
            """
            SELECT
                temp AS temp_c,
                humid
            FROM th_trans
            ORDER BY time DESC
            LIMIT 1
            """,
            (),
            source_name="environment",
        )
        sample = EnvironmentSample(
            temp_c=_required_float(row, "temp_c"),
            humid=_required_float(row, "humid"),
        )
        print(f"[RestRepository] END fetch_environment sample={sample}")
        return sample

    def fetch_watch(self, worker_id: str) -> WatchSample:
        print(f"[RestRepository] START fetch_watch worker_id={worker_id}")
        row = self._fetch_one(
            """
            SELECT
                h.hr
            FROM hb_trans h
            JOIN worker w
              ON h.sen_id = w.sen_id
            WHERE w.dept_id = %s
            ORDER BY h.time DESC
            LIMIT 1
            """,
            (_coerce_worker_id(worker_id),),
            source_name="watch",
        )
        sample = WatchSample(
            hr=_required_float(row, "hr"),
            baseline_hr=_optional_float(row, "baseline_hr"),
        )
        print(f"[RestRepository] END fetch_watch sample={sample}")
        return sample

    def fetch_worker_profile(self, worker_id: str) -> WorkerProfile:
        print(f"[RestRepository] START fetch_worker_profile worker_id={worker_id}")
        row = self._fetch_one(
            """
            SELECT
                w.*,
                s.sensor_id AS sensor_id,
                s.mqtt_topic AS mqtt_topic
            FROM worker w
            LEFT JOIN sensor s
              ON w.sen_id = s.sen_id
            WHERE w.dept_id = %s
            LIMIT 1
            """,
            (_coerce_worker_id(worker_id),),
            source_name="worker",
        )

        worker_id_value = str(_required_value(row, "dept_id"))
        age = int(_optional_float(row, "age", self.default_age))
        work_duration_min = _resolve_work_duration_min(
            row,
            default=self.default_work_duration_min,
        )

        profile = WorkerProfile(
            worker_id=worker_id_value,
            age=age,
            gender=_encode_gender(_pick(row, ("gender", "sex"), self.default_gender)),
            height_cm=_optional_float(row, "height_cm", self.default_height_cm),
            weight_kg=_optional_float(row, "weight_kg", self.default_weight_kg),
            work_duration_min=work_duration_min,
            elderly_flag=_to_int_flag(_pick(row, ("elderly_flag",), int(age >= 60))),
            heart_disease=_to_int_flag(_pick(row, ("heart_disease",), 0)),
            hypertension=_to_int_flag(_pick(row, ("hypertension",), 0)),
            other_disease=_to_int_flag(_pick(row, ("other_disease",), 0)),
            target_topic=_resolve_target_topic(row),
        )
        print(f"[RestRepository] END fetch_worker_profile profile={profile}")
        return profile

    def find_worker_id_by_sensor_id(self, sensor_id: str) -> Optional[str]:
        print(f"[RestRepository] START find_worker_id_by_sensor_id sensor_id={sensor_id}")
        row = self._fetch_optional(
            """
            SELECT
                w.dept_id
            FROM worker w
            JOIN sensor s
              ON w.sen_id = s.sen_id
            WHERE s.sensor_id = %s
            LIMIT 1
            """,
            (sensor_id,),
        )
        if not row:
            print(f"[RestRepository] END find_worker_id_by_sensor_id sensor_id={sensor_id}, worker_id=None")
            return None
        worker_id = str(row["dept_id"])
        print(
            "[RestRepository] END find_worker_id_by_sensor_id "
            f"sensor_id={sensor_id}, worker_id={worker_id}"
        )
        return worker_id

    def _fetch_one(
        self,
        query: str,
        params: tuple[Any, ...],
        *,
        source_name: str,
    ) -> dict[str, Any]:
        print(f"[RestRepository] START _fetch_one source={source_name}, params={params}")
        row = self._fetch_optional(query, params)
        if row is None:
            print(f"[RestRepository] END _fetch_one source={source_name}, row=None")
            raise LookupError(f"No {source_name} row found for params={params}")
        print(f"[RestRepository] END _fetch_one source={source_name}, row={row}")
        return row

    def _fetch_optional(
        self,
        query: str,
        params: tuple[Any, ...],
    ) -> Optional[dict[str, Any]]:
        compact_query = " ".join(query.split())
        print(
            "[RestRepository] START _fetch_optional "
            f"sql={compact_query[:180]}, params={params}"
        )
        with self.db_handler._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
        result = dict(row) if row else None
        print(f"[RestRepository] END _fetch_optional row={result}")
        return result


def _resolve_target_topic(row: dict[str, Any]) -> str:
    print(f"[RestRepository] START _resolve_target_topic row_sensor_id={row.get('sensor_id')}, mqtt_topic={row.get('mqtt_topic')}")
    sensor_id = row.get("sensor_id")
    if sensor_id:
        topic = f"sensors/{sensor_id}/alert"
        print(f"[RestRepository] END _resolve_target_topic source=sensor_id topic={topic}")
        return topic

    mqtt_topic = str(row.get("mqtt_topic") or "").strip()
    if mqtt_topic:
        if mqtt_topic.endswith("/telemetry"):
            topic = mqtt_topic[: -len("/telemetry")] + "/alert"
            print(f"[RestRepository] END _resolve_target_topic source=mqtt_topic topic={topic}")
            return topic
        topic = mqtt_topic.rstrip("/") + "/alert"
        print(f"[RestRepository] END _resolve_target_topic source=mqtt_topic topic={topic}")
        return topic

    print("[RestRepository] END _resolve_target_topic failed=no_sensor_topic")
    raise ValueError("작업자에게 매핑된 heart_band 센서가 없어 휴식 명령 topic을 만들 수 없습니다.")


def _resolve_work_duration_min(row: dict[str, Any], *, default: int) -> int:
    print(
        "[RestRepository] START _resolve_work_duration_min "
        f"work_duration_min={row.get('work_duration_min')}, default={default}"
    )
    if "work_duration_min" in row and row["work_duration_min"] not in (None, ""):
        result = int(_to_float(row["work_duration_min"], "work_duration_min"))
        print(f"[RestRepository] END _resolve_work_duration_min source=column result={result}")
        return result

    for key in ("work_start_at", "shift_start_at", "started_at"):
        value = row.get(key)
        if not value:
            continue
        start = _to_datetime(value)
        if start:
            result = max(0, int((datetime.now() - start).total_seconds() // 60))
            print(f"[RestRepository] END _resolve_work_duration_min source={key} result={result}")
            return result

    result = int(default)
    print(f"[RestRepository] END _resolve_work_duration_min source=default result={result}")
    return result


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("T", " ").replace("Z", ""))
        except ValueError:
            return None
    return None


def _coerce_worker_id(worker_id: str) -> int | str:
    text = str(worker_id).strip()
    if text.isdigit():
        return int(text)
    return text


def _pick(row: dict[str, Any], names: tuple[str, ...], default: Any = None) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return default


def _required_value(row: dict[str, Any], column: str) -> Any:
    if column not in row:
        raise KeyError(f"Query result is missing required column '{column}'")

    value = row[column]
    if value is None:
        raise ValueError(f"Column '{column}' is null")
    return value


def _required_float(row: dict[str, Any], column: str) -> float:
    value = _required_value(row, column)
    return _to_float(value, column)


def _optional_float(row: dict[str, Any], column: str, default: float | None = None) -> Optional[float]:
    if column not in row:
        return default

    value = row[column]
    if value is None or value == "":
        return default
    return _to_float(value, column)


def _to_float(value: Any, column: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Column '{column}' must be numeric: {value!r}") from exc


def _to_int_flag(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)

    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y"}:
        return 1
    if text in {"false", "f", "no", "n"}:
        return 0
    return int(float(value))


def _encode_gender(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().lower()
    if text in {"m", "male", "남", "남성"}:
        return 1
    if text in {"f", "female", "여", "여성"}:
        return 0

    try:
        return int(float(text))
    except ValueError:
        return -1
