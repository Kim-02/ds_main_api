import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from database.db_handler import DatabaseHandler

from .models import EnvironmentSample, WatchSample, WorkerProfile


logger = logging.getLogger(__name__)

DEFAULT_AGE = 40
DEFAULT_GENDER = 1
DEFAULT_HEIGHT_CM = 170.0
DEFAULT_WEIGHT_KG = 70.0
DEFAULT_WORK_DURATION_MIN = 0
TEMPERATURE_SENSOR_TYPES = ("temp_humidity", "temperature_humidity", "th", "temperature")


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
        placeholders = ", ".join(["%s"] * len(TEMPERATURE_SENSOR_TYPES))
        row = self._fetch_one(
            f"""
            SELECT
                t.temp AS temp_c,
                t.humid,
                ts.sensor_id AS sensor_id,
                ts.sen_name AS sensor_name,
                ws.space_id AS space_id,
                sp.space_name AS space_name
            FROM worker w
            JOIN sensor ws
              ON w.sen_id = ws.sen_id
            JOIN sensor ts
              ON ts.space_id = ws.space_id
             AND LOWER(ts.sensor_type) IN ({placeholders})
            JOIN th_trans t
              ON t.sen_id = ts.sen_id
            LEFT JOIN ds_space sp
              ON ws.space_id = sp.space_id
            WHERE w.dept_id = %s
            ORDER BY t.time DESC
            LIMIT 1
            """,
            (*TEMPERATURE_SENSOR_TYPES, _coerce_worker_id(worker_id)),
            source_name="environment",
        )
        sample = EnvironmentSample(
            temp_c=_required_float(row, "temp_c"),
            humid=_required_float(row, "humid"),
            sensor_id=str(row.get("sensor_id") or ""),
            sensor_name=str(row.get("sensor_name") or ""),
            space_id=_optional_int(row, "space_id"),
            space_name=str(row.get("space_name") or ""),
        )
        logger.debug(
            "[RestRepository] environment worker_id=%s sensor_id=%s space_id=%s temp=%s humid=%s",
            worker_id,
            sample.sensor_id,
            sample.space_id,
            sample.temp_c,
            sample.humid,
        )
        return sample

    def fetch_watch(self, worker_id: str) -> WatchSample:
        row = self._fetch_one(
            """
            SELECT
                h.hr,
                wh.baseline_hr,
                s.sensor_id AS sensor_id,
                s.sen_name AS sensor_name
            FROM hb_trans h
            JOIN worker w
              ON h.sen_id = w.sen_id
            JOIN worker_hr_data wh
              ON wh.dept_id = w.dept_id
            JOIN sensor s
              ON s.sen_id = w.sen_id
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
            sensor_id=str(row.get("sensor_id") or ""),
            sensor_name=str(row.get("sensor_name") or ""),
        )
        logger.debug(
            "[RestRepository] watch worker_id=%s sensor_id=%s hr=%s baseline_hr=%s",
            worker_id,
            sample.sensor_id,
            sample.hr,
            sample.baseline_hr,
        )
        return sample

    def fetch_worker_profile(self, worker_id: str) -> WorkerProfile:
        row = self._fetch_one(
            """
            SELECT
                w.dept_id,
                w.name,
                w.is_manager,
                w.sen_id,
                wh.age,
                wh.gender,
                wh.height_cm,
                wh.weight_kg,
                wh.elderly_flag,
                wh.heart_disease,
                wh.hypertension,
                wh.other_disease,
                wh.baseline_hr,
                s.sensor_id AS sensor_id,
                s.sen_name AS sensor_name,
                s.mqtt_topic AS mqtt_topic,
                s.space_id AS space_id,
                sp.space_name AS space_name
            FROM worker w
            JOIN worker_hr_data wh
              ON wh.dept_id = w.dept_id
            LEFT JOIN sensor s
              ON w.sen_id = s.sen_id
            LEFT JOIN ds_space sp
              ON s.space_id = sp.space_id
            WHERE w.dept_id = %s
            LIMIT 1
            """,
            (_coerce_worker_id(worker_id),),
            source_name="worker",
        )

        worker_id_value = str(_required_value(row, "dept_id"))
        age = int(_required_float(row, "age"))
        work_duration_min = int(self.default_work_duration_min)

        profile = WorkerProfile(
            worker_id=worker_id_value,
            age=age,
            gender=_encode_gender(_required_value(row, "gender")),
            height_cm=_required_float(row, "height_cm"),
            weight_kg=_required_float(row, "weight_kg"),
            work_duration_min=work_duration_min,
            elderly_flag=_to_int_flag(_required_value(row, "elderly_flag")),
            heart_disease=_to_int_flag(_required_value(row, "heart_disease")),
            hypertension=_to_int_flag(_required_value(row, "hypertension")),
            other_disease=_to_int_flag(_required_value(row, "other_disease")),
            target_topic=_resolve_target_topic(row),
            baseline_hr=_optional_float(row, "baseline_hr"),
            name=str(row.get("name") or ""),
            sensor_id=str(row.get("sensor_id") or ""),
            sensor_name=str(row.get("sensor_name") or ""),
            space_id=_optional_int(row, "space_id"),
            space_name=str(row.get("space_name") or ""),
        )
        logger.debug(
            "[RestRepository] worker_profile worker_id=%s name=%s sensor_id=%s space_id=%s age=%s baseline_hr=%s",
            profile.worker_id,
            profile.name,
            profile.sensor_id,
            profile.space_id,
            profile.age,
            profile.baseline_hr,
        )
        return profile
    def fetch_worker_vlm_context(self, worker_id: str) -> dict[str, Any]:
        """
        VLM 프롬프트 생성용 작업자 통합 context를 반환한다.

        목적:
        - VLM 화면 출력 3개 항목 생성에 필요한 데이터만 모은다.
        - worker, worker_hr_data, sensor, ds_space, hb_trans, th_trans 데이터를 통합한다.
        - 센서가 없거나 최신 측정값이 없어도 VLM 프롬프트 생성이 중단되지 않게 한다.

        반환 구조:
        {
            "worker": {...},
            "watch": {...},
            "environment": {...},
            "space": {...}
        }
        """
        worker_row = self._fetch_one(
            """
            SELECT
                w.dept_id,
                w.name,
                w.is_manager,
                w.sen_id,

                wh.age,
                wh.gender,
                wh.height_cm,
                wh.weight_kg,
                wh.elderly_flag,
                wh.heart_disease,
                wh.hypertension,
                wh.other_disease,
                wh.baseline_hr,

                s.sensor_id AS watch_sensor_id,
                s.sen_name AS watch_sensor_name,
                s.mqtt_topic AS watch_mqtt_topic,
                s.space_id AS space_id,

                sp.space_name AS space_name,
                sp.hazard_type AS hazard_type,
                sp.is_hazard AS is_hazard
            FROM worker w
            LEFT JOIN worker_hr_data wh
            ON wh.dept_id = w.dept_id
            LEFT JOIN sensor s
            ON w.sen_id = s.sen_id
            LEFT JOIN ds_space sp
            ON s.space_id = sp.space_id
            WHERE w.dept_id = %s
            LIMIT 1
            """,
            (_coerce_worker_id(worker_id),),
            source_name="worker_vlm_context",
        )

        watch_row = self._fetch_optional(
            """
            SELECT
                h.hr,
                h.time AS measured_at,
                s.sensor_id AS sensor_id,
                s.sen_name AS sensor_name
            FROM worker w
            JOIN hb_trans h
            ON h.sen_id = w.sen_id
            LEFT JOIN sensor s
            ON s.sen_id = w.sen_id
            WHERE w.dept_id = %s
            ORDER BY h.time DESC
            LIMIT 1
            """,
            (_coerce_worker_id(worker_id),),
        )

        environment_row = self.fetch_environment_optional(worker_id)

        baseline_hr = _optional_float(worker_row, "baseline_hr")
        hr = _optional_float(watch_row or {}, "hr")

        hr_delta_from_baseline = None
        if hr is not None and baseline_hr is not None:
            hr_delta_from_baseline = round(hr - baseline_hr, 2)

        environment = {
            "temperature_c": None,
            "humidity_pct": None,
            "heat_index_c": None,
            "sensor_id": "",
            "sensor_name": "",
            "space_id": _optional_int(worker_row, "space_id"),
            "space_name": str(worker_row.get("space_name") or ""),
        }

        if environment_row:
            temp_c = _optional_float(environment_row, "temp_c")
            humid = _optional_float(environment_row, "humid")

            environment.update(
                {
                    "temperature_c": temp_c,
                    "humidity_pct": humid,
                    "heat_index_c": _calculate_heat_index_c(temp_c, humid),
                    "sensor_id": str(environment_row.get("sensor_id") or ""),
                    "sensor_name": str(environment_row.get("sensor_name") or ""),
                    "space_id": _optional_int(environment_row, "space_id"),
                    "space_name": str(environment_row.get("space_name") or ""),
                }
            )

        return {
            "worker": {
                "worker_id": str(worker_row.get("dept_id") or ""),
                "worker_name": str(worker_row.get("name") or ""),
                "is_manager": _optional_int(worker_row, "is_manager"),
                "watch_sen_id": worker_row.get("sen_id"),
                "watch_sensor_id": str(worker_row.get("watch_sensor_id") or ""),
                "watch_sensor_name": str(worker_row.get("watch_sensor_name") or ""),
                "watch_mqtt_topic": str(worker_row.get("watch_mqtt_topic") or ""),
            },
            "watch": {
                "hr": hr,
                "baseline_hr": baseline_hr,
                "hr_delta_from_baseline": hr_delta_from_baseline,
                "measured_at": _serialize_datetime((watch_row or {}).get("measured_at")),
                "sensor_id": str((watch_row or {}).get("sensor_id") or worker_row.get("watch_sensor_id") or ""),
                "sensor_name": str((watch_row or {}).get("sensor_name") or worker_row.get("watch_sensor_name") or ""),
            },
            "health_profile": {
                "age": _optional_int(worker_row, "age"),
                "gender": _encode_gender(worker_row.get("gender")) if worker_row.get("gender") is not None else None,
                "height_cm": _optional_float(worker_row, "height_cm"),
                "weight_kg": _optional_float(worker_row, "weight_kg"),
                "elderly_flag": _optional_int(worker_row, "elderly_flag"),
                "heart_disease": _optional_int(worker_row, "heart_disease"),
                "hypertension": _optional_int(worker_row, "hypertension"),
                "other_disease": _optional_int(worker_row, "other_disease"),
                "baseline_hr": baseline_hr,
            },
            "space": {
                "space_id": _optional_int(worker_row, "space_id"),
                "space_name": str(worker_row.get("space_name") or ""),
                "hazard_type": str(worker_row.get("hazard_type") or ""),
                "is_hazard": _optional_int(worker_row, "is_hazard"),
            },
            "environment": environment,
        }

    def fetch_environment_optional(self, worker_id: str) -> Optional[dict[str, Any]]:
        """
        VLM용 온습도 조회.
        기존 fetch_environment()와 같은 의미지만,
        온습도 센서가 없거나 작업자 센서가 공간에 매핑되지 않아도 예외를 발생시키지 않는다.
        """
        placeholders = ", ".join(["%s"] * len(TEMPERATURE_SENSOR_TYPES))

        return self._fetch_optional(
            f"""
            SELECT
                t.temp AS temp_c,
                t.humid AS humid,
                ts.sensor_id AS sensor_id,
                ts.sen_name AS sensor_name,
                ws.space_id AS space_id,
                sp.space_name AS space_name
            FROM worker w
            JOIN sensor ws
              ON w.sen_id = ws.sen_id
            JOIN sensor ts
              ON ts.space_id = ws.space_id
             AND LOWER(ts.sensor_type) IN ({placeholders})
            JOIN th_trans t
              ON t.sen_id = ts.sen_id
            LEFT JOIN ds_space sp
              ON ws.space_id = sp.space_id
            WHERE w.dept_id = %s
            ORDER BY t.time DESC
            LIMIT 1
            """,
            (*TEMPERATURE_SENSOR_TYPES, _coerce_worker_id(worker_id)),
        )

    def build_worker_vlm_trigger(
        self,
        worker_id: str,
        *,
        regression_result: dict[str, Any] | None = None,
        body_temperature_c: float | None = None,
        triggered_at: str | None = None,
    ) -> dict[str, Any]:
        """
        VLM prompt_builder가 바로 사용할 수 있는 trigger를 생성한다.

        regression_result 예:
        {
            "result": "강한휴식권고",
            "reason": "기준 심박 대비 상승",
            "rest_reason_detail": "기준 심박 75 대비 현재 심박 102",
            "probabilities": {...}
        }
        """
        context = self.fetch_worker_vlm_context(worker_id)
        regression_result = regression_result or {}

        worker = context["worker"]
        watch = context["watch"]
        health_profile = context["health_profile"]
        space = context["space"]
        environment = context["environment"]

        result = (
            regression_result.get("result")
            or regression_result.get("label")
            or regression_result.get("prediction")
        )

        return {
            "trigger_type": "worker_regression",
            "worker_id": worker.get("worker_id"),
            "watch_sensor_id": worker.get("watch_sensor_id") or watch.get("sensor_id"),
            "triggered_at": triggered_at or datetime.now().isoformat(timespec="seconds"),

            "prediction": {
                "worker": {
                    "dept_id": worker.get("worker_id"),
                    "name": worker.get("worker_name"),
                    "is_manager": worker.get("is_manager"),
                    "watch_sensor_id": worker.get("watch_sensor_id") or watch.get("sensor_id"),

                    "space_id": space.get("space_id"),
                    "space_name": space.get("space_name"),
                    "hazard_type": space.get("hazard_type"),
                    "is_hazard": space.get("is_hazard"),
                },

                "measurements": {
                    "hr": watch.get("hr"),
                    "baseline_hr": watch.get("baseline_hr"),
                    "hr_delta_from_baseline": watch.get("hr_delta_from_baseline"),
                    "body_temperature_c": body_temperature_c,

                    "space_id": space.get("space_id"),
                    "space_name": space.get("space_name"),
                },

                "health_profile": health_profile,

                "result": result,
                "reason": regression_result.get("reason"),
                "rest_reason_detail": regression_result.get("rest_reason_detail"),
                "probabilities": regression_result.get("probabilities"),
            },

            "environment": environment,
        }
    def fetch_last_hr_time(self, sensor_id: str) -> Optional[datetime]:
        """hb_trans 에서 이 센서의 가장 최근 심박 수신 시각을 반환한다.

        sensor.last_seen_at 은 status ping 만으로도 갱신되므로
        실제로 워치를 착용 중인지 판단하기에 부적합하다.
        hb_trans 의 실제 심박 데이터 시각을 기준으로 삼는다.
        """
        row = self._fetch_optional(
            """
            SELECT h.time AS last_hr_time
            FROM hb_trans h
            JOIN sensor s ON h.sen_id = s.sen_id
            WHERE s.sensor_id = %s
            ORDER BY h.time DESC
            LIMIT 1
            """,
            (sensor_id,),
        )
        if not row or row.get("last_hr_time") is None:
            return None
        value = row["last_hr_time"]
        if isinstance(value, datetime):
            return value
        return _to_datetime(value)

    def find_worker_id_by_sensor_id(self, sensor_id: str) -> Optional[str]:
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
            return None
        return str(row["dept_id"])

    def _fetch_one(
        self,
        query: str,
        params: tuple[Any, ...],
        *,
        source_name: str,
    ) -> dict[str, Any]:
        row = self._fetch_optional(query, params)
        if row is None:
            raise LookupError(f"No {source_name} row found for params={params}")
        return row

    def _fetch_optional(
        self,
        query: str,
        params: tuple[Any, ...],
    ) -> Optional[dict[str, Any]]:
        with self.db_handler._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
        return dict(row) if row else None


def _resolve_target_topic(row: dict[str, Any]) -> str:
    sensor_id = row.get("sensor_id")
    if sensor_id:
        return f"sensors/{sensor_id}/alert"

    mqtt_topic = str(row.get("mqtt_topic") or "").strip()
    if mqtt_topic:
        if mqtt_topic.endswith("/telemetry"):
            return mqtt_topic[: -len("/telemetry")] + "/alert"
        return mqtt_topic.rstrip("/") + "/alert"

    raise ValueError("작업자에게 매핑된 heart_band 센서가 없어 휴식 명령 topic을 만들 수 없습니다.")


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


def _required_value(row: dict[str, Any], column: str) -> Any:
    if column not in row:
        raise KeyError(f"Query result is missing required column '{column}'")
    value = row[column]
    if value is None:
        raise ValueError(f"Column '{column}' is null")
    return value


def _required_float(row: dict[str, Any], column: str) -> float:
    return _to_float(_required_value(row, column), column)


def _optional_float(row: dict[str, Any], column: str, default: float | None = None) -> Optional[float]:
    if column not in row:
        return default
    value = row[column]
    if value is None or value == "":
        return default
    return _to_float(value, column)


def _optional_int(row: dict[str, Any], column: str, default: int | None = None) -> Optional[int]:
    value = _optional_float(row, column, None)
    if value is None:
        return default
    return int(value)


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

def _serialize_datetime(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)


def _calculate_heat_index_c(temp_c: float | None, humid: float | None) -> float | None:
    """
    간단한 열지수 계산 함수.
    입력값이 없거나, 온도가 낮은 경우에는 None을 반환한다.

    VLM 프롬프트에서는 이 값을 '상황 조치 방법'에만 사용하고,
    '건강정보 전달'의 직접 근거로 쓰지 않는다.
    """
    if temp_c is None or humid is None:
        return None

    try:
        t_c = float(temp_c)
        rh = float(humid)
    except (TypeError, ValueError):
        return None

    # 일반적으로 열지수는 고온 조건에서 의미가 크므로 낮은 온도에서는 생략
    if t_c < 26.0:
        return None

    t_f = (t_c * 9.0 / 5.0) + 32.0

    hi_f = (
        -42.379
        + 2.04901523 * t_f
        + 10.14333127 * rh
        - 0.22475541 * t_f * rh
        - 0.00683783 * t_f * t_f
        - 0.05481717 * rh * rh
        + 0.00122874 * t_f * t_f * rh
        + 0.00085282 * t_f * rh * rh
        - 0.00000199 * t_f * t_f * rh * rh
    )

    hi_c = (hi_f - 32.0) * 5.0 / 9.0
    return round(hi_c, 2)