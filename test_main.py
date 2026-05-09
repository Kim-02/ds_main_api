import argparse
import json
import os
from pathlib import Path
from typing import Any

from ai.rest import (
    DEFAULT_MODEL_PATH,
    BandControlCommandBuilder,
    DatabaseHandlerRestDataRepository,
    RestModelEngine,
    RestRuntimeService,
    WorkerRawInput,
)
from database.db_handler import DatabaseHandler


DEFAULT_SENSOR_ID = "watch-1386"


WORKER_HR_DATA_COLUMNS = (
    "worker_hr_data_id",
    "dept_id",
    "age",
    "gender",
    "height_cm",
    "weight_kg",
    "elderly_flag",
    "heart_disease",
    "hypertension",
    "other_disease",
    "baseline_hr",
    "created_at",
    "updated_at",
)


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        print(f"[test_main] .env 파일 없음: {env_path.resolve()}")
        return

    print(f"[test_main] .env 로드 시작: {env_path.resolve()}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))
    print("[test_main] .env 로드 완료")


def build_db_handler() -> DatabaseHandler:
    print("[test_main] DB 핸들러 생성 시작")
    handler = DatabaseHandler(
        host=os.getenv("MARIADB_HOST", "127.0.0.1"),
        user=os.getenv("MARIADB_USER", "root"),
        password=os.getenv("MARIADB_PASSWORD", "ekthf123"),
        db_name=os.getenv("MARIADB_DB_NAME", "ON_SAFE"),
        port=int(os.getenv("MARIADB_PORT", "3306")),
    )
    print(f"[test_main] DB 핸들러 생성 완료 config={handler.db_config | {'password': '***'}}")
    return handler


def find_worker_by_sensor_id(sensor_id: str) -> str | None:
    print(f"[test_main] 센서 기반 작업자 조회 시작 sensor_id={sensor_id}")
    load_dotenv()

    db_handler = build_db_handler()
    repository = DatabaseHandlerRestDataRepository(db_handler)

    worker_id = repository.find_worker_id_by_sensor_id(sensor_id)
    print(f"[test_main] 센서 기반 작업자 조회 완료 sensor_id={sensor_id}, worker_id={worker_id}")
    return worker_id


def fetch_worker_hr_data(db_handler: DatabaseHandler, dept_id: str | int) -> dict[str, Any] | None:
    print(f"[test_main] worker_hr_data 조회 시작 dept_id={dept_id}")
    columns = ", ".join(WORKER_HR_DATA_COLUMNS)
    query = f"""
        SELECT
            {columns}
        FROM worker_hr_data
        WHERE dept_id = %s
        LIMIT 1
    """
    compact_query = " ".join(query.split())
    print(f"[test_main] worker_hr_data SQL={compact_query}, params=({dept_id},)")

    with db_handler._get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dept_id,))
            row = cursor.fetchone()

    result = dict(row) if row else None
    print(f"[test_main] worker_hr_data 조회 완료 row={result}")
    return result


def build_raw_input_from_worker_hr_data(
    *,
    worker_id: str,
    worker_hr_data: dict[str, Any],
    hr: float,
    temp_c: float,
    humid: float,
    work_duration_min: int,
) -> WorkerRawInput:
    print(
        "[test_main] 모델 raw input 생성 시작 "
        f"worker_id={worker_id}, hr={hr}, temp_c={temp_c}, humid={humid}, "
        f"work_duration_min={work_duration_min}, worker_hr_data={worker_hr_data}"
    )
    raw = WorkerRawInput(
        worker_id=str(worker_id),
        hr=float(hr),
        temp_c=float(temp_c),
        humid=float(humid),
        age=_required_int(worker_hr_data, "age"),
        gender=_coerce_gender(worker_hr_data.get("gender")),
        height_cm=_required_float(worker_hr_data, "height_cm"),
        weight_kg=_required_float(worker_hr_data, "weight_kg"),
        work_duration_min=int(work_duration_min),
        elderly_flag=_int_flag(worker_hr_data.get("elderly_flag")),
        heart_disease=_int_flag(worker_hr_data.get("heart_disease")),
        hypertension=_int_flag(worker_hr_data.get("hypertension")),
        other_disease=_int_flag(worker_hr_data.get("other_disease")),
        baseline_hr=_optional_float(worker_hr_data.get("baseline_hr")),
    )
    print(f"[test_main] 모델 raw input 생성 완료 raw={raw}")
    return raw


def run_rest_model(raw: WorkerRawInput) -> dict[str, Any]:
    print(f"[test_main] 회귀모델 실행 시작 model_path={DEFAULT_MODEL_PATH}")
    engine = RestModelEngine(model_path=str(DEFAULT_MODEL_PATH))
    prediction = engine.predict(raw)
    print(f"[test_main] 회귀모델 실행 완료 prediction={prediction}")
    return prediction


def run_worker_hr_data_pipeline(sensor_id: str) -> dict[str, Any] | None:
    print(f"[test_main] 파이프라인 시작 sensor_id={sensor_id}")
    load_dotenv()

    db_handler = build_db_handler()
    repository = DatabaseHandlerRestDataRepository(db_handler)

    print("[test_main] 1단계: sensor_id로 worker.dept_id 조회 시작")
    worker_id = repository.find_worker_id_by_sensor_id(sensor_id)
    print(f"[test_main] 1단계 완료 worker_id={worker_id}")

    if worker_id is None:
        print(f"[test_main] 파이프라인 종료: sensor_id={sensor_id}에 연결된 작업자가 없습니다.")
        return None

    print("[test_main] 2단계: worker_hr_data 조회 시작")
    worker_hr_data = fetch_worker_hr_data(db_handler, worker_id)
    print(f"[test_main] 2단계 완료 worker_hr_data={worker_hr_data}")

    if worker_hr_data is None:
        output = {
            "sensor_id": sensor_id,
            "worker_id": worker_id,
            "worker_hr_data": None,
            "raw_input": None,
            "prediction": None,
            "command": None,
        }
        print(
            "[test_main] 파이프라인 종료: worker_hr_data 없음 output="
            + json.dumps(output, ensure_ascii=False, default=str, indent=2)
        )
        return output

    print("[test_main] 3단계: 온습도/심박/작업시간 조회 시작")
    environment = repository.fetch_environment(worker_id)
    watch = repository.fetch_watch(worker_id)
    profile = repository.fetch_worker_profile(worker_id)
    print(
        "[test_main] 3단계 완료 "
        f"environment={environment}, watch={watch}, profile_work_duration_min={profile.work_duration_min}"
    )

    print("[test_main] 4단계: 회귀모델 입력 생성 시작")
    raw_input = build_raw_input_from_worker_hr_data(
        worker_id=worker_id,
        worker_hr_data=worker_hr_data,
        hr=watch.hr,
        temp_c=environment.temp_c,
        humid=environment.humid,
        work_duration_min=profile.work_duration_min,
    )
    print(f"[test_main] 4단계 완료 raw_input={raw_input}")

    print("[test_main] 5단계: 회귀모델 예측 시작")
    prediction = run_rest_model(raw_input)
    print(f"[test_main] 5단계 완료 prediction={prediction}")

    print("[test_main] 6단계: 워치 알림 명령 생성 여부 판단 시작")
    command = None
    if RestRuntimeService.should_send_rest_command(prediction):
        command = BandControlCommandBuilder().build(profile.target_topic).to_dict()
    print(f"[test_main] 6단계 완료 command={command}")

    output = {
        "sensor_id": sensor_id,
        "worker_id": worker_id,
        "worker_hr_data": worker_hr_data,
        "environment": environment,
        "watch": watch,
        "work_duration_min": profile.work_duration_min,
        "raw_input": raw_input,
        "prediction": prediction,
        "command": command,
    }
    print(
        "[test_main] 파이프라인 완료 output="
        + json.dumps(output, ensure_ascii=False, default=str, indent=2)
    )
    return output


def _required_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"worker_hr_data.{key} 값이 필요합니다.")
    return int(value)


def _required_float(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    if value in (None, ""):
        raise ValueError(f"worker_hr_data.{key} 값이 필요합니다.")
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _int_flag(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1"}:
            return 1
        if normalized in {"false", "no", "n", "0"}:
            return 0
    return 1 if int(value) else 0


def _coerce_gender(value: Any) -> int:
    if value in (None, ""):
        raise ValueError("worker_hr_data.gender 값이 필요합니다.")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"male", "m", "man", "남", "남자", "1"}:
            return 1
        if normalized in {"female", "f", "woman", "여", "여자", "0"}:
            return 0
    return int(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB에서 센서 ID로 연결된 작업자를 찾고 worker_hr_data 기반 회귀모델 예측까지 실행합니다."
    )
    parser.add_argument(
        "sensor_id",
        nargs="?",
        default=DEFAULT_SENSOR_ID,
        help=f"조회할 센서 ID. 기본값: {DEFAULT_SENSOR_ID}",
    )
    args = parser.parse_args()

    try:
        result = run_worker_hr_data_pipeline(args.sensor_id)
    except Exception as e:
        print(f"[test_main] 조회 실패: {type(e).__name__}: {e}")
        return

    if result is None:
        print(f"[test_main] 결과 없음: sensor_id={args.sensor_id}")
        return

    if result["worker_hr_data"] is None:
        print(
            "[test_main] 결과: "
            f"sensor_id={args.sensor_id}, worker_id={result['worker_id']}, "
            "worker_hr_data 없음"
        )
        return

    print(
        "[test_main] 결과: "
        f"sensor_id={args.sensor_id}, worker_id={result['worker_id']}, "
        f"worker_hr_data_id={result['worker_hr_data'].get('worker_hr_data_id')}, "
        f"prediction={result['prediction']}"
    )


if __name__ == "__main__":
    main()
