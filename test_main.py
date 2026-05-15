"""워치 파이프라인 통합 테스트.

각 단계를 독립적인 함수로 분리해 실패 지점을 빠르게 파악할 수 있도록 한다.

실행:
    python test_main.py                  # 기본 sensor_id 사용
    python test_main.py watch-1386       # 특정 sensor_id 지정
"""
import argparse
import json
import os
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt

from ai.rest import (
    DEFAULT_MODEL_PATH,
    BandControlCommandBuilder,
    DatabaseHandlerRestDataRepository,
    EnvironmentSample,
    RestModelEngine,
    RestRuntimeService,
    WatchSample,
    WorkerRawInput,
)
from database.db_handler import DatabaseHandler


DEFAULT_SENSOR_ID = "watch-1386"
DEFAULT_API_BASE_URL = "http://127.0.0.1:8080"

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

# ─── ANSI 색상 (터미널 가독성) ─────────────────────────────────────────────────

_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _pass(label: str, detail: str = "") -> None:
    suffix = f" — {detail}" if detail else ""
    print(f"{_GREEN}{_BOLD}[PASS]{_RESET} {label}{suffix}")


def _fail(label: str, detail: str = "") -> None:
    suffix = f" — {detail}" if detail else ""
    print(f"{_RED}{_BOLD}[FAIL]{_RESET} {label}{suffix}")


def _info(msg: str) -> None:
    print(f"{_YELLOW}[INFO]{_RESET} {msg}")


def _section(title: str) -> None:
    bar = "─" * 60
    print(f"\n{_BOLD}{bar}\n  {title}\n{bar}{_RESET}")


# ─── 환경 / DB 헬퍼 ───────────────────────────────────────────────────────────

def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        _info(f".env 파일 없음: {env_path.resolve()}")
        return
    _info(f".env 로드: {env_path.resolve()}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def build_db_handler() -> DatabaseHandler:
    return DatabaseHandler(
        host=os.getenv("MARIADB_HOST", "127.0.0.1"),
        user=os.getenv("MARIADB_USER", "root"),
        password=os.getenv("MARIADB_PASSWORD", "ekthf123"),
        db_name=os.getenv("MARIADB_DB_NAME", "ON_SAFE"),
        port=int(os.getenv("MARIADB_PORT", "3306")),
    )


# ─── 파이프라인 서브 함수 ─────────────────────────────────────────────────────

def find_worker_id(repository: DatabaseHandlerRestDataRepository, sensor_id: str) -> str | None:
    print(f"  [Pipeline] find_worker_id sensor_id={sensor_id}")
    worker_id = repository.find_worker_id_by_sensor_id(sensor_id)
    print(f"  [Pipeline] find_worker_id result worker_id={worker_id}")
    return worker_id


def fetch_worker_hr_data(db_handler: DatabaseHandler, dept_id: str | int) -> dict[str, Any] | None:
    columns = ", ".join(WORKER_HR_DATA_COLUMNS)
    query = f"SELECT {columns} FROM worker_hr_data WHERE dept_id = %s LIMIT 1"
    print(f"  [Pipeline] fetch_worker_hr_data dept_id={dept_id}")
    with db_handler._get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (dept_id,))
            row = cursor.fetchone()
    result = dict(row) if row else None
    print(f"  [Pipeline] fetch_worker_hr_data result={result}")
    return result


def fetch_environment(repository: DatabaseHandlerRestDataRepository, worker_id: str) -> EnvironmentSample:
    print(f"  [Pipeline] fetch_environment worker_id={worker_id}")
    env = repository.fetch_environment(worker_id)
    print(f"  [Pipeline] fetch_environment result temp_c={env.temp_c} humid={env.humid}")
    return env


def fetch_watch_hr(repository: DatabaseHandlerRestDataRepository, worker_id: str) -> WatchSample:
    print(f"  [Pipeline] fetch_watch_hr worker_id={worker_id}")
    watch = repository.fetch_watch(worker_id)
    print(f"  [Pipeline] fetch_watch_hr result hr={watch.hr}")
    return watch


def build_raw_input(
    *,
    worker_id: str,
    worker_hr_data: dict[str, Any],
    hr: float,
    temp_c: float,
    humid: float,
    work_duration_min: int,
) -> WorkerRawInput:
    print(
        f"  [Pipeline] build_raw_input worker_id={worker_id} "
        f"hr={hr} temp_c={temp_c} humid={humid} work_duration_min={work_duration_min}"
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
    print(f"  [Pipeline] build_raw_input result raw={raw}")
    return raw


def run_rest_model(raw: WorkerRawInput) -> dict[str, Any]:
    print(f"  [Pipeline] run_rest_model model_path={DEFAULT_MODEL_PATH}")
    engine = RestModelEngine(model_path=str(DEFAULT_MODEL_PATH))
    prediction = engine.predict(raw)
    print(f"  [Pipeline] run_rest_model result prediction={prediction}")
    return prediction


def publish_watch_command(command: dict[str, Any]) -> dict[str, Any]:
    topic = command.get("target_topic")
    if not topic:
        raise ValueError("command.target_topic 값이 없어 MQTT topic을 만들 수 없습니다.")

    payload_dict = {k: v for k, v in command.items() if k != "target_topic"}
    payload = json.dumps(payload_dict, ensure_ascii=False)

    broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
    broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
    username = os.getenv("MQTT_USERNAME", "")
    password = os.getenv("MQTT_PASSWORD", "")

    print(
        f"  [Pipeline] publish_watch_command topic={topic} "
        f"host={broker_host}:{broker_port} payload={payload}"
    )
    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password or None)

    try:
        client.connect(broker_host, broker_port, 60)
        client.loop_start()
        info = client.publish(topic, payload)
        info.wait_for_publish(timeout=5)
        rc = getattr(info, "rc", None)
        if rc not in (None, mqtt.MQTT_ERR_SUCCESS):
            raise RuntimeError(f"MQTT publish 실패 rc={rc}")
        result = {"topic": topic, "payload": payload_dict, "rc": rc, "published": True}
        print(f"  [Pipeline] publish_watch_command result={result}")
        return result
    finally:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception as e:
            print(f"  [Pipeline] MQTT 종료 중 오류: {type(e).__name__}: {e}")


def run_temperature_vlm_debug_api(
    *,
    api_base_url: str,
    sensor_id: str,
    camera_sen_id: int | None = None,
    publish: bool = False,
    require_hot: bool = False,
) -> dict[str, Any]:
    import requests

    base_url = api_base_url.rstrip("/")
    url = f"{base_url}/api/temperature-vlm/sensors/{sensor_id}/debug/run-once"
    params: dict[str, Any] = {
        "publish": str(bool(publish)).lower(),
        "require_hot": str(bool(require_hot)).lower(),
    }
    if camera_sen_id is not None:
        params["camera_sen_id"] = int(camera_sen_id)

    print(f"  [Pipeline] call_temperature_vlm_debug_api url={url} params={params}")
    response = requests.post(url, params=params, timeout=180)
    print(f"  [Pipeline] response status_code={response.status_code}")
    try:
        body = response.json()
    except ValueError:
        body = {"raw_text": response.text}

    if response.status_code >= 400:
        raise RuntimeError(f"API 실패 status={response.status_code} body={body}")

    data = body.get("data", body)
    for camera in data.get("cameras", []):
        camera_meta = camera.get("camera", {})
        text = camera.get("text", "")
        print(
            f"[VLM TEXT] camera_sen_id={camera_meta.get('sen_id')} "
            f"space_id={camera_meta.get('space_id')} text={text}"
        )
    return data


def run_temperature_vlm_debug_api_test(
    *,
    api_base_url: str,
    sensor_id: str,
    camera_sen_id: int | None = None,
    publish: bool = False,
    require_hot: bool = False,
) -> None:
    load_dotenv()
    _section("온습도 CCTV autoregressive VLM 실제 API 실행")
    results: dict[str, bool] = {}
    try:
        data = run_temperature_vlm_debug_api(
            api_base_url=api_base_url,
            sensor_id=sensor_id,
            camera_sen_id=camera_sen_id,
            publish=publish,
            require_hot=require_hot,
        )
        camera_count = int(data.get("camera_count") or len(data.get("cameras", [])))
        if data.get("skipped"):
            _fail("온습도 VLM API", f"스킵됨 reason={data.get('reason')}")
            results["온습도 VLM API"] = False
        elif camera_count <= 0:
            _fail("온습도 VLM API", "분석된 CCTV가 없습니다.")
            results["온습도 VLM API"] = False
        else:
            _pass(
                "온습도 VLM API",
                f"sensor_id={sensor_id} space_id={data.get('space_id')} camera_count={camera_count}",
            )
            results["온습도 VLM API"] = True
    except Exception as exc:
        _fail("온습도 VLM API", f"{type(exc).__name__}: {exc}")
        results["온습도 VLM API"] = False
    _print_summary(results)


# ─── 개별 테스트 함수 ─────────────────────────────────────────────────────────

def test_db_connection(db_handler: DatabaseHandler) -> bool:
    """[TEST 1] DB 연결 — 간단한 SELECT 1 로 연결 가능 여부 확인."""
    _section("TEST 1 · DB 연결")
    try:
        with db_handler._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 AS ok")
                row = cursor.fetchone()
        ok = bool(row and row.get("ok") == 1)
        if ok:
            _pass("DB 연결", f"host={db_handler.db_config['host']}:{db_handler.db_config['port']}")
        else:
            _fail("DB 연결", f"SELECT 1 결과 이상: {row}")
        return ok
    except Exception as exc:
        _fail("DB 연결", f"{type(exc).__name__}: {exc}")
        return False


def test_sensor_worker_mapping(
    repository: DatabaseHandlerRestDataRepository,
    sensor_id: str,
) -> tuple[bool, str | None]:
    """[TEST 2] sensor_id → worker_id 매핑 확인."""
    _section(f"TEST 2 · sensor→worker 매핑  sensor_id={sensor_id}")
    try:
        worker_id = find_worker_id(repository, sensor_id)
        if worker_id is not None:
            _pass("sensor→worker 매핑", f"sensor_id={sensor_id} → worker_id={worker_id}")
            return True, worker_id
        else:
            _fail("sensor→worker 매핑", f"sensor_id={sensor_id} 에 연결된 작업자 없음")
            return False, None
    except Exception as exc:
        _fail("sensor→worker 매핑", f"{type(exc).__name__}: {exc}")
        return False, None


def test_worker_hr_data(
    db_handler: DatabaseHandler,
    worker_id: str,
) -> tuple[bool, dict[str, Any] | None]:
    """[TEST 3] worker_hr_data 테이블 조회 — 건강/신체 정보 확인."""
    _section(f"TEST 3 · worker_hr_data 조회  worker_id={worker_id}")
    try:
        data = fetch_worker_hr_data(db_handler, worker_id)
        if data is not None:
            _pass(
                "worker_hr_data 조회",
                f"age={data.get('age')} gender={data.get('gender')} "
                f"height={data.get('height_cm')} weight={data.get('weight_kg')}",
            )
            return True, data
        else:
            _fail("worker_hr_data 조회", f"worker_id={worker_id} 에 해당하는 행 없음")
            return False, None
    except Exception as exc:
        _fail("worker_hr_data 조회", f"{type(exc).__name__}: {exc}")
        return False, None


def test_environment_data(
    repository: DatabaseHandlerRestDataRepository,
    worker_id: str,
) -> tuple[bool, EnvironmentSample | None]:
    """[TEST 4] 온습도(th_trans) 최신 레코드 조회."""
    _section(f"TEST 4 · 온습도 데이터  worker_id={worker_id}")
    try:
        env = fetch_environment(repository, worker_id)
        _pass("온습도 조회", f"temp_c={env.temp_c} humid={env.humid}")
        return True, env
    except Exception as exc:
        _fail("온습도 조회", f"{type(exc).__name__}: {exc}")
        return False, None


def test_watch_hr_data(
    repository: DatabaseHandlerRestDataRepository,
    worker_id: str,
) -> tuple[bool, WatchSample | None]:
    """[TEST 5] 심박(hb_trans) 최신 레코드 조회."""
    _section(f"TEST 5 · 심박 데이터  worker_id={worker_id}")
    try:
        watch = fetch_watch_hr(repository, worker_id)
        _pass("심박 조회", f"hr={watch.hr}")
        return True, watch
    except Exception as exc:
        _fail("심박 조회", f"{type(exc).__name__}: {exc}")
        return False, None


def test_model_prediction(
    worker_id: str,
    worker_hr_data: dict[str, Any],
    env: EnvironmentSample,
    watch: WatchSample,
    work_duration_min: int,
) -> tuple[bool, dict[str, Any] | None]:
    """[TEST 6] 회귀모델 예측 — WorkerRawInput 생성 후 예측."""
    _section("TEST 6 · 회귀모델 예측")
    try:
        raw = build_raw_input(
            worker_id=worker_id,
            worker_hr_data=worker_hr_data,
            hr=watch.hr,
            temp_c=env.temp_c,
            humid=env.humid,
            work_duration_min=work_duration_min,
        )
        prediction = run_rest_model(raw)
        _pass(
            "회귀모델 예측",
            f"result={prediction.get('result')} score={prediction.get('score')}",
        )
        return True, prediction
    except Exception as exc:
        _fail("회귀모델 예측", f"{type(exc).__name__}: {exc}")
        return False, None


def test_mqtt_publish(
    prediction: dict[str, Any],
    target_topic: str,
) -> tuple[bool, dict[str, Any] | None]:
    """[TEST 7] MQTT 발행 — 휴식 권고 필요 시 워치에 명령 전송."""
    _section("TEST 7 · MQTT 발행")
    try:
        if not RestRuntimeService.should_send_rest_command(prediction):
            _info(
                f"MQTT 발행 스킵 — 휴식 불필요 result={prediction.get('result')}"
            )
            return True, None

        command = BandControlCommandBuilder().build_for_prediction(target_topic, prediction).to_dict()
        result = publish_watch_command(command)
        _pass("MQTT 발행", f"topic={result['topic']} rc={result['rc']}")
        return True, result
    except Exception as exc:
        _fail("MQTT 발행", f"{type(exc).__name__}: {exc}")
        return False, None


# ─── 전체 테스트 실행기 ───────────────────────────────────────────────────────

def run_all_tests(sensor_id: str) -> None:
    """모든 테스트를 순서대로 실행하고 PASS/FAIL 요약을 출력한다."""
    load_dotenv()
    db_handler = build_db_handler()
    repository = DatabaseHandlerRestDataRepository(db_handler)

    results: dict[str, bool] = {}

    # TEST 1
    results["DB 연결"] = test_db_connection(db_handler)
    if not results["DB 연결"]:
        _section("중단: DB 연결 실패 — 이후 테스트 불가")
        _print_summary(results)
        return

    # TEST 2
    ok2, worker_id = test_sensor_worker_mapping(repository, sensor_id)
    results["sensor→worker 매핑"] = ok2
    if not ok2:
        _print_summary(results)
        return

    # TEST 3
    ok3, worker_hr_data = test_worker_hr_data(db_handler, worker_id)
    results["worker_hr_data 조회"] = ok3
    if not ok3:
        # 이후 모델 예측 불가이지만 온습도/심박은 계속 테스트
        worker_hr_data = None

    # TEST 4
    ok4, env = test_environment_data(repository, worker_id)
    results["온습도 조회"] = ok4

    # TEST 5
    ok5, watch = test_watch_hr_data(repository, worker_id)
    results["심박 조회"] = ok5

    # TEST 6 — worker_hr_data / env / watch 모두 있어야 가능
    if worker_hr_data and env and watch:
        profile = repository.fetch_worker_profile(worker_id)
        ok6, prediction = test_model_prediction(
            worker_id=worker_id,
            worker_hr_data=worker_hr_data,
            env=env,
            watch=watch,
            work_duration_min=profile.work_duration_min,
        )
        results["회귀모델 예측"] = ok6

        # TEST 7
        if ok6 and prediction:
            ok7, _ = test_mqtt_publish(prediction, profile.target_topic)
            results["MQTT 발행"] = ok7
        else:
            results["MQTT 발행"] = False
    else:
        _info("TEST 6/7 스킵 — 이전 단계 데이터 부족")
        results["회귀모델 예측"] = False
        results["MQTT 발행"] = False

    _print_summary(results)


def _print_summary(results: dict[str, bool]) -> None:
    _section("테스트 결과 요약")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    for name, ok in results.items():
        mark = f"{_GREEN}PASS{_RESET}" if ok else f"{_RED}FAIL{_RESET}"
        print(f"  [{mark}] {name}")
    print()
    color = _GREEN if passed == total else _RED
    print(f"  {color}{_BOLD}{passed}/{total} 통과{_RESET}")


# ─── 타입 변환 헬퍼 ───────────────────────────────────────────────────────────

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


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="워치 파이프라인 통합 테스트 (온습도·작업자정보·심박 → 모델 예측 → MQTT)"
    )
    parser.add_argument(
        "sensor_id",
        nargs="?",
        default=DEFAULT_SENSOR_ID,
        help=f"테스트할 센서 ID. 기본값: {DEFAULT_SENSOR_ID}",
    )
    parser.add_argument(
        "--temperature-vlm-api",
        action="store_true",
        help="서버의 실제 온습도 CCTV autoregressive VLM 1회 실행 API를 호출",
    )
    parser.add_argument(
        "--temperature-sensor-id",
        default="",
        help="온습도 VLM API에 사용할 sensor.sensor_id. 비우면 positional sensor_id를 사용",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("API_BASE_URL", DEFAULT_API_BASE_URL),
        help=f"API 서버 주소. 기본값: {DEFAULT_API_BASE_URL}",
    )
    parser.add_argument(
        "--camera-sen-id",
        type=int,
        default=None,
        help="특정 CCTV sen_id만 VLM 1회 실행",
    )
    parser.add_argument(
        "--publish-vlm-result",
        action="store_true",
        help="VLM 결과를 WebSocket 앱 알림으로도 발행",
    )
    parser.add_argument(
        "--require-hot",
        action="store_true",
        help="현재 온도가 임계치 이상일 때만 VLM 실행",
    )
    args = parser.parse_args()
    if args.temperature_vlm_api:
        temperature_sensor_id = args.temperature_sensor_id or args.sensor_id
        run_temperature_vlm_debug_api_test(
            api_base_url=args.api_base_url,
            sensor_id=temperature_sensor_id,
            camera_sen_id=args.camera_sen_id,
            publish=args.publish_vlm_result,
            require_hot=args.require_hot,
        )
        return

    run_all_tests(args.sensor_id)


if __name__ == "__main__":
    main()
