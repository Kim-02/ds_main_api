import argparse
import os
from pathlib import Path

from ai.rest import DatabaseHandlerRestDataRepository
from database.db_handler import DatabaseHandler


DEFAULT_SENSOR_ID = "watch-1386"


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


def main() -> None:
    parser = argparse.ArgumentParser(description="DB에서 센서 ID로 연결된 작업자 ID를 조회합니다.")
    parser.add_argument(
        "sensor_id",
        nargs="?",
        default=DEFAULT_SENSOR_ID,
        help=f"조회할 센서 ID. 기본값: {DEFAULT_SENSOR_ID}",
    )
    args = parser.parse_args()

    try:
        worker_id = find_worker_by_sensor_id(args.sensor_id)
    except Exception as e:
        print(f"[test_main] 조회 실패: {type(e).__name__}: {e}")
        return

    if worker_id is None:
        print(f"[test_main] 결과 없음: sensor_id={args.sensor_id}에 연결된 작업자가 없습니다.")
        return

    print(f"[test_main] 결과: sensor_id={args.sensor_id}, worker_id={worker_id}")


if __name__ == "__main__":
    main()
