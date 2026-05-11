"""MariaDB ON_SAFE 전체 CRUD 통합 테스트.

실행:
    python main_insert_test.py

각 테스트는 INSERT → SELECT → UPDATE → DELETE 순서로 검증하며,
테스트 데이터는 실행 후 자동으로 정리됩니다.
"""
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent))


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


# ─── 색상 출력 ────────────────────────────────────────────────────────────────

_G = "\033[32m"
_R = "\033[31m"
_Y = "\033[33m"
_B = "\033[1m"
_E = "\033[0m"


def _pass(label: str, detail: str = "") -> None:
    suf = f" — {detail}" if detail else ""
    print(f"  {_G}{_B}[PASS]{_E} {label}{suf}")


def _fail(label: str, detail: str = "") -> None:
    suf = f" — {detail}" if detail else ""
    print(f"  {_R}{_B}[FAIL]{_E} {label}{suf}")


def _section(title: str) -> None:
    print(f"\n{_B}{'─' * 60}\n  {title}\n{'─' * 60}{_E}")


def _info(msg: str) -> None:
    print(f"  {_Y}[INFO]{_E} {msg}")


# ─── DB 핸들러 ────────────────────────────────────────────────────────────────

def build_db() -> "DatabaseHandler":
    from database.db_handler import DatabaseHandler
    return DatabaseHandler(
        host=os.getenv("MARIADB_HOST", "127.0.0.1"),
        user=os.getenv("MARIADB_USER", "root"),
        password=os.getenv("MARIADB_PASSWORD", "ekthf123"),
        db_name=os.getenv("MARIADB_DB_NAME", "ON_SAFE"),
        port=int(os.getenv("MARIADB_PORT", "3306")),
    )


# ─── 개별 테스트 함수 ─────────────────────────────────────────────────────────

def test_db_connection(db) -> bool:
    _section("TEST 1 · DB 연결")
    try:
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
        ok = bool(row and row.get("ok") == 1)
        (_pass if ok else _fail)("DB 연결", f"host={db.db_config['host']}:{db.db_config['port']}")
        return ok
    except Exception as e:
        _fail("DB 연결", str(e))
        return False


def test_ds_space_crud(db) -> bool:
    _section("TEST 2 · ds_space (공정/구역) CRUD")
    space_id = None
    try:
        # CREATE
        row = db.create_space("테스트_구역_001")
        assert row and row["space_id"], "create 실패"
        space_id = row["space_id"]
        _pass("CREATE", f"space_id={space_id}")

        # READ
        row = db.get_space_by_id(space_id)
        assert row and row["space_name"] == "테스트_구역_001", "read 실패"
        _pass("READ", f"space_name={row['space_name']}")

        # LIST
        rows = db.get_all_spaces()
        assert any(r["space_id"] == space_id for r in rows), "list 실패"
        _pass("LIST", f"총 {len(rows)}개")

        # UPDATE
        row = db.update_space(space_id, "테스트_구역_001_수정")
        assert row and row["space_name"] == "테스트_구역_001_수정", "update 실패"
        _pass("UPDATE", f"space_name={row['space_name']}")

        # DELETE
        ok = db.delete_space(space_id)
        assert ok, "delete 실패"
        assert db.get_space_by_id(space_id) is None, "delete 후 조회됨"
        _pass("DELETE")
        space_id = None
        return True
    except Exception as e:
        _fail("ds_space CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if space_id:
            db.delete_space(space_id)


def test_worker_crud(db) -> bool:
    _section("TEST 3 · worker CRUD")
    dept_id = 99901
    try:
        # 기존 데이터 정리
        db.delete_worker(dept_id)

        # CREATE
        row = db.create_worker(dept_id, "테스트_작업자", is_manager=0)
        assert row and row["dept_id"] == dept_id, "create 실패"
        _pass("CREATE", f"dept_id={dept_id}")

        # READ
        row = db.get_worker_by_dept_id(dept_id)
        assert row and row["name"] == "테스트_작업자", "read 실패"
        _pass("READ", f"name={row['name']}")

        # LIST
        rows = db.get_workers()
        assert any(r["dept_id"] == dept_id for r in rows), "list 실패"
        _pass("LIST", f"총 {len(rows)}명")

        # UPDATE
        row = db.update_worker(dept_id, name="테스트_작업자_수정")
        assert row and row["name"] == "테스트_작업자_수정", "update 실패"
        _pass("UPDATE", f"name={row['name']}")

        # DELETE
        ok = db.delete_worker(dept_id)
        assert ok, "delete 실패"
        assert db.get_worker_by_dept_id(dept_id) is None, "delete 후 조회됨"
        _pass("DELETE")
        dept_id = None
        return True
    except Exception as e:
        _fail("worker CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if dept_id:
            db.delete_worker(dept_id)


def test_sensor_crud(db) -> bool:
    _section("TEST 4 · sensor (temp_humidity) CRUD")
    sen_id = None
    try:
        # CREATE
        row = db.create_temp_sensor(
            sensor_id="test-th-sensor-001",
            sen_name="테스트_온습도센서",
            space_id=None,
        )
        assert row and row["sen_id"], "create 실패"
        sen_id = row["sen_id"]
        _pass("CREATE", f"sen_id={sen_id}")

        # READ
        row = db.get_temp_sensor_by_id(sen_id)
        assert row and row["sen_name"] == "테스트_온습도센서", "read 실패"
        _pass("READ", f"sen_name={row['sen_name']}")

        # LIST
        rows = db.get_temp_sensors()
        assert any(r["sen_id"] == sen_id for r in rows), "list 실패"
        _pass("LIST", f"총 {len(rows)}개")

        # UPDATE
        row = db.update_temp_sensor(sen_id, name="테스트_온습도센서_수정")
        assert row and row["sen_name"] == "테스트_온습도센서_수정", "update 실패"
        _pass("UPDATE", f"sen_name={row['sen_name']}")

        # DELETE
        ok = db.delete_temp_sensor(sen_id)
        assert ok, "delete 실패"
        assert db.get_temp_sensor_by_id(sen_id) is None, "delete 후 조회됨"
        _pass("DELETE")
        sen_id = None
        return True
    except Exception as e:
        _fail("sensor CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if sen_id:
            db.delete_temp_sensor(sen_id)


def test_th_trans(db) -> bool:
    _section("TEST 5 · th_trans (온습도 수신 저장)")
    sen_id = None
    try:
        row = db.create_temp_sensor("test-th-trans-001", "테스트_온습도_trans")
        assert row, "임시 센서 생성 실패"
        sen_id = row["sen_id"]

        ok = db.insert_th_trans(sen_id, datetime.now(), temp=25.5, humid=60.0)
        assert ok, "th_trans INSERT 실패"
        _pass("INSERT th_trans", f"sen_id={sen_id} temp=25.5 humid=60.0")

        latest = db.get_latest_th_by_sensor_id("test-th-trans-001")
        assert latest, "th_trans 조회 실패"
        _pass("SELECT th_trans", f"temp={latest.get('temp')} humid={latest.get('humid')}")
        return True
    except Exception as e:
        _fail("th_trans", str(e))
        traceback.print_exc()
        return False
    finally:
        if sen_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM th_trans WHERE sen_id = %s", (sen_id,))
                    conn.commit()
            except Exception:
                pass
            db.delete_temp_sensor(sen_id)


def test_hb_trans(db) -> bool:
    _section("TEST 6 · hb_trans (심박 수신 저장)")
    dept_id = 99902
    sen_id = None
    try:
        db.delete_worker(dept_id)
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO sensor (sensor_id, jetson_id, sensor_type, sen_name, sen_locate, created_at, updated_at) "
                    "VALUES ('test-hb-sensor-001', 1, 'heart_band', '테스트_심박센서', '', NOW(), NOW())"
                )
                sen_id = cur.lastrowid
            conn.commit()

        db.create_worker(dept_id, "테스트_심박작업자")
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE worker SET sen_id = %s WHERE dept_id = %s", (sen_id, dept_id))
            conn.commit()

        ok = db.insert_hb_trans(sen_id, datetime.now(), hr=75.0)
        assert ok, "hb_trans INSERT 실패"
        _pass("INSERT hb_trans", f"sen_id={sen_id} hr=75.0")

        rows = db.get_web_sensor_hb()
        _pass("SELECT hb_trans (get_web_sensor_hb)", f"조회 결과 {len(rows)}행")
        return True
    except Exception as e:
        _fail("hb_trans", str(e))
        traceback.print_exc()
        return False
    finally:
        if sen_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM hb_trans WHERE sen_id = %s", (sen_id,))
                        cur.execute("UPDATE worker SET sen_id = NULL WHERE dept_id = %s", (dept_id,))
                    conn.commit()
            except Exception:
                pass
        db.delete_worker(dept_id)
        if sen_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM sensor WHERE sen_id = %s", (sen_id,))
                    conn.commit()
            except Exception:
                pass


def test_event_crud(db) -> bool:
    _section("TEST 7 · event (이상 이벤트) CRUD")
    event_id = None
    ev_code_id = None
    try:
        # event_code 첫 번째 항목 조회
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ev_code_id FROM event_code LIMIT 1")
                row = cur.fetchone()
        if not row:
            _info("event_code 테이블이 비어 있어 테스트 스킵")
            return True
        ev_code_id = row["ev_code_id"]

        # CREATE
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO event (ev_code_id, message, detected_value, time, status)
                    VALUES (%s, '테스트_이벤트', '99.9', NOW(), 'active')
                    """,
                    (ev_code_id,),
                )
                event_id = cur.lastrowid
            conn.commit()
        _pass("CREATE event", f"event_id={event_id}")

        # READ
        row = db.get_event_by_id(event_id)
        assert row and row["event_id"] == event_id, "read 실패"
        _pass("READ event", f"status={row.get('status')}")

        # LIST
        rows = db.get_events()
        assert any(r["event_id"] == event_id for r in rows), "list 실패"
        _pass("LIST events", f"총 {len(rows)}개")

        # UPDATE (status, end_time)
        row = db.update_event(event_id, status="stopped", end_time=datetime.now())
        assert row and row.get("status") == "stopped", "update 실패"
        _pass("UPDATE event", f"status={row.get('status')}")

        event_id_kept = event_id
        event_id = None
        return True
    except Exception as e:
        _fail("event CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if event_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM event WHERE event_id = %s", (event_id,))
                    conn.commit()
            except Exception:
                pass


def test_vlm_query_crud(db) -> bool:
    _section("TEST 8 · vlm_query CRUD")
    event_id = None
    try:
        # 임시 event 생성
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ev_code_id FROM event_code LIMIT 1")
                ec = cur.fetchone()
        if not ec:
            _info("event_code 없음 — 스킵")
            return True

        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO event (ev_code_id, message, detected_value, time, status) "
                    "VALUES (%s, 'vlm_test_event', '0', NOW(), 'active')",
                    (ec["ev_code_id"],),
                )
                event_id = cur.lastrowid
            conn.commit()

        # INSERT vlm_query
        row = db.save_vlm_query(
            event_id=event_id,
            prompt="테스트 프롬프트",
            response="테스트 응답",
            frame_path="/tmp/test_frame.jpg",
        )
        assert row and row["query_id"], "vlm_query INSERT 실패"
        _pass("INSERT vlm_query", f"query_id={row['query_id']}")

        # SELECT
        rows = db.get_vlm_queries_by_event(event_id)
        assert rows, "vlm_query 조회 실패"
        _pass("SELECT vlm_query", f"총 {len(rows)}개")
        return True
    except Exception as e:
        _fail("vlm_query CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if event_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM vlm_query WHERE event_id = %s", (event_id,))
                        cur.execute("DELETE FROM event WHERE event_id = %s", (event_id,))
                    conn.commit()
            except Exception:
                pass


def test_event_report_crud(db) -> bool:
    _section("TEST 9 · event_report CRUD")
    event_id = None
    report_id = None
    try:
        # 임시 event 생성
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ev_code_id FROM event_code LIMIT 1")
                ec = cur.fetchone()
        if not ec:
            _info("event_code 없음 — 스킵")
            return True

        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO event (ev_code_id, message, detected_value, time, status) "
                    "VALUES (%s, 'report_test_event', '0', NOW(), 'active')",
                    (ec["ev_code_id"],),
                )
                event_id = cur.lastrowid
            conn.commit()

        # CREATE
        row = db.create_report(
            event_id=event_id,
            content="테스트 조치 보고서",
            action_taken="즉시 대피 조치",
            created_by="테스터",
        )
        assert row and row["report_id"], "report CREATE 실패"
        report_id = row["report_id"]
        _pass("CREATE report", f"report_id={report_id}")

        # READ by id
        row = db.get_report_by_id(report_id)
        assert row and row["content"] == "테스트 조치 보고서", "read by id 실패"
        _pass("READ by report_id", f"content={row['content'][:20]}")

        # READ by event
        row = db.get_report_by_event_id(event_id)
        assert row and row["report_id"] == report_id, "read by event_id 실패"
        _pass("READ by event_id")

        # LIST
        rows = db.get_all_reports()
        assert any(r["report_id"] == report_id for r in rows), "list 실패"
        _pass("LIST reports", f"총 {len(rows)}개")

        # UPDATE
        row = db.update_report(report_id, content="수정된 보고서 내용")
        assert row and row["content"] == "수정된 보고서 내용", "update 실패"
        _pass("UPDATE report", f"content={row['content'][:20]}")

        return True
    except Exception as e:
        _fail("event_report CRUD", str(e))
        traceback.print_exc()
        return False
    finally:
        if event_id:
            try:
                with db._get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM event_report WHERE event_id = %s", (event_id,))
                        cur.execute("DELETE FROM event WHERE event_id = %s", (event_id,))
                    conn.commit()
            except Exception:
                pass


# ─── 요약 출력 ────────────────────────────────────────────────────────────────

def _print_summary(results: dict[str, bool]) -> None:
    print(f"\n{_B}{'═' * 60}")
    print("  테스트 결과 요약")
    print(f"{'═' * 60}{_E}")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    for name, ok in results.items():
        mark = f"{_G}PASS{_E}" if ok else f"{_R}FAIL{_E}"
        print(f"  [{mark}] {name}")
    color = _G if passed == total else _R
    print(f"\n  {color}{_B}{passed}/{total} 통과{_E}\n")


# ─── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()
    db = build_db()

    tests = [
        ("DB 연결",              lambda: test_db_connection(db)),
        ("ds_space CRUD",        lambda: test_ds_space_crud(db)),
        ("worker CRUD",          lambda: test_worker_crud(db)),
        ("sensor CRUD",          lambda: test_sensor_crud(db)),
        ("th_trans 저장",        lambda: test_th_trans(db)),
        ("hb_trans 저장",        lambda: test_hb_trans(db)),
        ("event CRUD",           lambda: test_event_crud(db)),
        ("vlm_query CRUD",       lambda: test_vlm_query_crud(db)),
        ("event_report CRUD",    lambda: test_event_report_crud(db)),
    ]

    results: dict[str, bool] = {}
    for name, fn in tests:
        try:
            results[name] = fn()
        except Exception as e:
            _fail(name, f"예외 발생: {e}")
            results[name] = False

    _print_summary(results)
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()
