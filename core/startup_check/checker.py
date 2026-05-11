"""서버 기동 후 필수 서비스 및 DB 상태를 자동으로 점검한다.

점검 항목:
  1. DB 연결
  2. DB CRUD (ds_space 임시 레코드 insert→select→delete)
  3. 등록 센서 현황 (heart_band / temp_humidity 수)
  4. MQTT 브로커 연결
  5. mDNS 서비스 실행 중
  6. vLLM 서버 응답
  7. YOLO 모델 파일 존재
  8. 워치 파이프라인 스레드 현황
  9. 핵심 API 응답 (/health)
"""
import logging
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_G = "\033[32m"
_R = "\033[31m"
_Y = "\033[33m"
_B = "\033[1m"
_E = "\033[0m"

_TEST_SPACE_NAME = "__startup_check_tmp__"


def _log(ok: bool, label: str, detail: str = "") -> None:
    mark = f"{_G}[PASS]{_E}" if ok else f"{_R}[FAIL]{_E}"
    suffix = f" — {detail}" if detail else ""
    fn = logger.info if ok else logger.warning
    fn("[StartupCheck] %s %s%s", mark, label, suffix)


class StartupChecker:
    def __init__(self, app):
        self.app = app
        self.results: dict[str, bool] = {}

    # ── 공개 진입점 ───────────────────────────────────────────────────────────

    async def run_all(self) -> dict[str, bool]:
        logger.info(
            "\n%s%s[StartupCheck] ═══ 서버 기동 점검 시작 ═══%s",
            _B, _Y, _E,
        )
        t0 = time.perf_counter()

        checks = [
            ("DB 연결",            self._check_db_connection),
            ("DB CRUD",            self._check_db_crud),
            ("등록 센서 현황",     self._check_registered_sensors),
            ("MQTT 브로커",        self._check_mqtt),
            ("mDNS 서비스",        self._check_mdns),
            ("vLLM 서버",          self._check_vllm),
            ("YOLO 모델 파일",     self._check_yolo_model),
            ("워치 파이프라인",    self._check_watch_pipeline),
            ("API /health 응답",   self._check_health_api),
        ]

        for label, fn in checks:
            try:
                ok, detail = await fn()
            except Exception as exc:
                ok, detail = False, f"{type(exc).__name__}: {exc}"
            self.results[label] = ok
            _log(ok, label, detail)

        elapsed = time.perf_counter() - t0
        passed = sum(1 for v in self.results.values() if v)
        total = len(self.results)
        color = _G if passed == total else (_Y if passed >= total // 2 else _R)
        logger.info(
            "%s%s[StartupCheck] ═══ 점검 완료: %d/%d 통과 (%.2fs) ═══%s",
            _B, color, passed, total, elapsed, _E,
        )
        return self.results

    # ── 개별 점검 ─────────────────────────────────────────────────────────────

    async def _check_db_connection(self) -> tuple[bool, str]:
        db = self.app.state.db
        with db._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
        ok = bool(row and row.get("ok") == 1)
        return ok, f"host={db.db_config['host']}:{db.db_config['port']}"

    async def _check_db_crud(self) -> tuple[bool, str]:
        db = self.app.state.db
        row = db.create_space(_TEST_SPACE_NAME)
        if not row:
            return False, "INSERT 실패"
        space_id = row["space_id"]
        read = db.get_space_by_id(space_id)
        if not read:
            db.delete_space(space_id)
            return False, "SELECT 실패"
        db.delete_space(space_id)
        verify = db.get_space_by_id(space_id)
        if verify is not None:
            return False, "DELETE 실패"
        return True, "INSERT→SELECT→DELETE 정상"

    async def _check_registered_sensors(self) -> tuple[bool, str]:
        db = self.app.state.db
        rows = db.get_registered_sensor_rows()
        hb = sum(1 for r in rows if r.get("sensor_type") == "heart_band")
        th = sum(1 for r in rows if r.get("sensor_type") == "temp_humidity")
        cam = sum(1 for r in rows if r.get("sensor_type") == "camera")
        detail = f"heart_band={hb} temp_humidity={th} camera={cam} total={len(rows)}"
        return True, detail

    async def _check_mqtt(self) -> tuple[bool, str]:
        svc = getattr(self.app.state, "mqtt_sensor_service", None)
        if svc is None:
            return False, "mqtt_sensor_service 없음"
        ok = svc._running
        return ok, "브로커 연결 완료" if ok else "연결 실패 또는 미시작"

    async def _check_mdns(self) -> tuple[bool, str]:
        svc = getattr(self.app.state, "mdns_service", None)
        if svc is None:
            return False, "mdns_service 없음"
        running = getattr(svc, "_running", None) or getattr(svc, "is_running", None)
        if callable(running):
            running = running()
        return bool(running), "탐색 서비스 실행 중" if running else "미실행"

    async def _check_vllm(self) -> tuple[bool, str]:
        from config import settings
        base_url = settings.vllm_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{base_url}/models")
            ok = resp.status_code == 200
            return ok, f"status={resp.status_code} url={base_url}"
        except Exception as exc:
            return False, f"{type(exc).__name__} — {base_url}"

    async def _check_yolo_model(self) -> tuple[bool, str]:
        from config import settings
        path = settings.yolo_model_path
        exists = os.path.isfile(path)
        return exists, f"{'존재' if exists else '없음'}: {path}"

    async def _check_watch_pipeline(self) -> tuple[bool, str]:
        scheduler = getattr(self.app.state, "watch_scheduler", None)
        if scheduler is None:
            return False, "watch_scheduler 없음"
        count = len(getattr(scheduler, "_runners", {}))
        return True, f"활성 스레드 수={count}"

    async def _check_health_api(self) -> tuple[bool, str]:
        from config import settings
        url = f"http://127.0.0.1:{settings.api_port}/health"
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(url)
            ok = resp.status_code == 200
            return ok, f"status={resp.status_code}"
        except Exception as exc:
            return False, str(exc)

    # ── 결과 요약 반환 (API용) ────────────────────────────────────────────────

    def summary(self) -> dict[str, Any]:
        passed = sum(1 for v in self.results.values() if v)
        return {
            "passed": passed,
            "total": len(self.results),
            "all_ok": passed == len(self.results),
            "checks": {k: ("pass" if v else "fail") for k, v in self.results.items()},
        }
