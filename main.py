import logging
import socket
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect

from config import settings


class RuntimeLogFilter(logging.Filter):
    """Jetson 현장 테스트용 로그 필터.

    LOG_PROFILE=vlm_focus 일 때는 VLM 흐름, MQTT 센서 수신값, 전체 ERROR만 콘솔에 남긴다.
    """

    SENSOR_DATA_MARKERS = (
        "온습도 업데이트",
        "심박 업데이트",
    )
    VLM_LOGGER_PREFIXES = (
        "ai.vlm",
        "core.cctv_vlm_context",
        "core.watch_pipeline.camera_vlm",
    )
    TEST_LOGGER_PREFIXES = (
        "ai.rest",
        "core.watch_pipeline.runner",
        "core.sensor_registry",
        "worker",
    )
    VLM_MESSAGE_MARKERS = (
        "VLM",
        "vLLM",
        "VLLM",
        "[VLM TEXT]",
        "TemperatureVLM",
        "TemperatureVLMDebug",
        "WatchCameraVLM",
        "autoregressive VLM",
    )
    TEST_MESSAGE_MARKERS = (
        "WatchPipeline",
        "WatchScheduler",
        "RestRepository",
        "RestModelEngine",
        "WorkerWatchRegister",
        "sensor→worker",
        "worker_hr_data",
        "TEST",
        "test_main",
        "LOGGING",
    )

    def __init__(self, profile: str):
        super().__init__()
        self.profile = (profile or "default").strip().lower()

    def filter(self, record: logging.LogRecord) -> bool:
        if self.profile not in {"vlm_focus", "vlm-only", "vlm_only"}:
            return True

        if record.levelno >= logging.ERROR:
            return True

        message = record.getMessage()

        if record.name == "core.mqtt.sensor_service":
            return any(marker in message for marker in self.SENSOR_DATA_MARKERS)

        if record.name.startswith(self.VLM_LOGGER_PREFIXES):
            return True

        if record.name.startswith(self.TEST_LOGGER_PREFIXES):
            return True

        return any(marker in message for marker in self.VLM_MESSAGE_MARKERS + self.TEST_MESSAGE_MARKERS)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.DEBUG if settings.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )

    runtime_filter = RuntimeLogFilter(settings.log_profile)
    for handler in logging.getLogger().handlers:
        handler.addFilter(runtime_filter)

    if runtime_filter.profile in {"vlm_focus", "vlm-only", "vlm_only"}:
        for logger_name in ("uvicorn.access", "uvicorn.error", "httpx"):
            logging.getLogger(logger_name).setLevel(logging.ERROR)
    logging.getLogger(__name__).info("[LOGGING] configured profile=%s debug=%s", runtime_filter.profile, settings.debug)


configure_logging()
logger = logging.getLogger(__name__)


# ── WebSocket 연결 관리 ──────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)
        logger.info("[WS] client connected id=%s total_clients=%d", id(ws), len(self.active_connections))

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)
        logger.info("[WS] client disconnected id=%s total_clients=%d", id(ws), len(self.active_connections))

    async def broadcast(self, message: dict):
        event_id = message.get("event_id", "-")
        msg_type = message.get("type", "-")
        n = len(self.active_connections)
        logger.info("[WS] broadcasting type=%s event_id=%s clients=%d", msg_type, event_id, n)
        dead = []
        sent = 0
        for ws in list(self.active_connections):
            try:
                await ws.send_json(message)
                sent += 1
                logger.info("[WS] send ok client=%s event_id=%s", id(ws), event_id)
            except Exception as exc:
                logger.warning("[WS] send failed client=%s event_id=%s error=%s", id(ws), event_id, exc)
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
        return sent


ws_manager = ConnectionManager()
active_vital_ws: List[WebSocket] = []
active_th_ws: List[WebSocket] = []


# ── Transmission 어댑터 ──────────────────────────────────────────────────────

class RealTransmission:
    def __init__(self, loop):
        self.loop = loop

    def send_push_notification(self, payload: dict):
        import asyncio
        asyncio.run_coroutine_threadsafe(ws_manager.broadcast(payload), self.loop)

    def send_vital_data(self, payload: dict):
        import asyncio

        async def _broadcast():
            for ws in list(active_vital_ws):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

        asyncio.run_coroutine_threadsafe(_broadcast(), self.loop)

    def send_th_data(self, payload: dict):
        import asyncio

        async def _broadcast():
            for ws in list(active_th_ws):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

        asyncio.run_coroutine_threadsafe(_broadcast(), self.loop)


# ── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio

    # 0. VLM 서버 + YOLO 엔진 준비 확인
    from ai.vlm.server_runtime import VllmServerManager, preload_yolo_engine
    from ai.vlm.yolo_runtime import ensure_tensorrt_available

    app.state.vllm_server_manager = None
    app.state.yolo_engine_ready = False

    vllm_server_manager = VllmServerManager(settings)

    try:
        if settings.yolo_preload_on_startup:
            logger.info("Startup step 0-0: YOLO 런타임 사전 확인 시작")
            await asyncio.to_thread(ensure_tensorrt_available, settings.fire_pipeline_yolo_model_path)
            logger.info("Startup step 0-0: YOLO 런타임 사전 확인 완료")

        logger.info("Startup step 0-1: VLM 서버 준비 확인 시작")
        await vllm_server_manager.ensure_ready()
        app.state.vllm_server_manager = vllm_server_manager
        logger.info("Startup step 0-1: VLM 서버 준비 확인 완료")

        logger.info("Startup step 0-2: YOLO 엔진 preload 시작")
        await preload_yolo_engine(settings)
        app.state.yolo_engine_ready = True
        logger.info("Startup step 0-2: YOLO 엔진 preload 완료")
    except Exception:
        logger.exception("VLM/YOLO startup preflight 실패")
        vllm_server_manager.stop()
        raise

    # 1. MariaDB 핸들러
    from database.db_handler import DatabaseHandler

    db = DatabaseHandler(
        host=settings.mariadb_host,
        user=settings.mariadb_user,
        password=settings.mariadb_password,
        db_name=settings.mariadb_db_name,
        port=settings.mariadb_port,
    )

    app.state.db = db
    db.ensure_alert_event_table()

    # 2. IP 감지 + Jetson DB 업데이트
    from core.jetson.service import get_real_ip, startup_db_init

    current_ip = get_real_ip()
    startup_db_init(db, current_ip, settings.api_port)

    # 3. Transmission + SafetyDetectionModule
    main_loop = asyncio.get_running_loop()
    app.state.transmission = RealTransmission(main_loop)
    app.state.safety_core = None

    try:
        import sys

        sys.path.insert(0, "/home/vic06/Desktop/ds_api/jetson_api")

        from app.core_engine import SafetyDetectionModule
        from app.sensor_listener import SensorDataCollector

        app.state.safety_core = SafetyDetectionModule(db, app.state.transmission)
        app.state.safety_core.update_and_get_subscriptions()

        SensorDataCollector(app.state.safety_core).start()

        logger.info("SafetyDetectionModule 로드 완료")

    except Exception as e:
        logger.warning("SafetyDetectionModule 없음 (스킵): %s", e)

    # 4. mDNS 센서 탐색 시작
    from core.mdns.service import MdnsSensorService

    try:
        mdns_service = MdnsSensorService(db_handler=db)
    except TypeError:
        mdns_service = MdnsSensorService()

    await mdns_service.start()

    app.state.mdns_service = mdns_service
    app.state.mdns_sensor_service = mdns_service

    logger.info("mDNS 센서 탐색 서비스 시작 완료")

    # 5. temperature MQTT 핸들러에 db 주입
    from temperature.mqtt.handler import set_db as set_temp_db

    set_temp_db(db)

    # 6. paho MQTT 센서 서비스 시작
    from core.mqtt.sensor_service import MqttSensorService

    mqtt_sensor_svc = MqttSensorService(
        db_handler=db,
        broker_host=settings.mqtt_broker_host,
        broker_port=settings.mqtt_broker_port,
    )

    mqtt_sensor_svc.start()
    app.state.mqtt_sensor_service = mqtt_sensor_svc

    logger.info("MQTT 센서 서비스 시작 완료")

    # 7. 워치 파이프라인 스케줄러 — 등록된 heart_band 센서마다 스레드 1개씩 시작
    from core.watch_pipeline.camera_vlm import WatchCameraVlmManager
    from core.watch_pipeline.runner import WatchPipelineScheduler

    watch_camera_vlm_manager = WatchCameraVlmManager(
        db_handler=db,
        loop=main_loop,
        broadcast_fn=ws_manager.broadcast,
        session_seconds=settings.watch_camera_vlm_session_seconds,
    )
    app.state.watch_camera_vlm_manager = watch_camera_vlm_manager

    watch_scheduler = WatchPipelineScheduler(
        db_handler=db,
        mqtt_publish_fn=mqtt_sensor_svc.publish,
        camera_vlm_manager=watch_camera_vlm_manager,
        interval_sec=settings.watch_pipeline_interval_seconds,
        stale_threshold_sec=settings.watch_hr_stale_seconds,
        alert_duration_ms=settings.watch_alert_duration_ms,
        alert_reset_after_ms=settings.watch_alert_reset_after_ms,
        forced_rest_work_min=settings.rest_forced_rest_work_minutes,
        rest_repository_defaults={
            "default_age": settings.rest_default_age,
            "default_gender": settings.rest_default_gender,
            "default_height_cm": settings.rest_default_height_cm,
            "default_weight_kg": settings.rest_default_weight_kg,
            "default_work_duration_min": settings.rest_default_work_duration_min,
        },
    )
    watch_scheduler.start()
    app.state.watch_scheduler = watch_scheduler

    logger.info("워치 파이프라인 스케줄러 시작 완료")

    # 8. 온도 파이프라인 스케줄러 — 등록된 온습도 센서마다 스레드 1개씩 시작
    from core.temperature_pipeline.runner import build_temperature_pipeline_from_settings

    temperature_camera_vlm_manager, temperature_scheduler = build_temperature_pipeline_from_settings(
        db_handler=db,
        loop=main_loop,
        broadcast_fn=ws_manager.broadcast,
        mqtt_alert_fn=mqtt_sensor_svc.publish_hazard_alert_to_space_watches,
    )
    app.state.temperature_camera_vlm_manager = temperature_camera_vlm_manager
    temperature_scheduler.start()
    app.state.temperature_scheduler = temperature_scheduler

    logger.info("온도 파이프라인 스케줄러 시작 완료")

    # 9. CCTV autoregressive VLM 파이프라인 — CCTV_RTSP_URL 설정 시 자동 시작
    app.state.vlm_runner = None
    if settings.cctv_rtsp_url:
        from ai.vlm.cctv_runner import CctvVlmRunner

        def _on_vlm_result(text: str):
            import asyncio
            from core.notifications import make_vlm_push_payload

            asyncio.run_coroutine_threadsafe(
                ws_manager.broadcast(make_vlm_push_payload(
                    "vlm_analysis",
                    "CCTV autoregressive VLM 분석 완료",
                    text,
                )),
                main_loop,
            )

        vlm_runner = CctvVlmRunner(
            rtsp_url=settings.cctv_rtsp_url,
            on_result=_on_vlm_result,
        )
        vlm_runner.start()
        app.state.vlm_runner = vlm_runner
        logger.info("CCTV autoregressive VLM 파이프라인 시작 완료 source=%s", settings.cctv_rtsp_url)
    else:
        logger.info("CCTV autoregressive VLM 파이프라인 비활성화 (CCTV_RTSP_URL 미설정)")

    # 10. Fire pipeline manager (CCTV 화재 감지 — cctv/fire_pipeline/ 모듈 기반)
    fire_manager = None
    try:
        from cctv.fire_pipeline import manager as fire_manager_mod
        fire_manager_mod.init_manager(main_loop, ws_manager.broadcast, db)
        fire_manager = fire_manager_mod
        logger.info("Fire pipeline manager 초기화 완료")
    except ImportError as e:
        logger.warning("Fire pipeline 미사용 (모듈 없음): %s", e)

    # 11. DB에 이미 등록된 CCTV runtime 복구
    app.state.cctv_runtime_startup = None
    try:
        from cctv.api.service import start_registered_camera_runtime_components

        app.state.cctv_runtime_startup = await asyncio.to_thread(
            start_registered_camera_runtime_components,
            db,
        )
        logger.info("등록 CCTV runtime 복구 완료: %s", app.state.cctv_runtime_startup)
    except Exception as e:
        logger.warning("등록 CCTV runtime 복구 스킵: %s", e)

    # 12. mDNS 자기 방송
    aiozc, mdns_info = await _start_mdns_broadcast(current_ip)

    logger.info(
        "=== 서버 준비 완료 | IP: %s | Port: %s ===",
        current_ip,
        settings.api_port,
    )

    # 13. 기동 후 자동 점검 — 모든 서비스가 올라온 뒤 상태를 검증한다.
    import asyncio as _asyncio
    from core.startup_check.checker import StartupChecker

    checker = StartupChecker(app)
    app.state.startup_checker = checker
    # 서버가 완전히 accept 상태가 된 뒤 실행되도록 짧게 대기
    _asyncio.get_event_loop().call_later(
        1.5,
        lambda: _asyncio.ensure_future(checker.run_all()),
    )

    yield

    # ── 종료 처리 ────────────────────────────────────────────────────────────

    if fire_manager is not None:
        try:
            fire_manager.stop_all()
        except Exception as e:
            logger.warning("Fire pipeline 종료 중 오류: %s", e)

    try:
        from cctv.buffer import stop_all_buffers
        stop_all_buffers()
    except Exception as e:
        logger.warning("CCTV frame buffer 종료 중 오류: %s", e)

    try:
        from cctv.rtsp import stop_all_readers
        stop_all_readers()
    except Exception as e:
        logger.warning("CCTV RTSP reader 종료 중 오류: %s", e)

    try:
        if app.state.vlm_runner is not None:
            app.state.vlm_runner.stop()
    except Exception as e:
        logger.warning("CCTV autoregressive VLM 파이프라인 종료 중 오류: %s", e)

    try:
        watch_scheduler.stop()
    except Exception as e:
        logger.warning("워치 파이프라인 스케줄러 종료 중 오류: %s", e)

    try:
        watch_camera_vlm_manager.stop_all()
    except Exception as e:
        logger.warning("워치 연동 카메라 VLM 종료 중 오류: %s", e)

    try:
        temperature_scheduler.stop()
    except Exception as e:
        logger.warning("온도 파이프라인 스케줄러 종료 중 오류: %s", e)

    try:
        temperature_camera_vlm_manager.stop_all()
    except Exception as e:
        logger.warning("온도 연동 카메라 VLM 종료 중 오류: %s", e)

    try:
        await mdns_service.stop()
    except Exception as e:
        logger.warning("mDNS 센서 탐색 서비스 종료 중 오류: %s", e)

    try:
        mqtt_sensor_svc.stop()
    except Exception as e:
        logger.warning("MQTT 센서 서비스 종료 중 오류: %s", e)

    if aiozc and mdns_info:
        try:
            await aiozc.async_unregister_service(mdns_info)
            await aiozc.async_close()
        except Exception as e:
            logger.warning("Jetson mDNS 방송 종료 중 오류: %s", e)

    try:
        if app.state.vllm_server_manager is not None:
            app.state.vllm_server_manager.stop()
    except Exception as e:
        logger.warning("VLM 서버 종료 중 오류: %s", e)

    logger.info("서버 종료 완료")


async def _start_mdns_broadcast(current_ip: str):
    try:
        from zeroconf import ServiceInfo
        from zeroconf.asyncio import AsyncZeroconf

        service_type = settings.mdns_service_type

        if not service_type.endswith(".local."):
            service_type = service_type.rstrip(".") + ".local."

        service_name = settings.mdns_service_name

        if not service_name.endswith(service_type):
            service_name = f"{service_name.rstrip('.')}.{service_type}"

        info = ServiceInfo(
            service_type,
            service_name,
            addresses=[socket.inet_aton(current_ip)],
            port=settings.api_port,
            properties={
                "desc": "Industrial Safety Monitoring System",
                "ip_addr": current_ip,
                "port": str(settings.api_port),
            },
        )

        aiozc = AsyncZeroconf()
        await aiozc.async_register_service(info)

        logger.info(
            "[mDNS] Jetson 방송 시작 | name=%s | type=%s | IP=%s | Port=%s",
            service_name,
            service_type,
            current_ip,
            settings.api_port,
        )

        return aiozc, info

    except Exception as e:
        logger.warning(
            "[mDNS] Jetson 방송 실패 (스킵): type=%s, name=%s, error_type=%s, error=%r",
            getattr(settings, "mdns_service_type", None),
            getattr(settings, "mdns_service_name", None),
            type(e).__name__,
            e,
        )
        return None, None


# ── FastAPI 앱 ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DS Edge Device API",
    description="산업 안전 모니터링 엣지 디바이스 메인 API",
    version="2.0.0",
    lifespan=lifespan,
)


# ── 라우터 등록 ───────────────────────────────────────────────────────────────

from fastapi import APIRouter

api = APIRouter(prefix="/api/v1")

from process.router import router as process_router, spaces_router
from worker.router import router as worker_router
from cctv.api.scenario import router as scenario_router
from temperature.api.router import router as temperature_router, web_router as temp_web_router
from cctv.api.router import router as cctv_router
from core.jetson.router import router_v1 as jetson_v1_router
from dashboard.router import router as dashboard_router

api.include_router(process_router)
api.include_router(spaces_router)
api.include_router(jetson_v1_router)
api.include_router(dashboard_router)
api.include_router(worker_router)
api.include_router(temperature_router)
api.include_router(cctv_router)
api.include_router(scenario_router)

app.include_router(api)


# 기존 API 호환 (/api/...)
from worker.router import legacy_router as worker_legacy_router
from core.jetson.router import router as jetson_router
from core.sensor_registry.router import router as sensor_registry_router
from core.map.router import router as map_router
from core.watch_pipeline.router import router as watch_vlm_router
from core.temperature_pipeline.router import router as temperature_vlm_router
from core.report.router import events_router, internal_router, reports_router

app.include_router(jetson_router)
app.include_router(sensor_registry_router)
app.include_router(map_router)
app.include_router(watch_vlm_router)
app.include_router(temperature_vlm_router)
app.include_router(worker_legacy_router)
app.include_router(temp_web_router)
app.include_router(internal_router)
app.include_router(events_router)
app.include_router(reports_router)


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("[WS] alerts connection error id=%s error=%s", id(websocket), exc)
        ws_manager.disconnect(websocket)


@app.websocket("/ws/vital")
async def ws_vital(websocket: WebSocket):
    await websocket.accept()
    active_vital_ws.append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_vital_ws:
            active_vital_ws.remove(websocket)


@app.websocket("/ws/th")
async def ws_th(websocket: WebSocket):
    await websocket.accept()
    active_th_ws.append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_th_ws:
            active_th_ws.remove(websocket)


@app.websocket("/reg/band")
async def ws_band(websocket: WebSocket):
    await websocket.accept()
    logger.info("밴드 등록 WebSocket 연결")

    try:
        while True:
            data = await websocket.receive_text()
            logger.info("[밴드 로그]: %s", data)
    except WebSocketDisconnect:
        logger.info("밴드 등록 WebSocket 종료")


# ── 헬스체크 ──────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def root():
    from core.jetson.service import get_real_ip

    return {
        "status": "online",
        "ip_addr": get_real_ip(),
        "project": "Industrial Safety Monitoring",
    }


@app.get("/health/detail", tags=["health"])
async def health_detail(request: Request):
    """기동 점검 결과를 반환하고, 필요 시 즉시 재점검을 실행한다."""
    checker = getattr(request.app.state, "startup_checker", None)
    if checker is None:
        return {"error": "startup_checker 미초기화"}
    await checker.run_all()
    return checker.summary()


@app.get("/health", tags=["health"])
def health(request: Request):
    from ai.vlm.timeshare import get_vlm_timeshare_scheduler

    vllm_manager = getattr(request.app.state, "vllm_server_manager", None)
    yolo_engine_ready = getattr(request.app.state, "yolo_engine_ready", False)
    vllm_status = None
    vllm_ready = False
    vllm_detail = "manager_not_started"
    temperature_scheduler = getattr(request.app.state, "temperature_scheduler", None)

    if vllm_manager is not None:
        vllm_status = vllm_manager.status()
        vllm_ready, vllm_detail = vllm_manager.check_ready_once()

    return {
        "status": "ok",
        "yolo_engine_ready": yolo_engine_ready,
        "vllm_ready": vllm_ready,
        "vllm_detail": vllm_detail,
        "vllm": vllm_status,
        "vlm_timeshare": get_vlm_timeshare_scheduler().status(),
        "temperature_pipeline": (
            temperature_scheduler.get_status()
            if temperature_scheduler is not None
            else None
        ),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )
