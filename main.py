import logging
import socket
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from config import settings
from database import init_db

logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── WebSocket 연결 관리 ──────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active_connections.discard(ws) if hasattr(self.active_connections, "discard") else None
        if ws in self.active_connections:
            self.active_connections.remove(ws)

    async def broadcast(self, message: dict):
        for ws in list(self.active_connections):
            try:
                await ws.send_json(message)
            except Exception:
                pass


ws_manager = ConnectionManager()
active_vital_ws: List[WebSocket] = []
active_th_ws: List[WebSocket] = []


# ── Transmission (WebSocket 브로드캐스트 인터페이스) ────────────────────────

class RealTransmission:
    """SafetyDetectionModule → WebSocket 브로드캐스트 어댑터."""

    import asyncio as _asyncio

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


# ── Lifespan ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio

    # 1. SQLite (새 모델) DB 초기화
    logger.info("SQLite DB 초기화...")
    await init_db()

    # 2. MariaDB 핸들러 (기존 Jetson DB)
    from database.db_handler import DatabaseHandler
    db = DatabaseHandler(
        host=settings.mariadb_host,
        user=settings.mariadb_user,
        password=settings.mariadb_password,
        db_name=settings.mariadb_db_name,
        port=settings.mariadb_port,
    )
    app.state.db = db

    # 3. IP 감지 + Jetson 정보 DB 업데이트
    from core.jetson.service import get_real_ip, startup_db_init
    current_ip = get_real_ip()
    startup_db_init(db, current_ip, settings.api_port)

    # 4. Transmission 어댑터
    main_loop = asyncio.get_running_loop()
    app.state.transmission = RealTransmission(main_loop)

    # 5. SafetyDetectionModule (기존 코어엔진 — 있으면 로드, 없으면 stub)
    try:
        import sys, os
        sys.path.insert(0, "/home/vic06/Desktop/ds_api/jetson_api")
        from app.core_engine import SafetyDetectionModule
        app.state.safety_core = SafetyDetectionModule(db, app.state.transmission)
        app.state.safety_core.update_and_get_subscriptions()
        logger.info("SafetyDetectionModule 로드 완료")

        from app.sensor_listener import SensorDataCollector
        sensor_collector = SensorDataCollector(app.state.safety_core)
        sensor_collector.start()
        logger.info("SensorDataCollector 시작")
    except Exception as e:
        logger.warning("SafetyDetectionModule 없음 (스킵): %s", e)
        app.state.safety_core = None

    # 6. mDNS 센서 탐색 서비스 시작
    from core.mdns.service import MdnsSensorService
    mdns_service = MdnsSensorService()
    await mdns_service.start()
    app.state.mdns_service = mdns_service

    # 7. paho MQTT 센서 서비스 시작
    from core.mqtt.sensor_service import MqttSensorService
    mqtt_sensor_svc = MqttSensorService(
        db_handler=db,
        broker_host=settings.mqtt_broker_host,
        broker_port=settings.mqtt_broker_port,
    )
    mqtt_sensor_svc.start()
    app.state.mqtt_sensor_service = mqtt_sensor_svc

    # 8. aiomqtt 클라이언트 (새 센서 토픽용)
    try:
        from core.mqtt import start as mqtt_start
        await mqtt_start()
        logger.info("aiomqtt 클라이언트 시작")
    except Exception as e:
        logger.warning("aiomqtt 시작 실패 (스킵): %s", e)

    # 9. DB 동기화 스케줄러
    from core.scheduler import start as scheduler_start
    scheduler_start()

    # 10. 기등록 카메라 RTSP 버퍼 복원
    await _start_camera_buffers()

    # 11. mDNS 자기 방송 (앱이 Jetson을 탐색할 수 있도록)
    aiozc, mdns_info = await _start_mdns_broadcast(current_ip)

    logger.info("=== Edge Device API 서버 준비 완료 (IP: %s, Port: %s) ===", current_ip, settings.api_port)

    yield  # ── 서버 가동 중 ──────────────────────────────────────────────

    # 종료 처리
    logger.info("서버 종료 중...")
    await mdns_service.stop()
    mqtt_sensor_svc.stop()

    if aiozc and mdns_info:
        await aiozc.async_unregister_service(mdns_info)
        await aiozc.async_close()

    from core.mqtt import stop as mqtt_stop
    await mqtt_stop()

    from core.scheduler import stop as scheduler_stop
    scheduler_stop()


async def _start_camera_buffers() -> None:
    from database import AsyncSessionLocal
    from database.crud import sensor as sensor_crud
    from cctv.rtsp import register_reader
    from cctv.buffer import start_buffer

    async with AsyncSessionLocal() as db:
        sensors = await sensor_crud.get_all(db)

    for sensor in sensors:
        if not sensor.is_active or sensor.camera is None:
            continue
        cam = sensor.camera
        register_reader(cam.id, cam.rtsp_url)
        start_buffer(cam.id, sensor.process_id)
        logger.info("카메라 버퍼 복원: camera_id=%s", cam.id)


async def _start_mdns_broadcast(current_ip: str):
    """Jetson 자신을 mDNS로 방송 — 앱이 자동 탐색 가능."""
    try:
        from zeroconf import ServiceInfo
        from zeroconf.asyncio import AsyncZeroconf

        info = ServiceInfo(
            settings.mdns_service_type,
            settings.mdns_service_name,
            addresses=[socket.inet_aton(current_ip)],
            port=settings.api_port,
            properties={"desc": "Industrial Safety Monitoring System"},
        )
        aiozc = AsyncZeroconf()
        await aiozc.async_register_service(info)
        logger.info("[mDNS] Jetson 방송 시작 (IP: %s, Port: %s)", current_ip, settings.api_port)
        return aiozc, info
    except Exception as e:
        logger.warning("[mDNS] Jetson 방송 실패 (스킵): %s", e)
        return None, None


# ── FastAPI 앱 ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DS Edge Device API",
    description="산업 안전 모니터링 엣지 디바이스 메인 API",
    version="2.0.0",
    lifespan=lifespan,
)

# ── 라우터 등록 ──────────────────────────────────────────────────────────────

from fastapi import APIRouter

api = APIRouter(prefix="/api/v1")

# 공정·작업자
from process.router import router as process_router
from worker.router import router as worker_router, legacy_router as worker_legacy_router
api.include_router(process_router)
api.include_router(worker_router)

# 온습도 팀
from temperature.api.router import router as temperature_router, web_router as temp_web_router
api.include_router(temperature_router)

# 심박 팀
from heartrate.api.router import router as heartrate_router, web_router as hr_web_router
api.include_router(heartrate_router)

# CCTV 팀
from cctv.api.router import router as cctv_router, legacy_router as cctv_legacy_router
api.include_router(cctv_router)

# 이상 이벤트·리포트
from core.report.router import events_router, reports_router, internal_router
api.include_router(events_router)
api.include_router(reports_router)

app.include_router(api)

# 기존 API 호환 (/api/...)
from core.jetson.router import router as jetson_router
from core.sensor_registry.router import router as sensor_registry_router
from core.map.router import router as map_router

app.include_router(jetson_router)
app.include_router(sensor_registry_router)
app.include_router(map_router)
app.include_router(worker_legacy_router)
app.include_router(cctv_legacy_router)
app.include_router(temp_web_router)
app.include_router(hr_web_router)
app.include_router(internal_router)


# ── WebSocket 엔드포인트 ──────────────────────────────────────────────────────

@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await ws_manager.connect(websocket)
    logger.info("WebSocket 연결 (alerts) — 현재 연결 수: %d", len(ws_manager.active_connections))
    try:
        while True:
            data = await websocket.receive_text()
            logger.debug("[앱 응답]: %s", data)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info("WebSocket 종료 (alerts)")


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


# ── 헬스체크 ─────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def root():
    from core.jetson.service import get_real_ip
    return {
        "status": "online",
        "ip_addr": get_real_ip(),
        "project": "Industrial Safety Monitoring",
    }


@app.get("/health", tags=["health"])
def health():
    from core.detection_manager import manager
    return {
        "status": "ok",
        "active_detections": len(manager.get_active_sessions()),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=settings.debug)
