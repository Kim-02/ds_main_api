import logging
import socket
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from config import settings

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


# ── Transmission 어댑터 (SafetyDetectionModule → WebSocket) ─────────────────

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

    # 2. IP 감지 + Jetson DB 업데이트
    from core.jetson.service import get_real_ip, startup_db_init
    current_ip = get_real_ip()
    startup_db_init(db, current_ip, settings.api_port)

    # 3. Transmission + SafetyDetectionModule (있으면 로드)
    main_loop = asyncio.get_running_loop()
    app.state.transmission = RealTransmission(main_loop)
    app.state.safety_core = None

    try:
        import sys
        sys.path.insert(0, "/home/vic06/Desktop/ds_api/jetson_api")
        from app.core_engine import SafetyDetectionModule
        app.state.safety_core = SafetyDetectionModule(db, app.state.transmission)
        app.state.safety_core.update_and_get_subscriptions()
        from app.sensor_listener import SensorDataCollector
        SensorDataCollector(app.state.safety_core).start()
        logger.info("SafetyDetectionModule 로드 완료")
    except Exception as e:
        logger.warning("SafetyDetectionModule 없음 (스킵): %s", e)

    # 4. mDNS 센서 탐색 시작
    from core.mdns.service import MdnsSensorService
    mdns_service = MdnsSensorService()
    await mdns_service.start()
    app.state.mdns_service = mdns_service

    # 5. DB에서 temperature MQTT 핸들러에 db 주입
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

    # 7. mDNS 자기 방송 (앱이 Jetson을 탐색할 수 있도록)
    aiozc, mdns_info = await _start_mdns_broadcast(current_ip)

    logger.info("=== 서버 준비 완료 | IP: %s | Port: %s ===", current_ip, settings.api_port)

    yield  # ── 서버 가동 중 ────────────────────────────────────────────────

    # 종료
    await mdns_service.stop()
    mqtt_sensor_svc.stop()

    if aiozc and mdns_info:
        await aiozc.async_unregister_service(mdns_info)
        await aiozc.async_close()

    logger.info("서버 종료 완료")


async def _start_mdns_broadcast(current_ip: str):
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
        logger.info("[mDNS] Jetson 방송 시작 | IP: %s | Port: %s", current_ip, settings.api_port)
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

# ── 라우터 등록 ───────────────────────────────────────────────────────────────

from fastapi import APIRouter

api = APIRouter(prefix="/api/v1")

from process.router import router as process_router
from worker.router import router as worker_router
from temperature.api.router import router as temperature_router, web_router as temp_web_router

api.include_router(process_router)
api.include_router(worker_router)
api.include_router(temperature_router)
app.include_router(api)

# 기존 API 호환 (/api/...)
from worker.router import legacy_router as worker_legacy_router
from core.jetson.router import router as jetson_router
from core.sensor_registry.router import router as sensor_registry_router
from core.map.router import router as map_router
from core.report.router import internal_router

app.include_router(jetson_router)
app.include_router(sensor_registry_router)
app.include_router(map_router)
app.include_router(worker_legacy_router)
app.include_router(temp_web_router)
app.include_router(internal_router)


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await ws_manager.connect(websocket)
    logger.info("WebSocket 연결 (alerts) — 연결 수: %d", len(ws_manager.active_connections))
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
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


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=settings.debug)
