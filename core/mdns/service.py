"""mDNS 센서 탐색 서비스 — _onsafe-sensor._tcp.local. 스캔."""
import asyncio
import logging
import socket
from datetime import datetime
from typing import Any, Dict, Optional

from zeroconf import ServiceBrowser, ServiceStateChange
from zeroconf.asyncio import AsyncZeroconf

logger = logging.getLogger(__name__)

DISCOVERY_TTL_SECONDS = 15
CLEANUP_INTERVAL_SECONDS = 5


class MdnsSensorService:
    SERVICE_TYPE = "_onsafe-sensor._tcp.local."

    def __init__(self, db_handler=None):
        self.aiozc: Optional[AsyncZeroconf] = None
        self.browser = None
        self.discovered_sensors: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self.loop = None
        self._db = db_handler

    async def start(self):
        if self._running:
            return
        self.loop = asyncio.get_running_loop()
        self.aiozc = AsyncZeroconf()
        self.browser = ServiceBrowser(
            self.aiozc.zeroconf,
            self.SERVICE_TYPE,
            handlers=[self._on_service_state_change],
        )
        self._running = True
        asyncio.create_task(self._cleanup_loop())
        logger.info("[mDNS Sensor] 스캐너 시작 (TTL=%ds)", DISCOVERY_TTL_SECONDS)

    async def stop(self):
        self._running = False
        if self.aiozc:
            await self.aiozc.async_close()
            self.aiozc = None
        logger.info("[mDNS Sensor] 스캐너 중단")

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------

    def get_discovered_sensors(self) -> list:
        """TTL 이내에 확인된 온라인 센서만 반환한다."""
        now = datetime.now()
        result = []
        for sensor in list(self.discovered_sensors.values()):
            last_seen = sensor.get("last_seen_at")
            if last_seen is None:
                continue
            age = (now - last_seen).total_seconds()
            if sensor.get("is_online") and age <= DISCOVERY_TTL_SECONDS:
                result.append(sensor)
        return result

    def is_sensor_online(self, sensor_id: str) -> bool:
        """sensor_id가 TTL 이내의 온라인 센서인지 확인한다."""
        sensor = self.discovered_sensors.get(sensor_id)
        if not sensor:
            return False
        if not sensor.get("is_online"):
            return False
        last_seen = sensor.get("last_seen_at")
        if last_seen is None:
            return False
        age = (datetime.now() - last_seen).total_seconds()
        return age <= DISCOVERY_TTL_SECONDS

    # ------------------------------------------------------------------
    # 내부 이벤트 핸들러
    # ------------------------------------------------------------------

    def _on_service_state_change(self, zeroconf, service_type, name, state_change):
        if not self.loop:
            return
        if state_change in (ServiceStateChange.Added, ServiceStateChange.Updated):
            asyncio.run_coroutine_threadsafe(self._handle_upsert(name), self.loop)
        elif state_change is ServiceStateChange.Removed:
            asyncio.run_coroutine_threadsafe(self._handle_removed(name), self.loop)

    async def _handle_upsert(self, name: str):
        if not self.aiozc:
            return
        try:
            info = await self.aiozc.async_get_service_info(self.SERVICE_TYPE, name, timeout=3000)
            if not info:
                return

            props = {
                (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                for k, v in info.properties.items()
            }

            sensor_id = props.get("sensor_id")
            if not sensor_id:
                return

            ip_addr = None
            if info.addresses:
                try:
                    ip_addr = socket.inet_ntoa(info.addresses[0])
                except Exception:
                    pass

            mqtt_base = props.get("mqtt_base", f"sensors/{sensor_id}")

            self.discovered_sensors[sensor_id] = {
                "sensor_id": sensor_id,
                "sensor_type": props.get("sensor_type", "unknown"),
                "sen_name": props.get("sen_name", sensor_id),
                "sen_locate": props.get("sen_locate", "default"),
                "model": props.get("model", ""),
                "mqtt_base": mqtt_base,
                "mqtt_topic": f"{mqtt_base}/telemetry",
                "status_topic": f"{mqtt_base}/status",
                "cmd_topic": f"{mqtt_base}/cmd",
                "alert_topic": f"{mqtt_base}/alert",
                "mdns_hostname": info.server.rstrip(".") if info.server else name.rstrip("."),
                "ip_addr": ip_addr,
                "is_online": True,
                "last_seen_at": datetime.now(),
                "_service_name": name,
            }
            logger.info("[mDNS Sensor] 발견/갱신: %s @ %s", sensor_id, ip_addr)
        except Exception as e:
            logger.error("[mDNS Sensor] upsert 오류: %s", e)

    async def _handle_removed(self, name: str):
        for sensor_id, sensor in list(self.discovered_sensors.items()):
            if sensor.get("mdns_hostname") == name.rstrip(".") or sensor.get("_service_name") == name:
                self.discovered_sensors[sensor_id]["is_online"] = False
                self.discovered_sensors[sensor_id]["last_seen_at"] = datetime.now()
                logger.info("[mDNS Sensor] 오프라인(Removed): %s", sensor_id)
                break

    # ------------------------------------------------------------------
    # TTL 기반 정리 + 능동 재확인
    # ------------------------------------------------------------------

    async def _cleanup_loop(self):
        """주기적으로 알려진 센서를 재확인하고 TTL 초과 엔트리를 제거한다."""
        while self._running:
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
            try:
                await self._refresh_all_sensors()
                self._remove_stale_sensors()
            except Exception as e:
                logger.debug("[mDNS Sensor] cleanup loop 오류: %s", e)

    async def _refresh_all_sensors(self):
        """발견된 온라인 센서에 mDNS 쿼리를 보내 last_seen_at을 갱신한다."""
        if not self.aiozc:
            return
        tasks = []
        for sensor_id, sensor in list(self.discovered_sensors.items()):
            if not sensor.get("is_online"):
                continue
            service_name = sensor.get("_service_name")
            if service_name:
                tasks.append(self._try_refresh(sensor_id, service_name))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _try_refresh(self, sensor_id: str, service_name: str):
        """단일 센서 mDNS 재확인 — 응답하면 last_seen_at 갱신."""
        try:
            info = await self.aiozc.async_get_service_info(
                self.SERVICE_TYPE, service_name, timeout=2000
            )
            if info and sensor_id in self.discovered_sensors:
                self.discovered_sensors[sensor_id]["last_seen_at"] = datetime.now()
        except Exception:
            pass

    def _remove_stale_sensors(self):
        """last_seen_at이 TTL을 초과한 센서를 discovered 목록에서 제거한다."""
        now = datetime.now()
        stale = []
        for sensor_id, sensor in list(self.discovered_sensors.items()):
            last_seen = sensor.get("last_seen_at")
            if last_seen is None:
                stale.append(sensor_id)
                continue
            age = (now - last_seen).total_seconds()
            if age > DISCOVERY_TTL_SECONDS:
                stale.append(sensor_id)

        for sensor_id in stale:
            logger.info(
                "[mDNS Sensor] TTL 초과 제거: %s (TTL=%ds)",
                sensor_id, DISCOVERY_TTL_SECONDS,
            )
            # DB에 등록된 센서라면 is_online=0 업데이트 (DB 핸들러가 있을 때만)
            if self._db is not None:
                try:
                    self._db.update_sensor_online_status(sensor_id, online=False)
                except Exception:
                    pass
            self.discovered_sensors.pop(sensor_id, None)
