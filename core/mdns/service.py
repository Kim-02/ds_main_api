"""mDNS 센서 탐색 서비스 — _onsafe-sensor._tcp.local. 스캔."""
import asyncio
import logging
import socket
from datetime import datetime
from typing import Any, Dict, Optional

from zeroconf import ServiceBrowser, ServiceStateChange
from zeroconf.asyncio import AsyncZeroconf

logger = logging.getLogger(__name__)


class MdnsSensorService:
    SERVICE_TYPE = "_onsafe-sensor._tcp.local."

    def __init__(self):
        self.aiozc: Optional[AsyncZeroconf] = None
        self.browser = None
        self.discovered_sensors: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self.loop = None

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
        logger.info("[mDNS Sensor] 스캐너 시작")

    async def stop(self):
        self._running = False
        if self.aiozc:
            await self.aiozc.async_close()
            self.aiozc = None
        logger.info("[mDNS Sensor] 스캐너 중단")

    def get_discovered_sensors(self) -> list:
        return list(self.discovered_sensors.values())

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
            }
            logger.info("[mDNS Sensor] 발견/갱신: %s @ %s", sensor_id, ip_addr)
        except Exception as e:
            logger.error("[mDNS Sensor] upsert 오류: %s", e)

    async def _handle_removed(self, name: str):
        for sensor_id, sensor in self.discovered_sensors.items():
            if sensor.get("mdns_hostname") == name.rstrip("."):
                self.discovered_sensors[sensor_id]["is_online"] = False
                self.discovered_sensors[sensor_id]["last_seen_at"] = datetime.now()
                logger.info("[mDNS Sensor] 오프라인: %s", sensor_id)
                break
