"""paho-mqtt 기반 센서 서비스.

구독 토픽:
  기존) sensors/+/status      ← 센서 온라인 상태
        sensors/+/telemetry   ← 구형 payload (sensor_type 포함)
  신규) sensor/+/+            ← sensor/{sensor_id}/{data_type}
        sensors/+/data        ← sensors/{sensor_id}/data

처리 흐름:
  수신한 센서 데이터는 DB에 저장만 한다.
  휴식 권고 판단은 WatchPipelineScheduler(스레드 per 워치)가 담당한다.
"""
import json
import logging
import threading
from datetime import datetime
from typing import Callable, Optional

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

_TH_TYPES = {"temp_humidity", "th", "temperature", "temperature_humidity"}
_HB_TYPES = {"heart_band", "heartbeat", "heart_rate", "hb", "watch"}


class MqttSensorService:
    def __init__(
        self,
        db_handler,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        on_th_data: Optional[Callable[[dict], None]] = None,
        on_hb_data: Optional[Callable[[dict], None]] = None,
    ):
        self.db_handler = db_handler
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.on_th_data = on_th_data
        self.on_hb_data = on_hb_data

        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self._thread = threading.Thread(target=self.client.loop_forever, daemon=True)
            self._thread.start()
            self._running = True
            logger.info("[MQTT Sensor] 서비스 시작 broker=%s:%s", self.broker_host, self.broker_port)
        except Exception as e:
            self._running = False
            logger.error("[MQTT Sensor] 시작 실패: %s", e)

    def stop(self) -> None:
        self._running = False
        try:
            self.client.disconnect()
        except Exception:
            pass
        logger.info("[MQTT Sensor] 서비스 중단")

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc != 0:
            logger.error("[MQTT Sensor] 브로커 연결 실패 rc=%d", rc)
            return
        logger.info("[MQTT Sensor] 브로커 연결 완료")
        client.subscribe("sensors/+/status")
        client.subscribe("sensors/+/telemetry")
        client.subscribe("sensor/+/+")
        client.subscribe("sensors/+/data")

    def _on_message(self, client, userdata, msg) -> None:
        topic: str = msg.topic
        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            logger.warning("[MQTT Sensor] JSON 파싱 오류 topic=%s: %s", topic, e)
            return
        if not isinstance(payload, dict):
            return

        if topic.endswith("/status"):
            self._handle_status(payload)
        elif topic.endswith("/telemetry"):
            self._handle_telemetry(topic, payload)
        else:
            self._handle_sensor_data_topic(topic, payload)

    def _handle_status(self, payload: dict) -> None:
        sensor_id = payload.get("sensor_id")
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            return
        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            logger.warning("[MQTT Sensor] status DB 업데이트 실패 sensor_id=%s: %s", sensor_id, e)

    def _handle_telemetry(self, topic: str, payload: dict) -> None:
        sensor_id = payload.get("sensor_id") or self._extract_sensor_id_from_topic(topic)
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            return
        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            logger.warning("[MQTT Sensor] online 업데이트 실패 sensor_id=%s: %s", sensor_id, e)

        sensor_type = str(payload.get("sensor_type") or "")
        if not sensor_type:
            row = self.db_handler.get_sensor_by_sensor_id(sensor_id)
            sensor_type = str(row.get("sensor_type") or "") if row else ""

        data_type = self._classify_data_type(sensor_type, payload)
        if data_type == "th":
            self._store_th_by_sensor_id(topic, sensor_id, payload)
        elif data_type == "hb":
            self._store_hb_by_sensor_id(topic, sensor_id, payload)

    def _handle_sensor_data_topic(self, topic: str, payload: dict) -> None:
        sensor_id = self._extract_sensor_id_from_topic(topic) or payload.get("sensor_id")
        if not sensor_id:
            return

        sensor_row = self.db_handler.get_sensor_by_sensor_id(sensor_id)
        if sensor_row is None:
            sensor_row = self.db_handler.get_sensor_by_mqtt_topic(topic)
        if sensor_row is None:
            return

        sen_id: int = sensor_row["sen_id"]
        sensor_type: str = str(sensor_row.get("sensor_type") or "")
        ts = payload.get("time") or payload.get("timestamp") or datetime.now()

        data_type = self._classify_data_type(sensor_type, payload)
        if data_type == "th":
            self._save_th(topic, sensor_id, sen_id, ts, payload)
        elif data_type == "hb":
            self._save_hb(topic, sensor_id, sen_id, ts, payload)
        else:
            return

        try:
            self.db_handler.update_sensor_last_seen_by_id(sen_id)
        except Exception as e:
            logger.warning("[MQTT Sensor] last_seen 갱신 실패 sen_id=%s: %s", sen_id, e)

    def _save_th(self, topic: str, sensor_id: str, sen_id: int, ts, payload: dict) -> None:
        temp = payload.get("temp") if payload.get("temp") is not None else payload.get("temperature")
        humid = payload.get("humid") if payload.get("humid") is not None else payload.get("humidity")
        if temp is None and humid is None:
            return
        ok = self.db_handler.insert_th_trans(sen_id, ts, temp, humid)
        if ok:
            logger.info("[MQTT Sensor] 온습도 업데이트 sensor_id=%s temp=%s humid=%s", sensor_id, temp, humid)
            self._emit_th(sensor_id, temp, humid, ts)

    def _save_hb(self, topic: str, sensor_id: str, sen_id: int, ts, payload: dict) -> None:
        hr = payload.get("hr") if payload.get("hr") is not None else payload.get("heart_rate")
        if hr is None:
            return
        try:
            hr = float(hr)
        except Exception:
            return
        if hr <= 0:
            return
        ok = self.db_handler.insert_hb_trans(sen_id, ts, hr)
        if ok:
            logger.info("[MQTT Sensor] 심박 업데이트 sensor_id=%s hr=%s", sensor_id, hr)
            self._emit_hb(sensor_id, hr, ts)

    def _store_th_by_sensor_id(self, topic: str, sensor_id: str, payload: dict) -> None:
        temp = payload.get("temp") if payload.get("temp") is not None else payload.get("temperature")
        humid = payload.get("humid") if payload.get("humid") is not None else payload.get("humidity")
        try:
            self.db_handler.save_sensor_telemetry(
                sensor_id=sensor_id, temperature=temp, humidity=humid, ts=datetime.now()
            )
            logger.info("[MQTT Sensor] 온습도 업데이트 sensor_id=%s temp=%s humid=%s", sensor_id, temp, humid)
            self._emit_th(sensor_id, temp, humid, datetime.now())
        except Exception as e:
            logger.error("[MQTT Sensor] 온습도 저장 실패 sensor_id=%s: %s", sensor_id, e)

    def _store_hb_by_sensor_id(self, topic: str, sensor_id: str, payload: dict) -> None:
        hr = payload.get("hr") if payload.get("hr") is not None else payload.get("heart_rate")
        if hr is None:
            return
        try:
            hr = float(hr)
        except Exception:
            return
        if hr <= 0:
            return
        try:
            self.db_handler.save_heart_rate_telemetry(
                sensor_id=sensor_id, hr=hr, ts=datetime.now()
            )
            logger.info("[MQTT Sensor] 심박 업데이트 sensor_id=%s hr=%s", sensor_id, hr)
            self._emit_hb(sensor_id, hr, datetime.now())
        except Exception as e:
            logger.error("[MQTT Sensor] 심박 저장 실패 sensor_id=%s: %s", sensor_id, e)

    def _emit_th(self, sensor_id: str, temp, humid, ts) -> None:
        if self.on_th_data is None:
            return
        try:
            self.on_th_data({
                "type": "th",
                "sensor_id": sensor_id,
                "temp": temp,
                "humid": humid,
                "time": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            })
        except Exception as e:
            logger.warning("[MQTT Sensor] 온습도 웹소켓 전송 실패 sensor_id=%s: %s", sensor_id, e)

    def _emit_hb(self, sensor_id: str, hr, ts) -> None:
        if self.on_hb_data is None:
            return
        try:
            self.on_hb_data({
                "type": "watch",
                "sensor_id": sensor_id,
                "hr": hr,
                "time": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            })
        except Exception as e:
            logger.warning("[MQTT Sensor] 심박 웹소켓 전송 실패 sensor_id=%s: %s", sensor_id, e)

    @staticmethod
    def _extract_sensor_id_from_topic(topic: str) -> Optional[str]:
        parts = topic.split("/")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
        return None

    @staticmethod
    def _classify_data_type(sensor_type: str, payload: dict) -> str:
        st = sensor_type.lower().strip()
        if st in _TH_TYPES:
            return "th"
        if st in _HB_TYPES:
            return "hb"
        if any(k in payload for k in ("temp", "temperature", "humid", "humidity")):
            return "th"
        if any(k in payload for k in ("hr", "heart_rate")):
            return "hb"
        return "unknown"

    # ── Publish 명령 ──────────────────────────────────────────────────────────

    def publish(self, topic: str, payload: str) -> None:
        """범용 MQTT 발행 — WatchPipelineScheduler 등 외부에서 사용."""
        self.client.publish(topic, payload)

    def publish_register(self, sensor_id: str, site_id: str, interval_ms: int = 5000) -> None:
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "register", "site_id": site_id, "interval_ms": interval_ms}),
        )
        logger.info("[MQTT Sensor] register 발행 sensor_id=%s", sensor_id)

    def publish_unregister(self, sensor_id: str) -> None:
        self.client.publish(f"sensors/{sensor_id}/cmd", json.dumps({"cmd": "unregister"}))
        logger.info("[MQTT Sensor] unregister 발행 sensor_id=%s", sensor_id)

    def publish_set_interval(self, sensor_id: str, interval_ms: int) -> None:
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "set_interval", "interval_ms": interval_ms}),
        )

    def publish_alert(
        self,
        sensor_id: str,
        color: str = "red",
        vibration: bool = True,
        led: bool = True,
        duration_ms: int = 5000,
        reset_after_ms: int = 10000,
    ) -> None:
        topic = f"sensors/{sensor_id}/alert"
        self.client.publish(
            topic,
            json.dumps({
                "command": "alert_on",
                "color": color,
                "vibration": vibration,
                "led": led,
                "duration_ms": duration_ms,
                "reset_after_ms": reset_after_ms,
            }),
        )
        logger.info("[MQTT Sensor] alert 발행 sensor_id=%s", sensor_id)
