"""paho-mqtt 기반 센서 서비스 — sensors/+/status, sensors/+/telemetry 구독."""
import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

import paho.mqtt.client as mqtt

from core.mqtt.topics import Topics


class MqttSensorService:
    def __init__(self, db_handler, broker_host="127.0.0.1", broker_port=1883):
        self.db_handler = db_handler
        self.broker_host = broker_host
        self.broker_port = broker_port

        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        self._thread: Optional[threading.Thread] = None
        self._running = False

        self.hr_windows = defaultdict(lambda: deque(maxlen=10))
        self.hr_alert_states: dict = {}

    def start(self):
        if self._running:
            return
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self._thread = threading.Thread(target=self.client.loop_forever, daemon=True)
            self._thread.start()
            self._running = True
            print("[MQTT Sensor] 서비스 시작")
        except Exception as e:
            self._running = False
            print(f"[MQTT Sensor] 시작 실패: {e}")

    def stop(self):
        self._running = False
        try:
            self.client.disconnect()
        except Exception:
            pass
        print("[MQTT Sensor] 서비스 중단")

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("[MQTT Sensor] 브로커 연결 완료")
            client.subscribe(Topics.SENSOR_STATUS_WILDCARD)
            client.subscribe(Topics.SENSOR_TELEMETRY_WILDCARD)
        else:
            print(f"[MQTT Sensor] 연결 실패 rc={rc}")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            print(f"[MQTT Sensor] JSON 파싱 오류 {topic}: {e}")
            return

        if topic.endswith("/status"):
            self._handle_status(payload)
        elif topic.endswith("/telemetry"):
            self._handle_telemetry(payload)

    def _handle_status(self, payload: dict):
        sensor_id = payload.get("sensor_id")
        if not sensor_id:
            return
        if not self.db_handler.is_registered_sensor(sensor_id):
            return
        try:
            self.db_handler.update_sensor_online(sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now())
        except Exception as e:
            print(f"[MQTT Sensor] status DB 업데이트 실패: {e}")

    def _handle_telemetry(self, payload: dict):
        sensor_id = payload.get("sensor_id")
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            return

        sensor_type = payload.get("sensor_type", "unknown")
        try:
            self.db_handler.update_sensor_online(sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now())
        except Exception as e:
            print(f"[MQTT Sensor] online 업데이트 실패: {e}")

        if sensor_type == "temp_humidity":
            self._handle_temp_humidity(sensor_id, payload)
        elif sensor_type == "heart_band":
            self._handle_heart_band(sensor_id, payload)

    def _handle_temp_humidity(self, sensor_id: str, payload: dict):
        temp = payload.get("temperature")
        humid = payload.get("humidity")
        print(f"[MQTT Sensor] 온습도: {sensor_id} T={temp} H={humid}")
        try:
            self.db_handler.save_sensor_telemetry(
                sensor_id=sensor_id, temperature=temp, humidity=humid, ts=datetime.now()
            )
        except Exception as e:
            print(f"[MQTT Sensor] 온습도 저장 실패: {e}")

    def _handle_heart_band(self, sensor_id: str, payload: dict):
        hr = payload.get("hr")
        if hr is None:
            return
        try:
            hr = float(hr)
        except Exception:
            return
        if hr <= 0:
            return

        print(f"[MQTT Sensor] 심박: {sensor_id} HR={hr}")
        try:
            self.db_handler.save_heart_rate_telemetry(sensor_id=sensor_id, hr=hr, ts=datetime.now())
        except Exception as e:
            print(f"[MQTT Sensor] 심박 저장 실패: {e}")

        self._check_heart_rate_alert(sensor_id, hr)

    def _check_heart_rate_alert(self, sensor_id: str, hr: float):
        window = self.hr_windows[sensor_id]
        window.append(hr)
        avg_hr = sum(window) / len(window)

        if avg_hr >= 100:
            self._publish_sustained_hr_alert(sensor_id, level="red", color="red")
        elif avg_hr >= 80:
            self._publish_sustained_hr_alert(sensor_id, level="yellow", color="yellow")
        else:
            self.hr_alert_states.pop(sensor_id, None)

    def _publish_sustained_hr_alert(self, sensor_id: str, level: str, color: str):
        now = time.time()
        state = self.hr_alert_states.get(sensor_id)

        if not state or state["level"] != level:
            self.hr_alert_states[sensor_id] = {"level": level, "started_at": now}
            return

        duration = now - state["started_at"]
        if duration >= 5:
            self.publish_alert(
                sensor_id=sensor_id,
                color=color,
                vibration=True,
                led=True,
                duration_ms=3000,
                reset_after_ms=3000,
            )
            self.hr_alert_states.pop(sensor_id, None)

    # ------------------------------------------------------------------
    # Publish 명령
    # ------------------------------------------------------------------

    def publish_register(self, sensor_id: str, site_id: str, interval_ms: int = 3000):
        topic = Topics.sensor_cmd(sensor_id)
        self.client.publish(
            topic,
            json.dumps({"cmd": "register", "site_id": site_id, "interval_ms": interval_ms}),
        )
        print(f"[MQTT Sensor] register → {topic}")

    def publish_unregister(self, sensor_id: str):
        topic = Topics.sensor_cmd(sensor_id)
        self.client.publish(topic, json.dumps({"cmd": "unregister"}))
        print(f"[MQTT Sensor] unregister → {topic}")

    def publish_set_interval(self, sensor_id: str, interval_ms: int):
        topic = Topics.sensor_cmd(sensor_id)
        self.client.publish(
            topic,
            json.dumps({"cmd": "set_interval", "interval_ms": interval_ms}),
        )

    def publish_alert(
        self,
        sensor_id: str,
        color: str = "red",
        vibration: bool = True,
        led: bool = True,
        duration_ms: int = 3000,
        reset_after_ms: int = 3000,
    ):
        topic = Topics.sensor_alert(sensor_id)
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
        print(f"[MQTT Sensor] alert → {topic}")
