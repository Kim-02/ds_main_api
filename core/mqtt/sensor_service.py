"""paho-mqtt 기반 센서 서비스 — sensors/+/status, sensors/+/telemetry 구독."""
import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

import paho.mqtt.client as mqtt


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
        self.high_hr_start_times: dict = {}
        self.rest_repository = None
        self.rest_runtime_service = None
        self._init_rest_runtime()

    def _init_rest_runtime(self):
        try:
            from ai.rest import DatabaseHandlerRestDataRepository, RestRuntimeService

            self.rest_repository = DatabaseHandlerRestDataRepository(self.db_handler)
            self.rest_runtime_service = RestRuntimeService.from_model_path(
                repository=self.rest_repository,
            )
            print("[MQTT Sensor] 휴식 예측 모델 로드 완료")
        except Exception as e:
            self.rest_repository = None
            self.rest_runtime_service = None
            print(f"[MQTT Sensor] 휴식 예측 모델 비활성화: {e}")

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
            client.subscribe("sensors/+/status")
            client.subscribe("sensors/+/telemetry")
        else:
            print(f"[MQTT Sensor] 연결 실패 rc={rc}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            print(f"[MQTT Sensor] JSON 파싱 오류: {e}")
            return

        if msg.topic.endswith("/status"):
            self._handle_status(payload)
        elif msg.topic.endswith("/telemetry"):
            self._handle_telemetry(payload)

    def _handle_status(self, payload: dict):
        sensor_id = payload.get("sensor_id")
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            return
        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            print(f"[MQTT Sensor] status DB 업데이트 실패: {e}")

    def _handle_telemetry(self, payload: dict):
        sensor_id = payload.get("sensor_id")
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            return

        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            print(f"[MQTT Sensor] online 업데이트 실패: {e}")

        sensor_type = payload.get("sensor_type", "unknown")
        if sensor_type == "temp_humidity":
            self._handle_temp_humidity(sensor_id, payload)
        elif sensor_type == "heart_band":
            self._handle_heart_band(sensor_id, payload)

    def _handle_temp_humidity(self, sensor_id: str, payload: dict):
        temp = payload.get("temperature")
        humid = payload.get("humidity")
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

        try:
            self.db_handler.save_heart_rate_telemetry(
                sensor_id=sensor_id, hr=hr, ts=datetime.now()
            )
        except Exception as e:
            print(f"[MQTT Sensor] 심박 저장 실패: {e}")

        self._check_heart_rate_alert(sensor_id, hr)
        self._check_rest_recommendation(sensor_id)

    def _check_heart_rate_alert(self, sensor_id: str, hr: float):
        window = self.hr_windows[sensor_id]
        window.append(hr)
        avg_hr = sum(window) / len(window)

        if avg_hr >= 72:
            if sensor_id not in self.high_hr_start_times:
                self.high_hr_start_times[sensor_id] = time.time()
            duration = time.time() - self.high_hr_start_times[sensor_id]
            if duration >= 5:
                self.publish_alert(sensor_id=sensor_id, color="yellow", vibration=True, led=True)
                self.high_hr_start_times.pop(sensor_id, None)
        else:
            self.high_hr_start_times.pop(sensor_id, None)

    def _check_rest_recommendation(self, sensor_id: str):
        if self.rest_runtime_service is None or self.rest_repository is None:
            return

        try:
            worker_id = self.rest_repository.find_worker_id_by_sensor_id(sensor_id)
            if not worker_id:
                return

            result = self.rest_runtime_service.evaluate_worker(worker_id)
            if not result.should_rest or result.command is None:
                return

            topic, payload = result.command.to_topic_and_payload()
            self.client.publish(topic, payload)
            print(f"[MQTT Sensor] 휴식 권고 발행 sensor_id={sensor_id}")
        except Exception as e:
            print(f"[MQTT Sensor] 휴식 예측 오류: {e}")

    # ------------------------------------------------------------------
    # Publish 명령
    # ------------------------------------------------------------------

    def publish(self, topic: str, payload: str):
        """범용 MQTT 발행."""
        self.client.publish(topic, payload)

    def publish_register(self, sensor_id: str, site_id: str, interval_ms: int = 5000):
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "register", "site_id": site_id, "interval_ms": interval_ms}),
        )
        print(f"[MQTT Sensor] register 발행 sensor_id={sensor_id}")

    def publish_unregister(self, sensor_id: str):
        self.client.publish(f"sensors/{sensor_id}/cmd", json.dumps({"cmd": "unregister"}))
        print(f"[MQTT Sensor] unregister 발행 sensor_id={sensor_id}")

    def publish_set_interval(self, sensor_id: str, interval_ms: int):
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
    ):
        topic = f"sensors/{sensor_id}/alert"
        payload = json.dumps({
            "command": "alert_on",
            "color": color,
            "vibration": vibration,
            "led": led,
            "duration_ms": duration_ms,
            "reset_after_ms": reset_after_ms,
        })
        self.client.publish(topic, payload)
        print(f"[MQTT Sensor] alert 발행 sensor_id={sensor_id}")
