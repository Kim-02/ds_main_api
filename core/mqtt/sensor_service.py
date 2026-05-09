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
        print(
            "[MQTT Sensor] START __init__ "
            f"broker_host={broker_host}, broker_port={broker_port}, "
            f"db_handler={db_handler.__class__.__name__}"
        )
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
        print("[MQTT Sensor] END __init__")

    def _init_rest_runtime(self):
        print("[MQTT Sensor] START _init_rest_runtime")
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
        print(
            "[MQTT Sensor] END _init_rest_runtime "
            f"enabled={self.rest_runtime_service is not None}"
        )

    def start(self):
        print(f"[MQTT Sensor] START start running={self._running}")
        if self._running:
            print("[MQTT Sensor] END start skipped=already_running")
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
        print(f"[MQTT Sensor] END start running={self._running}")

    def stop(self):
        print(f"[MQTT Sensor] START stop running={self._running}")
        self._running = False
        try:
            self.client.disconnect()
        except Exception:
            pass
        print("[MQTT Sensor] 서비스 중단")
        print("[MQTT Sensor] END stop")

    def _on_connect(self, client, userdata, flags, rc):
        print(f"[MQTT Sensor] START _on_connect rc={rc}")
        if rc == 0:
            print("[MQTT Sensor] 브로커 연결 완료")
            client.subscribe("sensors/+/status")
            client.subscribe("sensors/+/telemetry")
        else:
            print(f"[MQTT Sensor] 연결 실패 rc={rc}")
        print("[MQTT Sensor] END _on_connect")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        print(f"[MQTT Sensor] START _on_message topic={topic}, raw_payload={msg.payload!r}")
        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            print(f"[MQTT Sensor] JSON 파싱 오류 {topic}: {e}")
            print("[MQTT Sensor] END _on_message parse_error=True")
            return
        print(f"[MQTT Sensor] parsed payload={payload}")

        if topic.endswith("/status"):
            self._handle_status(payload)
        elif topic.endswith("/telemetry"):
            self._handle_telemetry(payload)
        else:
            print(f"[MQTT Sensor] unknown topic ignored={topic}")
        print(f"[MQTT Sensor] END _on_message topic={topic}")

    def _handle_status(self, payload: dict):
        print(f"[MQTT Sensor] START _handle_status payload={payload}")
        sensor_id = payload.get("sensor_id")
        if not sensor_id:
            print("[MQTT Sensor] END _handle_status skipped=no_sensor_id")
            return
        if not self.db_handler.is_registered_sensor(sensor_id):
            print(f"[MQTT Sensor] END _handle_status skipped=unregistered sensor_id={sensor_id}")
            return
        try:
            self.db_handler.update_sensor_online(sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now())
            print(f"[MQTT Sensor] status online updated sensor_id={sensor_id}")
        except Exception as e:
            print(f"[MQTT Sensor] status DB 업데이트 실패: {e}")
        print(f"[MQTT Sensor] END _handle_status sensor_id={sensor_id}")

    def _handle_telemetry(self, payload: dict):
        print(f"[MQTT Sensor] START _handle_telemetry payload={payload}")
        sensor_id = payload.get("sensor_id")
        if not sensor_id or not self.db_handler.is_registered_sensor(sensor_id):
            print(
                "[MQTT Sensor] END _handle_telemetry skipped="
                f"sensor_id={sensor_id}, registered={bool(sensor_id and self.db_handler.is_registered_sensor(sensor_id))}"
            )
            return

        sensor_type = payload.get("sensor_type", "unknown")
        print(f"[MQTT Sensor] telemetry sensor_id={sensor_id}, sensor_type={sensor_type}")
        try:
            self.db_handler.update_sensor_online(sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now())
            print(f"[MQTT Sensor] telemetry online updated sensor_id={sensor_id}")
        except Exception as e:
            print(f"[MQTT Sensor] online 업데이트 실패: {e}")

        if sensor_type == "temp_humidity":
            self._handle_temp_humidity(sensor_id, payload)
        elif sensor_type == "heart_band":
            self._handle_heart_band(sensor_id, payload)
        else:
            print(f"[MQTT Sensor] telemetry ignored unknown sensor_type={sensor_type}")
        print(f"[MQTT Sensor] END _handle_telemetry sensor_id={sensor_id}")

    def _handle_temp_humidity(self, sensor_id: str, payload: dict):
        print(f"[MQTT Sensor] START _handle_temp_humidity sensor_id={sensor_id}, payload={payload}")
        temp = payload.get("temperature")
        humid = payload.get("humidity")
        print(f"[MQTT Sensor] 온습도: {sensor_id} T={temp} H={humid}")
        try:
            self.db_handler.save_sensor_telemetry(
                sensor_id=sensor_id, temperature=temp, humidity=humid, ts=datetime.now()
            )
            print(f"[MQTT Sensor] 온습도 저장 완료 sensor_id={sensor_id}")
        except Exception as e:
            print(f"[MQTT Sensor] 온습도 저장 실패: {e}")
        print(f"[MQTT Sensor] END _handle_temp_humidity sensor_id={sensor_id}")

    def _handle_heart_band(self, sensor_id: str, payload: dict):
        print(f"[MQTT Sensor] START _handle_heart_band sensor_id={sensor_id}, payload={payload}")
        hr = payload.get("hr")
        if hr is None:
            print("[MQTT Sensor] END _handle_heart_band skipped=no_hr")
            return
        try:
            hr = float(hr)
        except Exception:
            print(f"[MQTT Sensor] END _handle_heart_band skipped=invalid_hr value={hr}")
            return
        if hr <= 0:
            print(f"[MQTT Sensor] END _handle_heart_band skipped=non_positive_hr hr={hr}")
            return

        print(f"[MQTT Sensor] 심박: {sensor_id} HR={hr}")
        try:
            self.db_handler.save_heart_rate_telemetry(sensor_id=sensor_id, hr=hr, ts=datetime.now())
            print(f"[MQTT Sensor] 심박 저장 완료 sensor_id={sensor_id}, hr={hr}")
        except Exception as e:
            print(f"[MQTT Sensor] 심박 저장 실패: {e}")

        self._check_heart_rate_alert(sensor_id, hr)
        self._check_rest_recommendation(sensor_id)
        print(f"[MQTT Sensor] END _handle_heart_band sensor_id={sensor_id}, hr={hr}")

    def _check_heart_rate_alert(self, sensor_id: str, hr: float):
        print(f"[MQTT Sensor] START _check_heart_rate_alert sensor_id={sensor_id}, hr={hr}")
        window = self.hr_windows[sensor_id]
        window.append(hr)
        avg_hr = sum(window) / len(window)
        print(f"[MQTT Sensor] hr_window={list(window)}, avg_hr={avg_hr}")

        if avg_hr >= 72:
            if sensor_id not in self.high_hr_start_times:
                self.high_hr_start_times[sensor_id] = time.time()
            duration = time.time() - self.high_hr_start_times[sensor_id]
            if duration >= 5:
                self.publish_alert(sensor_id=sensor_id, color="yellow", vibration=True, led=True)
                self.high_hr_start_times.pop(sensor_id, None)
        else:
            self.high_hr_start_times.pop(sensor_id, None)
        print(f"[MQTT Sensor] END _check_heart_rate_alert sensor_id={sensor_id}")

    def _check_rest_recommendation(self, sensor_id: str):
        print(f"[MQTT Sensor] START _check_rest_recommendation sensor_id={sensor_id}")
        if self.rest_runtime_service is None or self.rest_repository is None:
            print("[MQTT Sensor] END _check_rest_recommendation skipped=runtime_disabled")
            return

        try:
            print("[MQTT Sensor] -> rest_repository.find_worker_id_by_sensor_id START")
            worker_id = self.rest_repository.find_worker_id_by_sensor_id(sensor_id)
            print(f"[MQTT Sensor] <- rest_repository.find_worker_id_by_sensor_id END worker_id={worker_id}")
            if not worker_id:
                print("[MQTT Sensor] END _check_rest_recommendation skipped=no_worker")
                return

            print("[MQTT Sensor] -> rest_runtime_service.evaluate_worker START")
            result = self.rest_runtime_service.evaluate_worker(worker_id)
            print(f"[MQTT Sensor] <- rest_runtime_service.evaluate_worker END prediction={result.prediction}")
            if not result.should_rest or result.command is None:
                print("[MQTT Sensor] END _check_rest_recommendation skipped=no_rest")
                return

            topic, payload = result.command.to_topic_and_payload()
            print(f"[MQTT Sensor] -> client.publish START topic={topic}, payload={payload}")
            self.client.publish(topic, payload)
            print("[MQTT Sensor] <- client.publish END")
            print(
                "[MQTT Sensor] 휴식 권고 → "
                f"worker={worker_id}, result={result.prediction.get('result')}, topic={topic}"
            )
        except Exception as e:
            print(f"[MQTT Sensor] 휴식 예측 스킵: {e}")
        print(f"[MQTT Sensor] END _check_rest_recommendation sensor_id={sensor_id}")

    # ------------------------------------------------------------------
    # Publish 명령
    # ------------------------------------------------------------------

    def publish_register(self, sensor_id: str, site_id: str, interval_ms: int = 5000):
        print(
            "[MQTT Sensor] START publish_register "
            f"sensor_id={sensor_id}, site_id={site_id}, interval_ms={interval_ms}"
        )
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "register", "site_id": site_id, "interval_ms": interval_ms}),
        )
        print(f"[MQTT Sensor] register → sensors/{sensor_id}/cmd")
        print("[MQTT Sensor] END publish_register")

    def publish_unregister(self, sensor_id: str):
        print(f"[MQTT Sensor] START publish_unregister sensor_id={sensor_id}")
        self.client.publish(f"sensors/{sensor_id}/cmd", json.dumps({"cmd": "unregister"}))
        print(f"[MQTT Sensor] unregister → sensors/{sensor_id}/cmd")
        print("[MQTT Sensor] END publish_unregister")

    def publish_set_interval(self, sensor_id: str, interval_ms: int):
        print(f"[MQTT Sensor] START publish_set_interval sensor_id={sensor_id}, interval_ms={interval_ms}")
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "set_interval", "interval_ms": interval_ms}),
        )
        print("[MQTT Sensor] END publish_set_interval")

    def publish(self, topic: str, payload: str):
        """범용 MQTT 발행 — WatchPipelineScheduler 등 외부에서 사용."""
        print(f"[MQTT Sensor] publish topic={topic} payload={payload}")
        self.client.publish(topic, payload)

    def publish_alert(
        self,
        sensor_id: str,
        color: str = "red",
        vibration: bool = True,
        led: bool = True,
        duration_ms: int = 5000,
        reset_after_ms: int = 10000,
    ):
        print(
            "[MQTT Sensor] START publish_alert "
            f"sensor_id={sensor_id}, color={color}, vibration={vibration}, "
            f"led={led}, duration_ms={duration_ms}, reset_after_ms={reset_after_ms}"
        )
        topic = f"sensors/{sensor_id}/alert"
        payload = json.dumps({
                "command": "alert_on",
                "color": color,
                "vibration": vibration,
                "led": led,
                "duration_ms": duration_ms,
                "reset_after_ms": reset_after_ms,
            })
        print(f"[MQTT Sensor] -> client.publish START topic={topic}, payload={payload}")
        self.client.publish(topic, payload)
        print("[MQTT Sensor] <- client.publish END")
        print(f"[MQTT Sensor] alert → {topic}")
        print("[MQTT Sensor] END publish_alert")
