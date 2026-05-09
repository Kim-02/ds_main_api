"""paho-mqtt 기반 센서 서비스.

구독 토픽:
  기존) sensors/+/status      ← 센서 온라인 상태
        sensors/+/telemetry   ← 구형 payload (sensor_type 포함)
  신규) sensor/+/+            ← sensor/{sensor_id}/{data_type}
        sensors/+/data        ← sensors/{sensor_id}/data

처리 흐름 (신규 토픽):
  1. topic 2번째 세그먼트에서 sensor_id 추출
  2. payload.sensor_id 로 보완
  3. sensor 테이블에서 sen_id / sensor_type 조회
  4. 데이터 종류 판별 (th / hb)
  5. th_trans 또는 hb_trans에 INSERT ... ON DUPLICATE KEY UPDATE
  6. sensor.last_seen_at 갱신
"""
import json
import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

# 온습도 센서 타입 식별자
_TH_TYPES = {"temp_humidity", "th", "temperature", "temperature_humidity"}
# 심박 센서 타입 식별자
_HB_TYPES = {"heart_band", "heartbeat", "heart_rate", "hb", "watch"}


class MqttSensorService:
    def __init__(self, db_handler, broker_host: str = "127.0.0.1", broker_port: int = 1883):
        self.db_handler = db_handler
        self.broker_host = broker_host
        self.broker_port = broker_port

        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        self._thread: Optional[threading.Thread] = None
        self._running = False

        self.hr_windows: dict = defaultdict(lambda: deque(maxlen=10))
        self.high_hr_start_times: dict = {}

    # ── 생명주기 ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self._thread = threading.Thread(target=self.client.loop_forever, daemon=True)
            self._thread.start()
            self._running = True
            logger.info("[MQTT Sensor] 서비스 시작 | broker=%s:%s", self.broker_host, self.broker_port)
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

    # ── MQTT 콜백 ─────────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc != 0:
            logger.error("[MQTT Sensor] 브로커 연결 실패 rc=%d", rc)
            return

        logger.info("[MQTT Sensor] 브로커 연결 완료")

        # 기존 토픽 (sensors/ 복수형)
        client.subscribe("sensors/+/status")
        client.subscribe("sensors/+/telemetry")

        # 신규 토픽
        # sensor/{sensor_id}/{data_type}  — 단수형
        client.subscribe("sensor/+/+")
        # sensors/{sensor_id}/data        — 복수형 data 엔드포인트
        client.subscribe("sensors/+/data")

        logger.info(
            "[MQTT Sensor] 구독 완료: sensors/+/status, sensors/+/telemetry, sensor/+/+, sensors/+/data"
        )

    def _on_message(self, client, userdata, msg) -> None:
        topic: str = msg.topic

        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            logger.warning("[MQTT Sensor] JSON 파싱 오류 | topic=%s | error=%s", topic, e)
            return

        if not isinstance(payload, dict):
            logger.warning("[MQTT Sensor] payload가 dict가 아님 | topic=%s", topic)
            return

        # 라우팅: 기존 / 신규 분리
        if topic.endswith("/status"):
            self._handle_status(payload)
        elif topic.endswith("/telemetry"):
            self._handle_telemetry(topic, payload)
        else:
            # sensor/{id}/{data_type}, sensors/{id}/data 등
            self._handle_sensor_data_topic(topic, payload)

    # ── 기존 핸들러 (sensors/ 복수형 토픽) ───────────────────────────────────

    def _handle_status(self, payload: dict) -> None:
        """sensors/{id}/status — 센서 온라인 상태 갱신."""
        sensor_id = payload.get("sensor_id")
        if not sensor_id:
            return
        if not self.db_handler.is_registered_sensor(sensor_id):
            return
        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            logger.warning("[MQTT Sensor] status DB 업데이트 실패 | sensor_id=%s | error=%s", sensor_id, e)

    def _handle_telemetry(self, topic: str, payload: dict) -> None:
        """sensors/{id}/telemetry — 구형 payload (payload에 sensor_type 포함)."""
        sensor_id = payload.get("sensor_id")
        if not sensor_id:
            # topic에서 추출 시도
            sensor_id = self._extract_sensor_id_from_topic(topic)
        if not sensor_id:
            return
        if not self.db_handler.is_registered_sensor(sensor_id):
            return

        try:
            self.db_handler.update_sensor_online(
                sensor_id=sensor_id, is_online=True, last_seen_at=datetime.now()
            )
        except Exception as e:
            logger.warning("[MQTT Sensor] online 업데이트 실패 | sensor_id=%s | error=%s", sensor_id, e)

        # sensor_type: payload 우선, 없으면 DB 조회
        sensor_type = str(payload.get("sensor_type") or "")
        if not sensor_type:
            row = self.db_handler.get_sensor_by_sensor_id(sensor_id)
            sensor_type = str(row.get("sensor_type") or "") if row else ""

        data_type = self._classify_data_type(sensor_type, payload)

        if data_type == "th":
            self._store_th_by_sensor_id(topic, sensor_id, payload)
        elif data_type == "hb":
            self._store_hb_by_sensor_id(topic, sensor_id, payload)

    # ── 신규 핸들러 (sensor/+/+, sensors/+/data) ─────────────────────────────

    def _handle_sensor_data_topic(self, topic: str, payload: dict) -> None:
        """sensor/{sensor_id}/{data_type} 또는 sensors/{sensor_id}/data 처리.

        sensor_id 결정 우선순위:
          1. topic 두 번째 세그먼트
          2. payload["sensor_id"]
        """
        sensor_id = self._extract_sensor_id_from_topic(topic)
        if not sensor_id:
            sensor_id = payload.get("sensor_id")
        if not sensor_id:
            logger.warning("[MQTT Sensor] sensor_id 추출 불가 | topic=%s", topic)
            return

        # DB에서 센서 행 조회
        sensor_row = self.db_handler.get_sensor_by_sensor_id(sensor_id)
        if sensor_row is None:
            # sensor_id로 못 찾으면 mqtt_topic 컬럼으로 재시도
            sensor_row = self.db_handler.get_sensor_by_mqtt_topic(topic)

        if sensor_row is None:
            logger.warning(
                "[MQTT Sensor] 미등록 센서 — 저장 건너뜀 | topic=%s | sensor_id=%s",
                topic,
                sensor_id,
            )
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
            logger.warning(
                "[MQTT Sensor] 데이터 타입 판별 불가 — 저장 건너뜀"
                " | topic=%s | sensor_id=%s | sensor_type=%s",
                topic,
                sensor_id,
                sensor_type,
            )
            return

        # last_seen_at 갱신 (sen_id 기준)
        try:
            self.db_handler.update_sensor_last_seen_by_id(sen_id)
        except Exception as e:
            logger.warning("[MQTT Sensor] last_seen 갱신 실패 | sen_id=%s | error=%s", sen_id, e)

    # ── 저장 헬퍼 ─────────────────────────────────────────────────────────────

    def _save_th(self, topic: str, sensor_id: str, sen_id: int, ts, payload: dict) -> None:
        """온습도 값을 th_trans에 저장 (ON DUPLICATE KEY UPDATE)."""
        # temp / temperature 둘 다 허용
        temp = payload.get("temp") if payload.get("temp") is not None else payload.get("temperature")
        # humid / humidity 둘 다 허용
        humid = payload.get("humid") if payload.get("humid") is not None else payload.get("humidity")

        if temp is None and humid is None:
            logger.warning(
                "[MQTT Sensor] 온습도 값 없음 | topic=%s | sensor_id=%s | payload=%s",
                topic, sensor_id, payload,
            )
            return

        ok = self.db_handler.insert_th_trans(sen_id, ts, temp, humid)
        if ok:
            logger.info(
                "[MQTT Sensor] th_trans 저장 완료 | sensor_id=%s | sen_id=%s | temp=%s | humid=%s",
                sensor_id, sen_id, temp, humid,
            )

    def _save_hb(self, topic: str, sensor_id: str, sen_id: int, ts, payload: dict) -> None:
        """심박 값을 hb_trans에 저장 (ON DUPLICATE KEY UPDATE)."""
        # hr / heart_rate 둘 다 허용
        hr = payload.get("hr") if payload.get("hr") is not None else payload.get("heart_rate")
        if hr is None:
            logger.warning(
                "[MQTT Sensor] hr 값 없음 | topic=%s | sensor_id=%s | payload=%s",
                topic, sensor_id, payload,
            )
            return

        try:
            hr = float(hr)
        except Exception:
            logger.warning("[MQTT Sensor] hr float 변환 실패 | topic=%s | hr=%s", topic, hr)
            return

        if hr <= 0:
            return

        ok = self.db_handler.insert_hb_trans(sen_id, ts, hr)
        if ok:
            logger.info(
                "[MQTT Sensor] hb_trans 저장 완료 | sensor_id=%s | sen_id=%s | hr=%s",
                sensor_id, sen_id, hr,
            )

        self._check_heart_rate_alert(sensor_id, hr)

    def _store_th_by_sensor_id(self, topic: str, sensor_id: str, payload: dict) -> None:
        """기존 telemetry 핸들러용 — sensor_id 문자열로 온습도 저장."""
        temp = payload.get("temp") if payload.get("temp") is not None else payload.get("temperature")
        humid = payload.get("humid") if payload.get("humid") is not None else payload.get("humidity")
        logger.info("[MQTT Sensor] 온습도(telemetry) | sensor_id=%s T=%s H=%s", sensor_id, temp, humid)
        try:
            self.db_handler.save_sensor_telemetry(
                sensor_id=sensor_id,
                temperature=temp,
                humidity=humid,
                ts=datetime.now(),
            )
        except Exception as e:
            logger.error("[MQTT Sensor] 온습도 저장 실패 | sensor_id=%s | error=%s", sensor_id, e)

    def _store_hb_by_sensor_id(self, topic: str, sensor_id: str, payload: dict) -> None:
        """기존 telemetry 핸들러용 — sensor_id 문자열로 심박 저장."""
        hr = payload.get("hr") if payload.get("hr") is not None else payload.get("heart_rate")
        if hr is None:
            return
        try:
            hr = float(hr)
        except Exception:
            return
        if hr <= 0:
            return

        logger.info("[MQTT Sensor] 심박(telemetry) | sensor_id=%s HR=%s", sensor_id, hr)
        try:
            self.db_handler.save_heart_rate_telemetry(
                sensor_id=sensor_id, hr=hr, ts=datetime.now()
            )
        except Exception as e:
            logger.error("[MQTT Sensor] 심박 저장 실패 | sensor_id=%s | error=%s", sensor_id, e)

        self._check_heart_rate_alert(sensor_id, hr)

    # ── 유틸 ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_sensor_id_from_topic(topic: str) -> Optional[str]:
        """topic 경로의 두 번째 세그먼트를 sensor_id로 반환.

        예:
          sensor/watch-1386/heart_rate  → "watch-1386"
          sensors/th-001/data           → "th-001"
          sensor/th-001/th              → "th-001"
        """
        parts = topic.split("/")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
        return None

    @staticmethod
    def _classify_data_type(sensor_type: str, payload: dict) -> str:
        """센서 타입 문자열 또는 payload 키로 데이터 종류 판별.

        반환값: "th" | "hb" | "unknown"

        1순위: sensor_type 문자열
        2순위: payload 키 (sensor_type 판별 불가 시 fallback)
        """
        st = sensor_type.lower().strip()

        if st in _TH_TYPES:
            return "th"
        if st in _HB_TYPES:
            return "hb"

        # payload 키로 추론 (sensor_type 없거나 알 수 없는 값일 때)
        if any(k in payload for k in ("temp", "temperature", "humid", "humidity")):
            return "th"
        if any(k in payload for k in ("hr", "heart_rate")):
            return "hb"

        return "unknown"

    def _check_heart_rate_alert(self, sensor_id: str, hr: float) -> None:
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

    # ── Publish 명령 ──────────────────────────────────────────────────────────

    def publish_register(self, sensor_id: str, site_id: str, interval_ms: int = 5000) -> None:
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "register", "site_id": site_id, "interval_ms": interval_ms}),
        )
        logger.info("[MQTT Sensor] register → sensors/%s/cmd", sensor_id)

    def publish_unregister(self, sensor_id: str) -> None:
        self.client.publish(
            f"sensors/{sensor_id}/cmd",
            json.dumps({"cmd": "unregister"}),
        )
        logger.info("[MQTT Sensor] unregister → sensors/%s/cmd", sensor_id)

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
        logger.info("[MQTT Sensor] alert → %s", topic)
