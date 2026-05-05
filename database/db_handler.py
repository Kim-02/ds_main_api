"""기존 MariaDB 핸들러 — pymysql 기반 동기 DB 접근."""
import logging
from datetime import datetime

import pymysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class DatabaseHandler:
    def __init__(self, host="127.0.0.1", user="myuser", password="mypassword", db_name="mydb", port=3306):
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": db_name,
            "port": port,
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": True,
        }

    def _get_connection(self):
        return pymysql.connect(**self.db_config)

    def _parse_to_mysql_time(self, time_val):
        try:
            if isinstance(time_val, datetime):
                dt = time_val
            elif isinstance(time_val, (int, float)):
                dt = datetime.fromtimestamp(time_val)
            elif isinstance(time_val, str):
                dt = datetime.fromisoformat(time_val)
            else:
                dt = datetime.now()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # Jetson 관리
    # ------------------------------------------------------------------

    def init_jetson_info(self, jetson_data: dict) -> bool:
        check_query = "SELECT jetson_id FROM jetson LIMIT 1"
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(check_query)
                    existing = cursor.fetchone()
                    if not existing:
                        cursor.execute(
                            "INSERT INTO jetson (jetson_wp, jetson_loc, jetson_status, ip_addr, port) VALUES (%s,%s,%s,%s,%s)",
                            (
                                jetson_data.get("jetson_wp"),
                                jetson_data.get("jetson_loc"),
                                jetson_data.get("jetson_status", True),
                                jetson_data.get("ip_addr"),
                                jetson_data.get("port"),
                            ),
                        )
                    else:
                        cursor.execute(
                            "UPDATE jetson SET ip_addr=%s, port=%s WHERE jetson_id=%s",
                            (jetson_data.get("ip_addr"), jetson_data.get("port"), existing["jetson_id"]),
                        )
            return True
        except Exception as e:
            logging.error("init_jetson_info 오류: %s", e)
            return False

    def register_jetson_connection(self, dept_id: int, app_id: str):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM jetson LIMIT 1")
                    jetson = cursor.fetchone()
                    if not jetson:
                        return None
                    cursor.execute(
                        "INSERT INTO connect (dept_id, jetson_id, app_id) VALUES (%s,%s,%s)",
                        (dept_id, jetson["jetson_id"], app_id),
                    )
            return jetson
        except Exception as e:
            logging.error("register_jetson_connection 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # 센서 등록 / 해제 (mDNS → DB)
    # ------------------------------------------------------------------

    def register_discovered_sensors(self, jetson_id: int, sensors: list) -> bool:
        query = """
            INSERT INTO sensor (
                sensor_id, jetson_id, sensor_type, sen_name, sen_locate, model,
                mqtt_topic, mdns_hostname, ip_addr, is_online, last_seen_at,
                registered_at, register_date, created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CURDATE(), NOW(), NOW())
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    for s in sensors:
                        d = s.model_dump() if hasattr(s, "model_dump") else s
                        cursor.execute(
                            query,
                            (
                                d.get("sensor_id"),
                                jetson_id,
                                d.get("sensor_type"),
                                d.get("sen_name") or d.get("sensor_name"),
                                d.get("sen_locate") or d.get("sensor_location"),
                                d.get("model"),
                                d.get("mqtt_topic") or d.get("topic_base"),
                                d.get("mdns_hostname"),
                                d.get("ip_addr"),
                                True,
                                self._parse_to_mysql_time(d.get("last_seen_at")),
                                self._parse_to_mysql_time(datetime.now()),
                            ),
                        )
            return True
        except Exception as e:
            logging.exception("register_discovered_sensors 오류: %s", e)
            return False

    def unregister_sensor_by_sensor_id(self, sensor_id: str) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute("DELETE FROM sensor WHERE sensor_id=%s", (sensor_id,))
            return affected > 0
        except Exception as e:
            logging.error("unregister_sensor_by_sensor_id 오류: %s", e)
            return False

    def is_registered_sensor(self, sensor_id: str) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM sensor WHERE sensor_id=%s LIMIT 1", (sensor_id,))
                    return cursor.fetchone() is not None
        except Exception as e:
            logging.error("is_registered_sensor 오류: %s", e)
            return False

    def get_registered_sensor_rows(self) -> list:
        query = """
            SELECT sen_id, sensor_id, sensor_type, sen_name, sen_locate, model,
                   mqtt_topic, mdns_hostname, ip_addr, is_online, last_seen_at,
                   registered_at, register_date, created_at, updated_at
            FROM sensor ORDER BY updated_at DESC
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_registered_sensor_rows 오류: %s", e)
            return []

    def update_sensor_online(self, sensor_id: str, is_online: bool, last_seen_at=None) -> bool:
        try:
            mysql_time = self._parse_to_mysql_time(last_seen_at)
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute(
                        "UPDATE sensor SET is_online=%s, last_seen_at=%s, updated_at=NOW() WHERE sensor_id=%s",
                        (is_online, mysql_time, sensor_id),
                    )
            return affected > 0
        except Exception as e:
            logging.error("update_sensor_online 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 센서 원격 측정 저장
    # ------------------------------------------------------------------

    def save_sensor_telemetry(self, sensor_id: str, temperature: float, humidity: float, ts=None) -> bool:
        query = """
            INSERT INTO th_trans (sen_id, temp, humid, time)
            VALUES ((SELECT sen_id FROM sensor WHERE sensor_id=%s LIMIT 1), %s, %s, %s)
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (sensor_id, temperature, humidity, self._parse_to_mysql_time(ts)))
            return True
        except Exception as e:
            logging.error("save_sensor_telemetry 오류: %s", e)
            return False

    def save_heart_rate_telemetry(self, sensor_id: str, hr: float, ts=None) -> bool:
        query = """
            INSERT INTO hb_trans (sen_id, hr, time)
            VALUES ((SELECT sen_id FROM sensor WHERE sensor_id=%s LIMIT 1), %s, %s)
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (sensor_id, hr, self._parse_to_mysql_time(ts)))
            return True
        except Exception as e:
            logging.error("save_heart_rate_telemetry 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 카메라
    # ------------------------------------------------------------------

    def register_camera_info(self, ip_address: str, camera_id: str, camera_pw: str):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM camera_info WHERE ip_address=%s LIMIT 1", (ip_address,))
                    if cursor.fetchone():
                        return None

                    cursor.execute("SELECT jetson_id, jetson_loc FROM jetson LIMIT 1")
                    jetson = cursor.fetchone()
                    if not jetson:
                        return False

                    conn.begin()
                    auto_name = f"CAM_{ip_address.split('.')[-1]}"
                    reg_date = datetime.now().strftime("%Y-%m-%d")
                    cursor.execute(
                        "INSERT INTO sensor (jetson_id, sensor_type, sen_name, sen_locate, register_date) VALUES (%s,%s,%s,%s,%s)",
                        (jetson["jetson_id"], "camera", auto_name, jetson["jetson_loc"], reg_date),
                    )
                    new_sen_id = cursor.lastrowid
                    cursor.execute(
                        "INSERT INTO camera_info (sen_id, ip_address, camera_id, camera_pw) VALUES (%s,%s,%s,%s)",
                        (new_sen_id, ip_address, camera_id, camera_pw),
                    )
                conn.commit()
            return {"ip_address": ip_address, "camera_id": camera_id, "camera_pw": camera_pw}
        except Exception as e:
            logging.error("register_camera_info 오류: %s", e)
            return False

    def get_cctv_list(self) -> list:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT c.ip_address, s.sen_name, s.sen_locate FROM camera_info c JOIN sensor s ON c.sen_id=s.sen_id"
                    )
                    return cursor.fetchall()
        except Exception:
            return []

    # ------------------------------------------------------------------
    # 이벤트 / 리포트
    # ------------------------------------------------------------------

    def save_event_log(self, db_payload: dict) -> dict:
        query = """
            INSERT INTO event (ev_code_id, sen_id, message, detected_value, time)
            VALUES (%s, (SELECT sen_id FROM sensor WHERE sen_name=%s LIMIT 1), %s, %s, %s)
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        query,
                        (
                            db_payload["ev_code_id"],
                            db_payload["sen_name"],
                            db_payload["message"],
                            db_payload["detected_value"],
                            db_payload["time"],
                        ),
                    )
                    return {"event_id": cursor.lastrowid}
        except Exception as e:
            logging.error("save_event_log 오류: %s", e)
            return {"event_id": int(datetime.now().timestamp() * 1000)}

    def process_ai_event(self, req_payload: dict) -> dict:
        ip_address = req_payload["ip_address"]
        ev_code_name = req_payload.get("ev_code_name", "")
        mysql_time = self._parse_to_mysql_time(req_payload.get("time"))
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT c.sen_id, s.sen_name, s.sen_locate,
                               (SELECT ev_code_id FROM event_code WHERE ev_code_name=%s LIMIT 1) AS ev_code_id
                        FROM camera_info c JOIN sensor s ON c.sen_id=s.sen_id
                        WHERE c.ip_address=%s LIMIT 1
                        """,
                        (ev_code_name, ip_address),
                    )
                    info = cursor.fetchone()
                    sen_id = info["sen_id"] if info else None
                    camera_name = info["sen_name"] if info else "unknown_camera"
                    camera_loc = info["sen_locate"] if info else "알 수 없음"
                    ev_code_id = (info["ev_code_id"] if info and info["ev_code_id"] else 0)

                    cursor.execute(
                        "INSERT INTO event (ev_code_id, sen_id, message, detected_value, time) VALUES (%s,%s,%s,'AI_VISION_DETECTION',%s)",
                        (ev_code_id, sen_id, req_payload.get("message"), mysql_time),
                    )
                    return {"event_id": cursor.lastrowid, "camera_name": camera_name, "camera_loc": camera_loc}
        except Exception as e:
            logging.error("process_ai_event 오류: %s", e)
            return {"event_id": 0, "camera_name": "unknown_camera", "camera_loc": "알 수 없음"}

    def update_event_measures(self, event_id: int, measures: str) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute("UPDATE event SET measures=%s WHERE event_id=%s", (measures, event_id))
            return affected > 0
        except Exception as e:
            logging.error("update_event_measures 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 작업자
    # ------------------------------------------------------------------

    def get_worker_name_by_id(self, worker_id: str):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT name FROM worker WHERE dept_id=%s", (worker_id,))
                    result = cursor.fetchone()
            return result["name"] if result else None
        except Exception as e:
            logging.error("get_worker_name_by_id 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # 플로어 맵 / 센서 위치
    # ------------------------------------------------------------------

    def get_floor_map_by_jetson_id(self, jetson_id: int):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT map_id, jetson_id, map_name, image_base64, image_mime_type, image_width, image_height
                           FROM floor_map WHERE jetson_id=%s LIMIT 1""",
                        (jetson_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_floor_map_by_jetson_id 오류: %s", e)
            return None

    def get_sensor_positions_by_map_id(self, map_id: int) -> list:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT p.position_id, p.map_id, p.sensor_id, p.x_ratio, p.y_ratio,
                                  s.sen_name, s.sensor_type, s.sen_locate, s.model, s.is_online
                           FROM sensor_map_position p JOIN sensor s ON p.sensor_id=s.sensor_id
                           WHERE p.map_id=%s ORDER BY s.sen_name""",
                        (map_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_sensor_positions_by_map_id 오류: %s", e)
            return []

    def get_registered_sensors_by_jetson_id(self, jetson_id: int) -> list:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT sensor_id, sensor_type, sen_name, sen_locate, model, mqtt_topic, is_online FROM sensor WHERE jetson_id=%s ORDER BY sen_name",
                        (jetson_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_registered_sensors_by_jetson_id 오류: %s", e)
            return []

    def upsert_sensor_position(self, map_id: int, sensor_id: str, x_ratio: float, y_ratio: float) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO sensor_map_position (map_id, sensor_id, x_ratio, y_ratio)
                           VALUES (%s,%s,%s,%s)
                           ON DUPLICATE KEY UPDATE x_ratio=VALUES(x_ratio), y_ratio=VALUES(y_ratio), updated_at=NOW()""",
                        (map_id, sensor_id, x_ratio, y_ratio),
                    )
            return True
        except Exception as e:
            logging.error("upsert_sensor_position 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 웹 대시보드용 최신 데이터
    # ------------------------------------------------------------------

    def get_web_sensor_th(self) -> list:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM th_trans ORDER BY time DESC LIMIT 1")
                    return cursor.fetchall()
        except Exception:
            return []

    def get_web_sensor_hb(self) -> list:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM hb_trans ORDER BY time DESC LIMIT 1")
                    return cursor.fetchall()
        except Exception:
            return []
