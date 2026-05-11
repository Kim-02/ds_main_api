"""MariaDB 핸들러 — pymysql 기반 동기 DB 접근.

현재 실제 DDL 기준 테이블:
- jetson
- sensor
- camera_info
- worker
- connect
- event
- event_code
- hb_trans
- th_trans
- manage
"""

import logging
from datetime import datetime
from typing import Any, Optional

import pymysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class DatabaseHandler:
    def __init__(
        self,
        host: str = "127.0.0.1",
        user: str = "root",
        password: str = "ekthf123",
        db_name: str = "ON_SAFE",
        port: int = 3306,
    ):
        self.db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": db_name,
            "port": port,
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": False,
        }

    def _get_connection(self):
        return pymysql.connect(**self.db_config)

    def _parse_to_mysql_time(self, time_val: Any = None) -> str:
        try:
            if isinstance(time_val, datetime):
                dt = time_val
            elif isinstance(time_val, (int, float)):
                dt = datetime.fromtimestamp(time_val)
            elif isinstance(time_val, str):
                # "2026-05-06 23:10:00" 형태도 처리
                normalized = time_val.replace("Z", "").replace("T", " ")
                dt = datetime.fromisoformat(normalized)
            else:
                dt = datetime.now()

            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # Jetson 관리
    # ------------------------------------------------------------------

    def init_jetson_info(self, jetson_data: dict) -> bool:
        """
        Jetson이 DB에 없으면 1개 생성하고,
        이미 있으면 IP/PORT/status를 갱신한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT jetson_id FROM jetson LIMIT 1")
                    existing = cursor.fetchone()

                    if not existing:
                        cursor.execute(
                            """
                            INSERT INTO jetson (
                                jetson_wp,
                                jetson_loc,
                                jetson_status,
                                ip_addr,
                                port
                            ) VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                jetson_data.get("jetson_wp", "default_workplace"),
                                jetson_data.get("jetson_loc", "default_location"),
                                1 if jetson_data.get("jetson_status", True) else 0,
                                jetson_data.get("ip_addr"),
                                jetson_data.get("port"),
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE jetson
                            SET
                                ip_addr = %s,
                                port = %s,
                                jetson_status = %s
                            WHERE jetson_id = %s
                            """,
                            (
                                jetson_data.get("ip_addr"),
                                jetson_data.get("port"),
                                1 if jetson_data.get("jetson_status", True) else 0,
                                existing["jetson_id"],
                            ),
                        )

                conn.commit()
            return True

        except Exception as e:
            logging.error("init_jetson_info 오류: %s", e)
            return False

    def get_first_jetson(self) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM jetson ORDER BY jetson_id LIMIT 1")
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_first_jetson 오류: %s", e)
            return None

    def register_jetson_connection(self, dept_id: int, app_id: str):
        """
        관리자 앱 접속 기록.
        dept_id는 사번이며 worker.dept_id를 참조한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM jetson ORDER BY jetson_id LIMIT 1")
                   
                    jetson = cursor.fetchone()
                    print(jetson)
                    if not jetson:
                        return None

                    cursor.execute(
                        """
                        INSERT INTO connect (
                            dept_id,
                            jetson_id,
                            app_id,
                            connected_at,
                            last_seen_at,
                            is_active
                        ) VALUES (%s, %s, %s, NOW(), NOW(), 1)
                        """,
                        (dept_id, jetson["jetson_id"], app_id),
                    )

                conn.commit()
            return jetson

        except Exception as e:
            logging.error("register_jetson_connection 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # 센서 조회 / 등록 / 해제
    # ------------------------------------------------------------------

    def is_registered_sensor(self, sensor_id: str) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM sensor
                        WHERE sensor_id = %s
                        LIMIT 1
                        """,
                        (sensor_id,),
                    )
                    return cursor.fetchone() is not None

        except Exception as e:
            logging.error("is_registered_sensor 오류: %s", e)
            return False

    def get_sensor_by_sensor_id(self, sensor_id: str) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT *
                        FROM sensor
                        WHERE sensor_id = %s
                        LIMIT 1
                        """,
                        (sensor_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_sensor_by_sensor_id 오류: %s", e)
            return None

    def get_registered_sensor_rows(self) -> list[dict]:
        """
        register_date 제거 버전.
        """
        query = """
            SELECT
                sen_id,
                sensor_id,
                jetson_id,
                sensor_type,
                sen_name,
                sen_locate,
                model,
                mqtt_topic,
                mdns_hostname,
                ip_addr,
                space_id,
                is_online,
                last_seen_at,
                registered_at,
                created_at,
                updated_at
            FROM sensor
            ORDER BY updated_at DESC
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_registered_sensor_rows 오류: %s", e)
            return []

    def get_sensor_space_by_sensor_id(self, sensor_id: str) -> Optional[dict]:
        """sensor_id 기준 센서의 space_id를 조회한다."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            s.sen_id,
                            s.sensor_id,
                            s.sensor_type,
                            s.sen_name,
                            s.space_id,
                            sp.space_name
                        FROM sensor s
                        LEFT JOIN ds_space sp
                          ON s.space_id = sp.space_id
                        WHERE s.sensor_id = %s
                        LIMIT 1
                        """,
                        (sensor_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_sensor_space_by_sensor_id 오류: %s", e)
            return None

    def get_cameras_by_space_id(self, space_id: int) -> list[dict]:
        """space_id 기준 등록된 카메라 목록을 반환한다.

        camera_info.space_id가 있으면 우선 사용하고, 없으면 sensor.space_id를 사용한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            s.sen_id,
                            s.sensor_id,
                            s.sen_name,
                            s.sen_locate,
                            COALESCE(c.space_id, s.space_id) AS space_id,
                            sp.space_name,
                            s.is_online,
                            c.ip_address,
                            c.camera_id,
                            c.camera_pw,
                            c.health
                        FROM camera_info c
                        JOIN sensor s
                          ON c.sen_id = s.sen_id
                        LEFT JOIN ds_space sp
                          ON COALESCE(c.space_id, s.space_id) = sp.space_id
                        WHERE COALESCE(c.space_id, s.space_id) = %s
                          AND s.sensor_type IN ('camera', 'cctv')
                        ORDER BY s.sen_name
                        """,
                        (space_id,),
                    )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_cameras_by_space_id 오류: %s", e)
            return []

    def register_discovered_sensors(self, jetson_id: int, sensors: list) -> bool:
        """
        mDNS로 발견된 센서를 DB에 등록한다.
        기존 register_date 컬럼은 사용하지 않는다.

        주의:
        - 워치-근로자 매핑은 이 함수가 아니라 register_sensor_with_worker()에서 처리한다.
        - 이 함수는 센서만 등록할 때 사용한다.
        """
        query = """
            INSERT INTO sensor (
                sensor_id,
                jetson_id,
                sensor_type,
                sen_name,
                sen_locate,
                model,
                mqtt_topic,
                mdns_hostname,
                ip_addr,
                is_online,
                last_seen_at,
                registered_at,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, NOW(), NOW(), NOW()
            )
            ON DUPLICATE KEY UPDATE
                jetson_id = VALUES(jetson_id),
                sensor_type = VALUES(sensor_type),
                sen_name = VALUES(sen_name),
                sen_locate = VALUES(sen_locate),
                model = VALUES(model),
                mqtt_topic = VALUES(mqtt_topic),
                mdns_hostname = VALUES(mdns_hostname),
                ip_addr = VALUES(ip_addr),
                is_online = VALUES(is_online),
                last_seen_at = VALUES(last_seen_at),
                updated_at = NOW()
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    for s in sensors:
                        d = s.model_dump() if hasattr(s, "model_dump") else s

                        sensor_id = d.get("sensor_id")
                        if not sensor_id:
                            continue

                        mqtt_topic = (
                            d.get("mqtt_topic")
                            or d.get("telemetry_topic")
                            or (
                                f"{d.get('mqtt_base')}/telemetry"
                                if d.get("mqtt_base")
                                else None
                            )
                        )

                        cursor.execute(
                            query,
                            (
                                sensor_id,
                                jetson_id,
                                d.get("sensor_type", "unknown"),
                                d.get("sen_name") or d.get("sensor_name") or sensor_id,
                                d.get("sen_locate") or d.get("sensor_location") or "default",
                                d.get("model"),
                                mqtt_topic,
                                d.get("mdns_hostname"),
                                d.get("ip_addr"),
                                1 if d.get("is_online", True) else 0,
                                self._parse_to_mysql_time(d.get("last_seen_at")),
                            ),
                        )

                conn.commit()
            return True

        except Exception as e:
            logging.exception("register_discovered_sensors 오류: %s", e)
            return False

    def register_sensor_with_worker(
        self,
        sensor_info: dict,
        jetson_id: int,
        dept_id: int,
    ) -> dict:
        """
        mDNS로 발견된 heart_band 센서를 sensor 테이블에 등록하고,
        worker.sen_id에 1대1 매핑한다.

        dept_id는 사번이다.
        is_manager=0인 작업자에게만 워치 매핑을 허용한다.
        """
        sensor_id = sensor_info.get("sensor_id")
        if not sensor_id:
            raise ValueError("sensor_id가 없습니다.")

        sensor_type = sensor_info.get("sensor_type", "unknown")
        if sensor_type != "heart_band":
            raise ValueError("근로자 매핑은 heart_band 센서에만 허용됩니다.")

        mqtt_topic = (
            sensor_info.get("mqtt_topic")
            or sensor_info.get("telemetry_topic")
            or (
                f"{sensor_info.get('mqtt_base')}/telemetry"
                if sensor_info.get("mqtt_base")
                else None
            )
        )

        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        # 1. 작업자 확인
                        cursor.execute(
                            """
                            SELECT
                                dept_id,
                                name,
                                is_manager,
                                sen_id
                            FROM worker
                            WHERE dept_id = %s
                            FOR UPDATE
                            """,
                            (dept_id,),
                        )
                        worker = cursor.fetchone()

                        if not worker:
                            raise ValueError("존재하지 않는 사번입니다.")

                        if int(worker["is_manager"]) == 1:
                            raise ValueError("관리자 계정에는 심박 밴드를 매핑할 수 없습니다.")

                        if worker["sen_id"] is not None:
                            raise ValueError("이미 센서가 매핑된 작업자입니다.")

                        # 2. 센서 등록 여부 확인
                        cursor.execute(
                            """
                            SELECT sen_id
                            FROM sensor
                            WHERE sensor_id = %s
                            FOR UPDATE
                            """,
                            (sensor_id,),
                        )
                        sensor = cursor.fetchone()

                        if sensor:
                            sen_id = sensor["sen_id"]

                            # 이미 다른 작업자에게 매핑되어 있는지 확인
                            cursor.execute(
                                """
                                SELECT dept_id, name
                                FROM worker
                                WHERE sen_id = %s
                                  AND dept_id <> %s
                                LIMIT 1
                                """,
                                (sen_id, dept_id),
                            )
                            owner = cursor.fetchone()

                            if owner:
                                raise ValueError(
                                    f"이미 다른 작업자에게 매핑된 센서입니다. "
                                    f"dept_id={owner['dept_id']}, name={owner['name']}"
                                )

                            cursor.execute(
                                """
                                UPDATE sensor
                                SET
                                    jetson_id = %s,
                                    sensor_type = %s,
                                    sen_name = %s,
                                    sen_locate = %s,
                                    model = %s,
                                    mqtt_topic = %s,
                                    mdns_hostname = %s,
                                    ip_addr = %s,
                                    is_online = %s,
                                    last_seen_at = %s,
                                    registered_at = COALESCE(registered_at, NOW()),
                                    updated_at = NOW()
                                WHERE sen_id = %s
                                """,
                                (
                                    jetson_id,
                                    sensor_type,
                                    sensor_info.get("sen_name", sensor_id),
                                    sensor_info.get("sen_locate", "worker_wrist"),
                                    sensor_info.get("model", "Galaxy Watch"),
                                    mqtt_topic,
                                    sensor_info.get("mdns_hostname"),
                                    sensor_info.get("ip_addr"),
                                    1 if sensor_info.get("is_online", True) else 0,
                                    self._parse_to_mysql_time(sensor_info.get("last_seen_at")),
                                    sen_id,
                                ),
                            )

                        else:
                            cursor.execute(
                                """
                                INSERT INTO sensor (
                                    sensor_id,
                                    jetson_id,
                                    sensor_type,
                                    sen_name,
                                    sen_locate,
                                    model,
                                    mqtt_topic,
                                    mdns_hostname,
                                    ip_addr,
                                    is_online,
                                    last_seen_at,
                                    registered_at,
                                    created_at,
                                    updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, NOW(), NOW(), NOW()
                                )
                                """,
                                (
                                    sensor_id,
                                    jetson_id,
                                    sensor_type,
                                    sensor_info.get("sen_name", sensor_id),
                                    sensor_info.get("sen_locate", "worker_wrist"),
                                    sensor_info.get("model", "Galaxy Watch"),
                                    mqtt_topic,
                                    sensor_info.get("mdns_hostname"),
                                    sensor_info.get("ip_addr"),
                                    1 if sensor_info.get("is_online", True) else 0,
                                    self._parse_to_mysql_time(sensor_info.get("last_seen_at")),
                                ),
                            )
                            sen_id = cursor.lastrowid

                        # 3. 작업자와 센서 1대1 매핑
                        cursor.execute(
                            """
                            UPDATE worker
                            SET sen_id = %s
                            WHERE dept_id = %s
                              AND is_manager = 0
                            """,
                            (sen_id, dept_id),
                        )

                        if cursor.rowcount == 0:
                            raise ValueError("작업자 센서 매핑에 실패했습니다.")

                    conn.commit()

                    return {
                        "sen_id": sen_id,
                        "sensor_id": sensor_id,
                        "sensor_type": sensor_type,
                        "dept_id": dept_id,
                        "worker_name": worker["name"],
                        "mqtt_base": sensor_info.get("mqtt_base"),
                        "mqtt_topic": mqtt_topic,
                    }

                except Exception:
                    conn.rollback()
                    raise

        except Exception as e:
            logging.error("register_sensor_with_worker 오류: %s", e)
            raise

    def unregister_sensor_by_sensor_id(self, sensor_id: str) -> bool:
        """
        sensor 삭제.
        FK 정책에 따라 worker.sen_id는 ON DELETE SET NULL로 비워진다.
        hb_trans/th_trans가 ON DELETE CASCADE라면 측정 데이터도 삭제된다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute(
                        """
                        DELETE FROM sensor
                        WHERE sensor_id = %s
                        """,
                        (sensor_id,),
                    )
                conn.commit()
            return affected > 0

        except Exception as e:
            logging.error("unregister_sensor_by_sensor_id 오류: %s", e)
            return False

    def unassign_worker_sensor(self, dept_id: int) -> bool:
        """
        작업자의 착용 센서 매핑만 해제한다.
        sensor 테이블의 센서는 삭제하지 않는다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute(
                        """
                        UPDATE worker
                        SET sen_id = NULL
                        WHERE dept_id = %s
                          AND is_manager = 0
                        """,
                        (dept_id,),
                    )
                conn.commit()
            return affected > 0

        except Exception as e:
            logging.error("unassign_worker_sensor 오류: %s", e)
            return False

    def update_sensor_online(
        self,
        sensor_id: str,
        is_online: bool,
        last_seen_at: Any = None,
    ) -> bool:
        try:
            mysql_time = self._parse_to_mysql_time(last_seen_at)

            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute(
                        """
                        UPDATE sensor
                        SET
                            is_online = %s,
                            last_seen_at = %s,
                            updated_at = NOW()
                        WHERE sensor_id = %s
                        """,
                        (1 if is_online else 0, mysql_time, sensor_id),
                    )
                conn.commit()
            return affected > 0

        except Exception as e:
            logging.error("update_sensor_online 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 센서 원격 측정 저장
    # ------------------------------------------------------------------

    def _get_sen_id_by_sensor_id(self, cursor, sensor_id: str) -> Optional[int]:
        cursor.execute(
            """
            SELECT sen_id
            FROM sensor
            WHERE sensor_id = %s
            LIMIT 1
            """,
            (sensor_id,),
        )
        row = cursor.fetchone()
        return row["sen_id"] if row else None

    def save_sensor_telemetry(
        self,
        sensor_id: str,
        temperature: float,
        humidity: float,
        ts: Any = None,
    ) -> bool:
        """온습도 측정값 저장 (sensor_id 문자열 기준).

        복합 PK(sen_id, time) 중복 시 temp/humid를 덮어씁니다.
        미등록 센서이면 저장하지 않습니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    sen_id = self._get_sen_id_by_sensor_id(cursor, sensor_id)
                    if sen_id is None:
                        logging.warning("미등록 온습도 센서 telemetry 무시: sensor_id=%s", sensor_id)
                        return False

                    cursor.execute(
                        """
                        INSERT INTO th_trans (sen_id, time, temp, humid)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            temp  = VALUES(temp),
                            humid = VALUES(humid)
                        """,
                        (sen_id, self._parse_to_mysql_time(ts), temperature, humidity),
                    )

                conn.commit()
            return True

        except Exception as e:
            logging.error("save_sensor_telemetry 오류: %s", e)
            return False

    def save_heart_rate_telemetry(
        self,
        sensor_id: str,
        hr: float,
        ts: Any = None,
    ) -> bool:
        """심박 측정값 저장 (sensor_id 문자열 기준).

        복합 PK(sen_id, time) 중복 시 hr을 덮어씁니다.
        미등록 센서이면 저장하지 않습니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    sen_id = self._get_sen_id_by_sensor_id(cursor, sensor_id)
                    if sen_id is None:
                        logging.warning("미등록 심박 센서 telemetry 무시: sensor_id=%s", sensor_id)
                        return False

                    cursor.execute(
                        """
                        INSERT INTO hb_trans (sen_id, time, hr)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            hr = VALUES(hr)
                        """,
                        (sen_id, self._parse_to_mysql_time(ts), hr),
                    )

                conn.commit()
            return True

        except Exception as e:
            logging.error("save_heart_rate_telemetry 오류: %s", e)
            return False

    def insert_th_trans(
        self,
        sen_id: int,
        time_val: Any = None,
        temp: float = None,
        humid: float = None,
    ) -> bool:
        """온습도 측정값을 th_trans에 직접 저장 (sen_id int 기준).

        복합 PK(sen_id, time) 중복 시 temp/humid를 덮어씁니다.
        sensor_service._handle_sensor_data_topic() 에서 호출됩니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO th_trans (sen_id, time, temp, humid)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            temp  = VALUES(temp),
                            humid = VALUES(humid)
                        """,
                        (sen_id, self._parse_to_mysql_time(time_val), temp, humid),
                    )
                conn.commit()
            return True

        except Exception as e:
            logging.error("insert_th_trans 오류 | sen_id=%s | error=%s", sen_id, e)
            return False

    def insert_hb_trans(
        self,
        sen_id: int,
        time_val: Any = None,
        hr: float = None,
    ) -> bool:
        """심박 측정값을 hb_trans에 직접 저장 (sen_id int 기준).

        복합 PK(sen_id, time) 중복 시 hr을 덮어씁니다.
        sensor_service._handle_sensor_data_topic() 에서 호출됩니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO hb_trans (sen_id, time, hr)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            hr = VALUES(hr)
                        """,
                        (sen_id, self._parse_to_mysql_time(time_val), hr),
                    )
                conn.commit()
            return True

        except Exception as e:
            logging.error("insert_hb_trans 오류 | sen_id=%s | error=%s", sen_id, e)
            return False

    def update_sensor_last_seen_by_id(self, sen_id: int) -> bool:
        """센서 온라인 상태와 last_seen_at을 sen_id(int PK) 기준으로 갱신.

        insert_th_trans / insert_hb_trans 직후에 호출됩니다.
        기존 update_sensor_online()은 sensor_id(문자열) 기준이라 별도로 추가합니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE sensor
                        SET
                            is_online    = 1,
                            last_seen_at = NOW(),
                            updated_at   = NOW()
                        WHERE sen_id = %s
                        """,
                        (sen_id,),
                    )
                conn.commit()
            return True

        except Exception as e:
            logging.error("update_sensor_last_seen_by_id 오류 | sen_id=%s | error=%s", sen_id, e)
            return False

    def get_sensor_by_mqtt_topic(self, topic: str) -> Optional[dict]:
        """mqtt_topic 컬럼 값이 topic과 일치하는 센서 행을 반환.

        sensor_id로 조회가 안 될 때의 fallback으로 사용됩니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT *
                        FROM sensor
                        WHERE mqtt_topic = %s
                        LIMIT 1
                        """,
                        (topic,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_sensor_by_mqtt_topic 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # 카메라
    # ------------------------------------------------------------------

    def register_camera_info(
        self,
        ip_address: str,
        camera_id: str,
        camera_pw: str,
        rtsp_url: str | None = None,
        space_id: int | None = None,
        sen_name: str | None = None,
        sen_locate: str | None = None,
    ):
        """
        카메라를 sensor + camera_info에 등록한다.
        기존 코드에서는 sensor 필수 컬럼이 부족할 수 있어 보완했다.
        """
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        cursor.execute(
                            """
                            SELECT sen_id
                            FROM camera_info
                            WHERE ip_address = %s
                            LIMIT 1
                            """,
                            (ip_address,),
                        )
                        existing = cursor.fetchone()
                        if existing:
                            return self.get_cctv_by_sen_id(existing["sen_id"])

                        cursor.execute(
                            """
                            SELECT jetson_id, jetson_loc
                            FROM jetson
                            ORDER BY jetson_id
                            LIMIT 1
                            """
                        )
                        jetson = cursor.fetchone()
                        if not jetson:
                            raise ValueError("등록된 Jetson 정보가 없습니다.")

                        auto_name = sen_name or f"CAM_{ip_address.split('.')[-1]}"
                        auto_loc = sen_locate or jetson["jetson_loc"]
                        sensor_id = f"camera-{ip_address.replace('.', '-')}"

                        cursor.execute(
                            """
                            INSERT INTO sensor (
                                sensor_id,
                                jetson_id,
                                sensor_type,
                                sen_name,
                                sen_locate,
                                model,
                                mqtt_topic,
                                mdns_hostname,
                                ip_addr,
                                is_online,
                                last_seen_at,
                                registered_at,
                                created_at,
                                updated_at,
                                space_id
                            ) VALUES (
                                %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                NOW(), NOW(), NOW(), NOW(), %s
                            )
                            """,
                            (
                                sensor_id,
                                jetson["jetson_id"],
                                "camera",
                                auto_name,
                                auto_loc,
                                "ip_camera",
                                None,
                                None,
                                ip_address,
                                1,
                                space_id,
                            ),
                        )

                        new_sen_id = cursor.lastrowid

                        cursor.execute(
                            """
                            INSERT INTO camera_info (
                                sen_id,
                                ip_address,
                                camera_id,
                                camera_pw,
                                health,
                                space_id
                            ) VALUES (
                                %s, %s, %s, %s, 1, %s
                            )
                            """,
                            (new_sen_id, ip_address, camera_id, camera_pw, space_id),
                        )

                    conn.commit()

                    row = self.get_cctv_by_sen_id(new_sen_id)
                    if row is not None:
                        row["rtsp_url"] = rtsp_url
                    return row

                except Exception:
                    conn.rollback()
                    raise

        except Exception as e:
            logging.error("register_camera_info 오류: %s", e)
            return False

    def get_cctv_list(self, space_id: int | None = None) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    params = []
                    where_sql = "WHERE s.sensor_type IN ('camera', 'cctv')"
                    if space_id is not None:
                        where_sql += " AND COALESCE(c.space_id, s.space_id) = %s"
                        params.append(space_id)

                    cursor.execute(
                        f"""
                        SELECT
                            c.sen_id,
                            c.ip_address,
                            c.camera_id,
                            c.camera_pw,
                            c.health,
                            s.sensor_id,
                            s.sen_name,
                            s.sen_locate,
                            s.is_online,
                            s.registered_at,
                            s.created_at,
                            s.updated_at,
                            COALESCE(c.space_id, s.space_id) AS space_id,
                            sp.space_name
                        FROM camera_info c
                        JOIN sensor s
                          ON c.sen_id = s.sen_id
                        LEFT JOIN ds_space sp
                          ON COALESCE(c.space_id, s.space_id) = sp.space_id
                        {where_sql}
                        ORDER BY s.sen_name
                        """,
                        tuple(params),
                    )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_cctv_list 오류: %s", e)
            return []

    def get_cctv_by_sen_id(self, sen_id: int) -> dict | None:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            c.sen_id,
                            c.ip_address,
                            c.camera_id,
                            c.camera_pw,
                            c.health,
                            s.sensor_id,
                            s.sen_name,
                            s.sen_locate,
                            s.is_online,
                            s.registered_at,
                            s.created_at,
                            s.updated_at,
                            COALESCE(c.space_id, s.space_id) AS space_id,
                            sp.space_name
                        FROM camera_info c
                        JOIN sensor s
                          ON c.sen_id = s.sen_id
                        LEFT JOIN ds_space sp
                          ON COALESCE(c.space_id, s.space_id) = sp.space_id
                        WHERE c.sen_id = %s
                          AND s.sensor_type IN ('camera', 'cctv')
                        LIMIT 1
                        """,
                        (sen_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_cctv_by_sen_id 오류: %s", e)
            return None

    def update_camera_info(
        self,
        sen_id: int,
        *,
        ip_address: str | None = None,
        camera_id: str | None = None,
        camera_pw: str | None = None,
        sen_name: str | None = None,
        is_online: bool | None = None,
        health: int | bool | None = None,
        space_id: int | None = None,
    ) -> dict | None:
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        sensor_sets = []
                        sensor_params = []
                        camera_sets = []
                        camera_params = []

                        if ip_address is not None:
                            sensor_sets.append("ip_addr = %s")
                            sensor_params.append(ip_address)
                            camera_sets.append("ip_address = %s")
                            camera_params.append(ip_address)

                        if sen_name is not None:
                            sensor_sets.append("sen_name = %s")
                            sensor_params.append(sen_name)

                        if is_online is not None:
                            sensor_sets.append("is_online = %s")
                            sensor_params.append(1 if is_online else 0)

                        if space_id is not None:
                            sensor_sets.append("space_id = %s")
                            sensor_params.append(space_id)
                            camera_sets.append("space_id = %s")
                            camera_params.append(space_id)

                        if camera_id is not None:
                            camera_sets.append("camera_id = %s")
                            camera_params.append(camera_id)

                        if camera_pw is not None:
                            camera_sets.append("camera_pw = %s")
                            camera_params.append(camera_pw)

                        if health is not None:
                            camera_sets.append("health = %s")
                            camera_params.append(1 if health else 0)

                        if sensor_sets:
                            sensor_params.append(sen_id)
                            cursor.execute(
                                f"""
                                UPDATE sensor
                                SET {", ".join(sensor_sets)}
                                WHERE sen_id = %s
                                """,
                                tuple(sensor_params),
                            )

                        if camera_sets:
                            camera_params.append(sen_id)
                            cursor.execute(
                                f"""
                                UPDATE camera_info
                                SET {", ".join(camera_sets)}
                                WHERE sen_id = %s
                                """,
                                tuple(camera_params),
                            )

                    conn.commit()

                except Exception:
                    conn.rollback()
                    raise

            return self.get_cctv_by_sen_id(sen_id)

        except Exception as e:
            logging.error("update_camera_info 오류: %s", e)
            return None

    def delete_camera_info(self, sen_id: int) -> bool:
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()
                        cursor.execute("DELETE FROM camera_info WHERE sen_id = %s", (sen_id,))
                        cursor.execute(
                            """
                            DELETE FROM sensor
                            WHERE sen_id = %s
                              AND sensor_type IN ('camera', 'cctv')
                            """,
                            (sen_id,),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            return True

        except Exception as e:
            logging.error("delete_camera_info 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 이벤트 / 리포트
    # ------------------------------------------------------------------

    def save_event_log(self, db_payload: dict) -> dict:
        """
        event 저장.
        가능하면 sen_name보다 sensor_id 기반 저장을 권장한다.
        기존 호환을 위해 sen_name 기반도 유지한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    sensor_id = db_payload.get("sensor_id")
                    sen_name = db_payload.get("sen_name")

                    sen_id = None

                    if sensor_id:
                        cursor.execute(
                            """
                            SELECT sen_id
                            FROM sensor
                            WHERE sensor_id = %s
                            LIMIT 1
                            """,
                            (sensor_id,),
                        )
                        row = cursor.fetchone()
                        sen_id = row["sen_id"] if row else None

                    elif sen_name:
                        cursor.execute(
                            """
                            SELECT sen_id
                            FROM sensor
                            WHERE sen_name = %s
                            LIMIT 1
                            """,
                            (sen_name,),
                        )
                        row = cursor.fetchone()
                        sen_id = row["sen_id"] if row else None

                    cursor.execute(
                        """
                        INSERT INTO event (
                            ev_code_id,
                            sen_id,
                            message,
                            detected_value,
                            time,
                            measures
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            db_payload["ev_code_id"],
                            sen_id,
                            db_payload.get("message"),
                            db_payload.get("detected_value"),
                            self._parse_to_mysql_time(db_payload.get("time")),
                            db_payload.get("measures"),
                        ),
                    )

                    event_id = cursor.lastrowid

                conn.commit()

            return {"event_id": event_id}

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
                        SELECT
                            c.sen_id,
                            s.sen_name,
                            s.sen_locate,
                            (
                                SELECT ev_code_id
                                FROM event_code
                                WHERE ev_code_name = %s
                                LIMIT 1
                            ) AS ev_code_id
                        FROM camera_info c
                        JOIN sensor s
                          ON c.sen_id = s.sen_id
                        WHERE c.ip_address = %s
                        LIMIT 1
                        """,
                        (ev_code_name, ip_address),
                    )

                    info = cursor.fetchone()

                    sen_id = info["sen_id"] if info else None
                    camera_name = info["sen_name"] if info else "unknown_camera"
                    camera_loc = info["sen_locate"] if info else "알 수 없음"
                    ev_code_id = info["ev_code_id"] if info and info["ev_code_id"] else 0

                    cursor.execute(
                        """
                        INSERT INTO event (
                            ev_code_id,
                            sen_id,
                            message,
                            detected_value,
                            time
                        ) VALUES (
                            %s, %s, %s, 'AI_VISION_DETECTION', %s
                        )
                        """,
                        (
                            ev_code_id,
                            sen_id,
                            req_payload.get("message"),
                            mysql_time,
                        ),
                    )

                    event_id = cursor.lastrowid

                conn.commit()

            return {
                "event_id": event_id,
                "camera_name": camera_name,
                "camera_loc": camera_loc,
            }

        except Exception as e:
            logging.error("process_ai_event 오류: %s", e)
            return {
                "event_id": 0,
                "camera_name": "unknown_camera",
                "camera_loc": "알 수 없음",
            }

    def update_event_measures(self, event_id: int, measures: str) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    affected = cursor.execute(
                        """
                        UPDATE event
                        SET measures = %s
                        WHERE event_id = %s
                        """,
                        (measures, event_id),
                    )

                conn.commit()
            return affected > 0

        except Exception as e:
            logging.error("update_event_measures 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # 작업자 / 관리자
    # ------------------------------------------------------------------

    def get_worker_name_by_id(self, worker_id: str):
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT name
                        FROM worker
                        WHERE dept_id = %s
                        """,
                        (worker_id,),
                    )
                    result = cursor.fetchone()

            return result["name"] if result else None

        except Exception as e:
            logging.error("get_worker_name_by_id 오류: %s", e)
            return None

    def get_workers(self, is_manager: Optional[int] = None) -> list[dict]:
        """
        앱에서 작업자 선택 목록을 보여주기 위한 함수.
        is_manager=0이면 현장 작업자만 조회한다.
        is_manager=1이면 관리자만 조회한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if is_manager is None:
                        cursor.execute(
                            """
                            SELECT
                                w.dept_id,
                                w.name,
                                w.is_manager,
                                w.sen_id,
                                s.sensor_id,
                                s.sensor_type,
                                s.sen_name AS sensor_name
                            FROM worker w
                            LEFT JOIN sensor s
                              ON w.sen_id = s.sen_id
                            ORDER BY w.dept_id
                            """
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT
                                w.dept_id,
                                w.name,
                                w.is_manager,
                                w.sen_id,
                                s.sensor_id,
                                s.sensor_type,
                                s.sen_name AS sensor_name
                            FROM worker w
                            LEFT JOIN sensor s
                              ON w.sen_id = s.sen_id
                            WHERE w.is_manager = %s
                            ORDER BY w.dept_id
                            """,
                            (is_manager,),
                        )

                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_workers 오류: %s", e)
            return []

    def get_worker_by_dept_id(self, dept_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            w.dept_id,
                            w.name,
                            w.is_manager,
                            w.sen_id,
                            s.sensor_id,
                            s.sensor_type,
                            s.sen_name AS sensor_name
                        FROM worker w
                        LEFT JOIN sensor s
                          ON w.sen_id = s.sen_id
                        WHERE w.dept_id = %s
                        LIMIT 1
                        """,
                        (dept_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_worker_by_dept_id 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # 플로어 맵 / 센서 위치
    # ------------------------------------------------------------------

    def get_floor_map_by_jetson_id(self, jetson_id: int):
        """jetson_id 기준 가장 최근 평면도 1개 반환."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            map_id,
                            jetson_id,
                            map_name,
                            image_base64,
                            image_mime_type,
                            image_width,
                            image_height,
                            created_at,
                            updated_at
                        FROM floor_map
                        WHERE jetson_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (jetson_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_floor_map_by_jetson_id 오류: %s", e)
            return None

    def get_floor_map_by_map_id(self, map_id: int):
        """map_id 기준 평면도 1개 반환 (존재 여부 확인용)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT map_id, jetson_id, map_name,
                               image_mime_type, image_width, image_height,
                               created_at, updated_at
                        FROM floor_map
                        WHERE map_id = %s
                        LIMIT 1
                        """,
                        (map_id,),
                    )
                    return cursor.fetchone()

        except Exception as e:
            logging.error("get_floor_map_by_map_id 오류: %s", e)
            return None

    def get_sensor_positions_by_map_id(self, map_id: int) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            p.position_id,
                            p.map_id,
                            p.sensor_id,
                            p.x_ratio,
                            p.y_ratio,
                            s.sen_name,
                            s.sensor_type,
                            s.sen_locate,
                            s.model,
                            s.is_online
                        FROM sensor_map_position p
                        JOIN sensor s
                          ON p.sensor_id = s.sensor_id
                        WHERE p.map_id = %s
                        ORDER BY s.sen_name
                        """,
                        (map_id,),
                    )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_sensor_positions_by_map_id 오류: %s", e)
            return []

    def get_registered_sensors_by_jetson_id(
        self,
        jetson_id: int,
        map_id: Optional[int] = None,
    ) -> list[dict]:
        """jetson_id에 등록된 온습도 센서 목록 반환.

        map_id 를 전달하면 해당 맵에 이미 배치된 센서에 placed=1 을 함께 반환합니다.
        온습도 센서 타입만 반환합니다 (temp_humidity, temperature_humidity, th, temperature).
        """
        TH_TYPES = ("temp_humidity", "temperature_humidity", "th", "temperature")
        placeholders = ", ".join(["%s"] * len(TH_TYPES))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if map_id is not None:
                        # 특정 맵의 배치 여부(placed)를 함께 조회
                        cursor.execute(
                            f"""
                            SELECT
                                s.sensor_id,
                                s.sensor_type,
                                s.sen_name,
                                s.sen_locate,
                                s.model,
                                s.mqtt_topic,
                                s.is_online,
                                CASE WHEN p.position_id IS NOT NULL THEN 1 ELSE 0 END AS placed,
                                p.x_ratio,
                                p.y_ratio
                            FROM sensor s
                            LEFT JOIN sensor_map_position p
                              ON s.sensor_id = p.sensor_id
                             AND p.map_id = %s
                            WHERE s.jetson_id = %s
                              AND s.sensor_type IN ({placeholders})
                            ORDER BY s.sen_name
                            """,
                            (map_id, jetson_id, *TH_TYPES),
                        )
                    else:
                        cursor.execute(
                            f"""
                            SELECT
                                sensor_id,
                                sensor_type,
                                sen_name,
                                sen_locate,
                                model,
                                mqtt_topic,
                                is_online,
                                0 AS placed,
                                NULL AS x_ratio,
                                NULL AS y_ratio
                            FROM sensor
                            WHERE jetson_id = %s
                              AND sensor_type IN ({placeholders})
                            ORDER BY sen_name
                            """,
                            (jetson_id, *TH_TYPES),
                        )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_registered_sensors_by_jetson_id 오류: %s", e)
            return []

    def upsert_sensor_position(
        self,
        map_id: int,
        sensor_id: str,
        x_ratio: float,
        y_ratio: float,
    ) -> bool:
        """센서 위치 저장 / 갱신.

        DB에 UNIQUE KEY(map_id, sensor_id)가 있으면 ON DUPLICATE KEY UPDATE 가 동작합니다.
        없으면 SELECT 후 UPDATE/INSERT 방식으로 처리합니다.

        권장 ALTER TABLE (아직 적용하지 않은 경우):
            ALTER TABLE sensor_map_position
            ADD UNIQUE KEY uq_map_sensor (map_id, sensor_id);
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # 먼저 존재 여부 확인 (UNIQUE KEY 유무에 관계없이 안전하게 처리)
                    cursor.execute(
                        """
                        SELECT position_id
                        FROM sensor_map_position
                        WHERE map_id = %s AND sensor_id = %s
                        LIMIT 1
                        """,
                        (map_id, sensor_id),
                    )
                    existing = cursor.fetchone()

                    if existing:
                        cursor.execute(
                            """
                            UPDATE sensor_map_position
                            SET x_ratio = %s, y_ratio = %s, updated_at = NOW()
                            WHERE map_id = %s AND sensor_id = %s
                            """,
                            (x_ratio, y_ratio, map_id, sensor_id),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO sensor_map_position
                                (map_id, sensor_id, x_ratio, y_ratio)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (map_id, sensor_id, x_ratio, y_ratio),
                        )

                conn.commit()
            return True

        except Exception as e:
            logging.error("upsert_sensor_position 오류: %s", e)
            return False


    def get_latest_th_by_sensor_id(self, sensor_id: str) -> Optional[dict]:
        """
        sensor.sensor_id 문자열 기준으로 온습도 최신값 1개 조회.

        앱 API:
            GET /api/maps/sensors/{sensor_id}/latest

        센서가 존재하지만 측정값이 없으면
        time, temp, humid는 None으로 반환합니다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. 센서 존재 확인
                    cursor.execute(
                        """
                        SELECT
                            sen_id,
                            sensor_id,
                            sen_name,
                            sensor_type,
                            sen_locate,
                            is_online
                        FROM sensor
                        WHERE sensor_id = %s
                        LIMIT 1
                        """,
                        (sensor_id,),
                    )
                    sensor = cursor.fetchone()

                    if not sensor:
                        return None

                    # 2. 해당 센서의 최신 온습도 데이터 조회
                    cursor.execute(
                        """
                        SELECT
                            time,
                            temp,
                            humid
                        FROM th_trans
                        WHERE sen_id = %s
                        ORDER BY time DESC
                        LIMIT 1
                        """,
                        (sensor["sen_id"],),
                    )
                    latest = cursor.fetchone()

                    return {
                        "sen_id": sensor["sen_id"],
                        "sensor_id": sensor["sensor_id"],
                        "sen_name": sensor["sen_name"],
                        "sensor_type": sensor["sensor_type"],
                        "sen_locate": sensor["sen_locate"],
                        "is_online": sensor["is_online"],
                        "time": latest["time"] if latest else None,
                        "temp": latest["temp"] if latest else None,
                        "humid": latest["humid"] if latest else None,
                    }

        except Exception as e:
            logging.error(
                "get_latest_th_by_sensor_id 오류 | sensor_id=%s | error=%s",
                sensor_id,
                e,
            )
            return None


    # ------------------------------------------------------------------
    # 웹 대시보드용 최신 데이터
    # ------------------------------------------------------------------

    def get_web_sensor_th(self) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            t.sen_id,
                            s.sensor_id,
                            s.sen_name,
                            t.time,
                            t.temp,
                            t.humid
                        FROM th_trans t
                        JOIN sensor s
                          ON t.sen_id = s.sen_id
                        ORDER BY t.time DESC
                        LIMIT 1
                        """
                    )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_web_sensor_th 오류: %s", e)
            return []

    def get_web_sensor_hb(self) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            h.sen_id,
                            s.sensor_id,
                            s.sen_name,
                            w.dept_id,
                            w.name AS worker_name,
                            h.time,
                            h.hr
                        FROM hb_trans h
                        JOIN sensor s
                          ON h.sen_id = s.sen_id
                        LEFT JOIN worker w
                          ON w.sen_id = s.sen_id
                        ORDER BY h.time DESC
                        LIMIT 1
                        """
                    )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_web_sensor_hb 오류: %s", e)
            return []

    # ------------------------------------------------------------------
    # ds_space (공정/구역) CRUD
    # ------------------------------------------------------------------

    def get_all_spaces(self) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT space_id, space_name FROM ds_space ORDER BY space_id")
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_all_spaces 오류: %s", e)
            return []

    def get_space_by_id(self, space_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT space_id, space_name FROM ds_space WHERE space_id = %s",
                        (space_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_space_by_id 오류: %s", e)
            return None

    def create_space(self, space_name: str) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO ds_space (space_name) VALUES (%s)",
                        (space_name,),
                    )
                    space_id = cursor.lastrowid
                conn.commit()
            return self.get_space_by_id(space_id)
        except Exception as e:
            logging.error("create_space 오류: %s", e)
            return None

    def update_space(self, space_id: int, space_name: str) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE ds_space SET space_name = %s WHERE space_id = %s",
                        (space_name, space_id),
                    )
                conn.commit()
            return self.get_space_by_id(space_id)
        except Exception as e:
            logging.error("update_space 오류: %s", e)
            return None

    def delete_space(self, space_id: int) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM ds_space WHERE space_id = %s", (space_id,))
                conn.commit()
            return True
        except Exception as e:
            logging.error("delete_space 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # worker CRUD
    # ------------------------------------------------------------------

    def create_worker(self, dept_id: int, name: str, is_manager: int = 0) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO worker (dept_id, name, is_manager) VALUES (%s, %s, %s)",
                        (dept_id, name, is_manager),
                    )
                conn.commit()
            return self.get_worker_by_dept_id(dept_id)
        except Exception as e:
            logging.error("create_worker 오류: %s", e)
            return None

    def update_worker(self, dept_id: int, **kwargs) -> Optional[dict]:
        allowed = {"name", "is_manager", "sen_id"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return self.get_worker_by_dept_id(dept_id)
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE worker SET {set_clause} WHERE dept_id = %s",
                        (*fields.values(), dept_id),
                    )
                conn.commit()
            return self.get_worker_by_dept_id(dept_id)
        except Exception as e:
            logging.error("update_worker 오류: %s", e)
            return None

    def delete_worker(self, dept_id: int) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM worker WHERE dept_id = %s", (dept_id,))
                conn.commit()
            return True
        except Exception as e:
            logging.error("delete_worker 오류: %s", e)
            return False

    # ------------------------------------------------------------------
    # sensor (온습도 타입) CRUD — temperature router 용
    # ------------------------------------------------------------------

    def get_temp_sensors(self, space_id: int | None = None) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    q = """
                        SELECT sen_id, sensor_id, sen_name, space_id,
                               is_online, registered_at, sensor_type
                        FROM sensor
                        WHERE sensor_type = 'temp_humidity'
                    """
                    params: tuple = ()
                    if space_id is not None:
                        q += " AND space_id = %s"
                        params = (space_id,)
                    q += " ORDER BY sen_id"
                    cursor.execute(q, params)
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_temp_sensors 오류: %s", e)
            return []

    def get_temp_sensor_by_id(self, sen_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT sen_id, sensor_id, sen_name, space_id,
                               is_online, registered_at, sensor_type
                        FROM sensor
                        WHERE sen_id = %s AND sensor_type = 'temp_humidity'
                        """,
                        (sen_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_temp_sensor_by_id 오류: %s", e)
            return None

    def create_temp_sensor(
        self,
        sensor_id: str,
        sen_name: str,
        space_id: int | None = None,
        jetson_id: int | None = None,
    ) -> Optional[dict]:
        try:
            jetson_id = jetson_id or self._get_first_jetson_id()
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO sensor
                          (sensor_id, jetson_id, sensor_type, sen_name, sen_locate,
                           space_id, registered_at, created_at, updated_at)
                        VALUES (%s, %s, 'temp_humidity', %s, '', %s, NOW(), NOW(), NOW())
                        """,
                        (sensor_id, jetson_id, sen_name, space_id),
                    )
                    sen_id = cursor.lastrowid
                conn.commit()
            return self.get_temp_sensor_by_id(sen_id)
        except Exception as e:
            logging.error("create_temp_sensor 오류: %s", e)
            return None

    def update_temp_sensor(self, sen_id: int, **kwargs) -> Optional[dict]:
        mapping = {"name": "sen_name", "is_active": "is_online", "space_id": "space_id"}
        fields = {mapping[k]: v for k, v in kwargs.items() if k in mapping}
        if not fields:
            return self.get_temp_sensor_by_id(sen_id)
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE sensor SET {set_clause} WHERE sen_id = %s",
                        (*fields.values(), sen_id),
                    )
                conn.commit()
            return self.get_temp_sensor_by_id(sen_id)
        except Exception as e:
            logging.error("update_temp_sensor 오류: %s", e)
            return None

    def delete_temp_sensor(self, sen_id: int) -> bool:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM sensor WHERE sen_id = %s AND sensor_type = 'temp_humidity'",
                        (sen_id,),
                    )
                conn.commit()
            return True
        except Exception as e:
            logging.error("delete_temp_sensor 오류: %s", e)
            return False

    def _get_first_jetson_id(self) -> int:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT jetson_id FROM jetson LIMIT 1")
                    row = cursor.fetchone()
                    return row["jetson_id"] if row else 1
        except Exception:
            return 1

    # ------------------------------------------------------------------
    # event (이상 이벤트) CRUD
    # ------------------------------------------------------------------

    def get_events(
        self,
        space_id: int | None = None,
        status: str | None = None,
    ) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    q = """
                        SELECT e.event_id, e.ev_code_id, e.sen_id, e.message,
                               e.detected_value, e.time AS start_time,
                               e.end_time, e.status, e.measures,
                               ec.ev_code_name AS anomaly_type,
                               s.space_id
                        FROM event e
                        LEFT JOIN event_code ec ON e.ev_code_id = ec.ev_code_id
                        LEFT JOIN sensor s ON e.sen_id = s.sen_id
                    """
                    conditions, params = [], []
                    if space_id is not None:
                        conditions.append(
                            "e.sen_id IN (SELECT sen_id FROM sensor WHERE space_id = %s)"
                        )
                        params.append(space_id)
                    if status is not None:
                        conditions.append("e.status = %s")
                        params.append(status)
                    if conditions:
                        q += " WHERE " + " AND ".join(conditions)
                    q += " ORDER BY e.time DESC"
                    cursor.execute(q, params)
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_events 오류: %s", e)
            return []

    def get_event_by_id(self, event_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT e.event_id, e.ev_code_id, e.sen_id, e.message,
                               e.detected_value, e.time AS start_time,
                               e.end_time, e.status, e.measures,
                               ec.ev_code_name AS anomaly_type,
                               s.space_id
                        FROM event e
                        LEFT JOIN event_code ec ON e.ev_code_id = ec.ev_code_id
                        LEFT JOIN sensor s ON e.sen_id = s.sen_id
                        WHERE e.event_id = %s
                        """,
                        (event_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_event_by_id 오류: %s", e)
            return None

    def update_event(self, event_id: int, **kwargs) -> Optional[dict]:
        allowed = {"status", "end_time", "measures"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return self.get_event_by_id(event_id)
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE event SET {set_clause} WHERE event_id = %s",
                        (*fields.values(), event_id),
                    )
                conn.commit()
            return self.get_event_by_id(event_id)
        except Exception as e:
            logging.error("update_event 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # vlm_query CRUD
    # ------------------------------------------------------------------

    def save_vlm_query(
        self,
        event_id: int,
        prompt: str,
        response: str | None = None,
        frame_path: str | None = None,
    ) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO vlm_query (event_id, prompt, response, frame_path)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (event_id, prompt, response, frame_path),
                    )
                    query_id = cursor.lastrowid
                conn.commit()
            return self.get_vlm_query_by_id(query_id)
        except Exception as e:
            logging.error("save_vlm_query 오류: %s", e)
            return None

    def get_vlm_query_by_id(self, query_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM vlm_query WHERE query_id = %s",
                        (query_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_vlm_query_by_id 오류: %s", e)
            return None

    def get_vlm_queries_by_event(self, event_id: int) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM vlm_query WHERE event_id = %s ORDER BY queried_at",
                        (event_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_vlm_queries_by_event 오류: %s", e)
            return []

    # ------------------------------------------------------------------
    # event_report CRUD
    # ------------------------------------------------------------------

    def get_all_reports(self) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM event_report ORDER BY created_at DESC"
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_all_reports 오류: %s", e)
            return []

    def get_report_by_id(self, report_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM event_report WHERE report_id = %s",
                        (report_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_report_by_id 오류: %s", e)
            return None

    def get_report_by_event_id(self, event_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM event_report WHERE event_id = %s",
                        (event_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_report_by_event_id 오류: %s", e)
            return None

    def create_report(
        self,
        event_id: int,
        content: str,
        action_taken: str | None = None,
        created_by: str | None = None,
    ) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO event_report (event_id, content, action_taken, created_by)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (event_id, content, action_taken, created_by),
                    )
                    report_id = cursor.lastrowid
                conn.commit()
            return self.get_report_by_id(report_id)
        except Exception as e:
            logging.error("create_report 오류: %s", e)
            return None

    def update_report(self, report_id: int, **kwargs) -> Optional[dict]:
        allowed = {"content", "action_taken"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return self.get_report_by_id(report_id)
        set_clause = ", ".join(f"{k} = %s" for k in fields)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"UPDATE event_report SET {set_clause} WHERE report_id = %s",
                        (*fields.values(), report_id),
                    )
                conn.commit()
            return self.get_report_by_id(report_id)
        except Exception as e:
            logging.error("update_report 오류: %s", e)
            return None
