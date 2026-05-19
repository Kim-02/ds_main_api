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

    def get_all_jetsons(self) -> list[dict]:
        """Jetson 전체 목록 조회 (space_name 포함)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            j.jetson_id,
                            j.jetson_wp,
                            j.jetson_loc,
                            j.jetson_status,
                            j.ip_addr,
                            j.port,
                            j.space_id,
                            sp.space_name,
                            j.created_at,
                            j.updated_at
                        FROM jetson j
                        LEFT JOIN ds_space sp ON j.space_id = sp.space_id
                        ORDER BY j.jetson_id
                        """
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_all_jetsons 오류: %s", e)
            return []

    def get_jetson_by_id(self, jetson_id: int) -> Optional[dict]:
        """jetson_id 기준 단건 조회 (space_name 포함)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            j.jetson_id,
                            j.jetson_wp,
                            j.jetson_loc,
                            j.jetson_status,
                            j.ip_addr,
                            j.port,
                            j.space_id,
                            sp.space_name,
                            j.created_at,
                            j.updated_at
                        FROM jetson j
                        LEFT JOIN ds_space sp ON j.space_id = sp.space_id
                        WHERE j.jetson_id = %s
                        LIMIT 1
                        """,
                        (jetson_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_jetson_by_id 오류: %s", e)
            return None

    def get_camera_sen_ids_by_jetson_id(self, jetson_id: int) -> list:
        """jetson_id에 연결된 카메라 센서의 sen_id 목록 반환 (runtime stop용)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT sen_id FROM sensor
                        WHERE jetson_id = %s
                          AND sensor_type IN ('camera', 'cctv')
                        """,
                        (jetson_id,),
                    )
                    return [row["sen_id"] for row in cursor.fetchall()]
        except Exception as e:
            logging.error("get_camera_sen_ids_by_jetson_id 오류: %s", e)
            return []

    def delete_jetson_cascade(self, jetson_id: int) -> bool:
        """Jetson + 연결된 camera_info + sensor 전체 삭제 (순서 보장)."""
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()
                        cursor.execute(
                            """
                            DELETE ci FROM camera_info ci
                            JOIN sensor s ON ci.sen_id = s.sen_id
                            WHERE s.jetson_id = %s
                            """,
                            (jetson_id,),
                        )
                        cursor.execute(
                            "DELETE FROM sensor WHERE jetson_id = %s",
                            (jetson_id,),
                        )
                        cursor.execute(
                            "DELETE FROM jetson WHERE jetson_id = %s",
                            (jetson_id,),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            return True
        except Exception as e:
            logging.error("delete_jetson_cascade 오류: %s", e)
            return False

    def upsert_jetson(self, data: dict) -> tuple:
        """
        ip_addr + port 기준으로 Jetson을 찾아 없으면 INSERT, 있으면 UPDATE.
        Returns: (row, is_new: bool)
        """
        ip_addr = data.get("ip_addr")
        port = data.get("port", 8080)
        space_id = data.get("space_id")

        if not ip_addr:
            raise ValueError("ip_addr는 필수입니다.")
        if space_id is None:
            raise ValueError("space_id는 필수입니다.")

        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        cursor.execute(
                            "SELECT space_id FROM ds_space WHERE space_id = %s LIMIT 1",
                            (space_id,),
                        )
                        if not cursor.fetchone():
                            raise ValueError(f"존재하지 않는 space_id입니다: {space_id}")

                        cursor.execute(
                            "SELECT jetson_id FROM jetson WHERE ip_addr = %s AND port = %s LIMIT 1",
                            (ip_addr, port),
                        )
                        existing = cursor.fetchone()

                        if existing:
                            jetson_id = existing["jetson_id"]
                            cursor.execute(
                                """
                                UPDATE jetson
                                SET
                                    jetson_wp = %s,
                                    jetson_loc = %s,
                                    jetson_status = %s,
                                    space_id = %s,
                                    updated_at = NOW()
                                WHERE jetson_id = %s
                                """,
                                (
                                    data.get("jetson_wp", "Jetson"),
                                    data.get("jetson_loc", ""),
                                    1 if data.get("jetson_status", True) else 0,
                                    space_id,
                                    jetson_id,
                                ),
                            )
                            cursor.execute(
                                "UPDATE sensor SET space_id = %s, updated_at = NOW() WHERE jetson_id = %s",
                                (space_id, jetson_id),
                            )
                            cursor.execute(
                                """
                                UPDATE camera_info ci
                                JOIN sensor s ON ci.sen_id = s.sen_id
                                SET ci.space_id = %s
                                WHERE s.jetson_id = %s
                                """,
                                (space_id, jetson_id),
                            )
                            is_new = False
                        else:
                            cursor.execute(
                                """
                                INSERT INTO jetson (
                                    jetson_wp, jetson_loc, jetson_status,
                                    ip_addr, port, space_id
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    data.get("jetson_wp", "Jetson"),
                                    data.get("jetson_loc", ""),
                                    1 if data.get("jetson_status", True) else 0,
                                    ip_addr,
                                    port,
                                    space_id,
                                ),
                            )
                            jetson_id = cursor.lastrowid
                            is_new = True

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            return self.get_jetson_by_id(jetson_id), is_new

        except ValueError:
            raise
        except Exception as e:
            logging.error("upsert_jetson 오류: %s", e)
            return None, False

    def deactivate_jetson(self, jetson_id: int) -> bool:
        """
        Jetson 비활성화 (등록 해제).
        jetson_status = 0, 연결된 sensor.is_online = 0.
        이력 보존을 위해 space_id는 유지한다.
        """
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()
                        cursor.execute(
                            "UPDATE jetson SET jetson_status = 0, updated_at = NOW() WHERE jetson_id = %s",
                            (jetson_id,),
                        )
                        cursor.execute(
                            "UPDATE sensor SET is_online = 0, updated_at = NOW() WHERE jetson_id = %s",
                            (jetson_id,),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            return True
        except Exception as e:
            logging.error("deactivate_jetson 오류: %s", e)
            return False

    def create_jetson(self, data: dict) -> Optional[dict]:
        """Jetson 신규 등록. space_id가 전달되면 ds_space 존재 여부를 검증한다."""
        space_id = data.get("space_id")
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if space_id is not None:
                        cursor.execute(
                            "SELECT space_id FROM ds_space WHERE space_id = %s LIMIT 1",
                            (space_id,),
                        )
                        if not cursor.fetchone():
                            raise ValueError(f"존재하지 않는 space_id입니다: {space_id}")

                    cursor.execute(
                        """
                        INSERT INTO jetson (
                            jetson_wp,
                            jetson_loc,
                            jetson_status,
                            ip_addr,
                            port,
                            space_id
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            data.get("jetson_wp", "default_workplace"),
                            data.get("jetson_loc", "default_location"),
                            1 if data.get("jetson_status", True) else 0,
                            data.get("ip_addr"),
                            data.get("port", 8080),
                            space_id,
                        ),
                    )
                    new_id = cursor.lastrowid
                conn.commit()
            return self.get_jetson_by_id(new_id)
        except ValueError:
            raise
        except Exception as e:
            logging.error("create_jetson 오류: %s", e)
            return None

    def update_jetson(self, jetson_id: int, data: dict) -> Optional[dict]:
        """
        Jetson 정보 수정.
        space_id가 변경되면 해당 Jetson에 속한 sensor와 camera_info의 space_id도 동기화한다.
        """
        space_id = data.get("space_id")
        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        if space_id is not None:
                            cursor.execute(
                                "SELECT space_id FROM ds_space WHERE space_id = %s LIMIT 1",
                                (space_id,),
                            )
                            if not cursor.fetchone():
                                raise ValueError(f"존재하지 않는 space_id입니다: {space_id}")

                        sets, params = [], []
                        for col in ("jetson_wp", "jetson_loc", "ip_addr", "port"):
                            if col in data and data[col] is not None:
                                sets.append(f"{col} = %s")
                                params.append(data[col])
                        if "jetson_status" in data:
                            sets.append("jetson_status = %s")
                            params.append(1 if data["jetson_status"] else 0)
                        if space_id is not None:
                            sets.append("space_id = %s")
                            params.append(space_id)

                        if sets:
                            params.append(jetson_id)
                            cursor.execute(
                                f"UPDATE jetson SET {', '.join(sets)} WHERE jetson_id = %s",
                                tuple(params),
                            )

                        # space_id 변경 시 연결된 sensor와 camera_info도 동기화
                        if space_id is not None:
                            cursor.execute(
                                """
                                UPDATE sensor
                                SET space_id = %s, updated_at = NOW()
                                WHERE jetson_id = %s
                                """,
                                (space_id, jetson_id),
                            )
                            cursor.execute(
                                """
                                UPDATE camera_info ci
                                JOIN sensor s ON ci.sen_id = s.sen_id
                                SET ci.space_id = %s
                                WHERE s.jetson_id = %s
                                """,
                                (space_id, jetson_id),
                            )

                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            return self.get_jetson_by_id(jetson_id)
        except ValueError:
            raise
        except Exception as e:
            logging.error("update_jetson 오류: %s", e)
            return None

    def get_cameras_by_danger_sensor_id(self, sensor_id: str) -> list[dict]:
        """
        위험 센서의 sensor_id 기준으로 같은 space_id에 속한 카메라 목록을 반환한다.
        VLM 분석 요청 대상 CCTV를 찾을 때 사용한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            ci.sen_id,
                            ci.ip_address,
                            ci.camera_id,
                            ci.camera_pw,
                            ci.health,
                            ci.space_id,
                            s.sensor_id,
                            s.sen_name,
                            s.sensor_type,
                            s.jetson_id
                        FROM sensor danger_sensor
                        JOIN camera_info ci
                          ON ci.space_id = danger_sensor.space_id
                        JOIN sensor s
                          ON ci.sen_id = s.sen_id
                        WHERE danger_sensor.sensor_id = %s
                          AND danger_sensor.space_id IS NOT NULL
                          AND s.sensor_type IN ('camera', 'cctv')
                        """,
                        (sensor_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_cameras_by_danger_sensor_id 오류: %s", e)
            return []

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
                    logging.debug("register_jetson_connection jetson=%s", jetson)
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
        register_date 제거 버전. space_name 포함.
        """
        query = """
            SELECT
                s.sen_id,
                s.sensor_id,
                s.jetson_id,
                s.sensor_type,
                s.sen_name,
                s.sen_locate,
                s.model,
                s.mqtt_topic,
                s.mdns_hostname,
                s.ip_addr,
                s.space_id,
                sp.space_name,
                sp.hazard_type,
                sp.is_hazard,
                s.is_online,
                s.last_seen_at,
                s.registered_at,
                s.created_at,
                s.updated_at
            FROM sensor s
            LEFT JOIN ds_space sp ON s.space_id = sp.space_id
            ORDER BY s.updated_at DESC
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
                            sp.space_name,
                            sp.hazard_type,
                            sp.is_hazard
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
                            sp.hazard_type,
                            sp.is_hazard,
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
        jetson.space_id가 NULL이면 ValueError를 발생시킨다.

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
                updated_at,
                space_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, NOW(), NOW(), NOW(), %s
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
                space_id = VALUES(space_id),
                updated_at = NOW()
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # jetson의 space_id를 먼저 조회
                    cursor.execute(
                        "SELECT space_id FROM jetson WHERE jetson_id = %s LIMIT 1",
                        (jetson_id,),
                    )
                    jetson_row = cursor.fetchone()
                    if not jetson_row:
                        raise ValueError(f"존재하지 않는 Jetson입니다. jetson_id={jetson_id}")
                    space_id = jetson_row["space_id"]
                    if space_id is None:
                        raise ValueError("Jetson에 공간이 매핑되어 있지 않아 센서를 등록할 수 없습니다.")

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
                                space_id,
                            ),
                        )

                conn.commit()
            return True

        except ValueError:
            raise
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

                        # 0. Jetson의 space_id 조회
                        cursor.execute(
                            "SELECT space_id FROM jetson WHERE jetson_id = %s LIMIT 1",
                            (jetson_id,),
                        )
                        jetson_row = cursor.fetchone()
                        if not jetson_row:
                            raise ValueError(f"존재하지 않는 Jetson입니다. jetson_id={jetson_id}")
                        space_id = jetson_row["space_id"]
                        if space_id is None:
                            raise ValueError("Jetson에 공간이 매핑되어 있지 않아 센서를 등록할 수 없습니다.")

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
                                    space_id = %s,
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
                                    space_id,
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
                                    updated_at,
                                    space_id
                                ) VALUES (
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, NOW(), NOW(), NOW(), %s
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
                                    space_id,
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
                            SELECT jetson_id, jetson_loc, space_id
                            FROM jetson
                            ORDER BY jetson_id
                            LIMIT 1
                            """
                        )
                        jetson = cursor.fetchone()
                        if not jetson:
                            raise ValueError("등록된 Jetson 정보가 없습니다.")

                        # space_id 미전달 시 Jetson의 space_id로 대체
                        if space_id is None:
                            space_id = jetson.get("space_id")

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
                            sp.space_name,
                            sp.hazard_type,
                            sp.is_hazard
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

    def get_camera_rtsp_by_sen_id(self, sen_id: int) -> dict | None:
        """camera_info에서 RTSP 접속 정보 조회. sensor_type 조건 없음.

        get_cctv_by_sen_id가 sensor_type 필터 때문에 None을 반환할 때 사용.
        stream API auto-start reader 용도.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            c.sen_id,
                            c.ip_address,
                            c.camera_id,
                            c.camera_pw
                        FROM camera_info c
                        WHERE c.sen_id = %s
                        LIMIT 1
                        """,
                        (sen_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_camera_rtsp_by_sen_id 오류: %s", e)
            return None

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
                            sp.space_name,
                            sp.hazard_type,
                            sp.is_hazard
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

    def ensure_alert_event_table(self) -> None:
        """alert_event 테이블이 없으면 자동으로 생성한다."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS alert_event (
                            event_id      INT AUTO_INCREMENT PRIMARY KEY,
                            space_id      INT NULL,
                            jetson_id     INT NULL,
                            camera_sen_id INT NULL,
                            sensor_id     VARCHAR(100) NULL,
                            title         VARCHAR(200) NOT NULL,
                            message       TEXT NOT NULL,
                            level         VARCHAR(30)  NOT NULL DEFAULT 'warning',
                            source        VARCHAR(50)  NOT NULL DEFAULT 'vlm',
                            event_type    VARCHAR(100) NULL,
                            is_read       TINYINT(1)   NOT NULL DEFAULT 0,
                            created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_alert_event_space_created (space_id, created_at),
                            INDEX idx_alert_event_sensor (sensor_id),
                            INDEX idx_alert_event_camera (camera_sen_id)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                        """
                    )
                conn.commit()
        except Exception as e:
            logging.error("ensure_alert_event_table 오류: %s", e)

    def save_vlm_alert_event(
        self,
        *,
        space_id: int | None = None,
        jetson_id: int | None = None,
        camera_sen_id: int | None = None,
        sensor_id: str | None = None,
        title: str,
        message: str,
        level: str = "warning",
        source: str = "vlm",
        event_type: str | None = None,
    ) -> int | None:
        """VLM 분석 결과를 alert_event 테이블에 저장한다. 저장된 event_id를 반환한다."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO alert_event
                            (space_id, jetson_id, camera_sen_id, sensor_id,
                             title, message, level, source, event_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            space_id,
                            jetson_id,
                            camera_sen_id,
                            sensor_id,
                            title,
                            message,
                            level,
                            source,
                            event_type,
                        ),
                    )
                    event_id = cursor.lastrowid
                conn.commit()
            logging.info(
                "[VLM_ALERT] saved event_id=%s space_id=%s camera_sen_id=%s sensor_id=%s level=%s",
                event_id, space_id, camera_sen_id, sensor_id, level,
            )
            return event_id
        except Exception as e:
            logging.error("save_vlm_alert_event 오류: %s", e)
            return None

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

    def get_floor_map_by_space_id(self, space_id: int) -> Optional[dict]:
        """space_id 기준 가장 최근 평면도 1개 반환."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            fm.map_id,
                            fm.jetson_id,
                            fm.space_id,
                            sp.space_name,
                            fm.map_name,
                            fm.image_base64,
                            fm.image_mime_type,
                            fm.image_width,
                            fm.image_height,
                            fm.created_at,
                            fm.updated_at
                        FROM floor_map fm
                        LEFT JOIN ds_space sp ON fm.space_id = sp.space_id
                        WHERE fm.space_id = %s
                        ORDER BY fm.updated_at DESC
                        LIMIT 1
                        """,
                        (space_id,),
                    )
                    return cursor.fetchone()
        except Exception as e:
            logging.error("get_floor_map_by_space_id 오류: %s", e)
            return None

    def validate_sensor_map_space(self, map_id: int, sensor_id: str) -> Optional[str]:
        """map_id와 sensor_id의 space_id가 일치하는지 검증.
        일치하면 None 반환, 불일치하면 에러 메시지 반환."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT space_id FROM floor_map WHERE map_id = %s LIMIT 1",
                        (map_id,),
                    )
                    map_row = cursor.fetchone()
                    if not map_row:
                        return f"map_id={map_id} 평면도가 없습니다."

                    cursor.execute(
                        "SELECT space_id FROM sensor WHERE sensor_id = %s LIMIT 1",
                        (sensor_id,),
                    )
                    sensor_row = cursor.fetchone()
                    if not sensor_row:
                        return f"sensor_id='{sensor_id}' 센서가 없습니다."

                    map_space = map_row["space_id"]
                    sensor_space = sensor_row["space_id"]

                    if map_space is None or sensor_space is None:
                        return "평면도 또는 센서에 공간(space_id) 정보가 없습니다."

                    if map_space != sensor_space:
                        return "센서와 평면도의 space_id가 일치하지 않습니다."

                    return None
        except Exception as e:
            logging.error("validate_sensor_map_space 오류: %s", e)
            return "공간 검증 중 오류가 발생했습니다."

    def get_registered_sensors_by_space_id(
        self,
        space_id: int,
        map_id: Optional[int] = None,
    ) -> list[dict]:
        """space_id 기준 온습도 센서 목록 반환.

        map_id 전달 시 해당 map에 이미 배치된 센서에 placed=1 반환.
        """
        TH_TYPES = ("temp_humidity", "temperature_humidity", "th", "temperature")
        placeholders = ", ".join(["%s"] * len(TH_TYPES))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if map_id is not None:
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
                                s.space_id,
                                sp.space_name,
                                CASE WHEN p.position_id IS NOT NULL THEN 1 ELSE 0 END AS placed,
                                p.x_ratio,
                                p.y_ratio
                            FROM sensor s
                            LEFT JOIN ds_space sp ON s.space_id = sp.space_id
                            LEFT JOIN sensor_map_position p
                              ON s.sensor_id = p.sensor_id
                             AND p.map_id = %s
                            WHERE s.space_id = %s
                              AND s.sensor_type IN ({placeholders})
                            ORDER BY s.sen_name
                            """,
                            (map_id, space_id, *TH_TYPES),
                        )
                    else:
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
                                s.space_id,
                                sp.space_name,
                                0 AS placed,
                                NULL AS x_ratio,
                                NULL AS y_ratio
                            FROM sensor s
                            LEFT JOIN ds_space sp ON s.space_id = sp.space_id
                            WHERE s.space_id = %s
                              AND s.sensor_type IN ({placeholders})
                            ORDER BY s.sen_name
                            """,
                            (space_id, *TH_TYPES),
                        )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_registered_sensors_by_space_id 오류: %s", e)
            return []

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
        """map_id 기준 평면도 1개 반환 (존재 여부 확인용). space_id, space_name 포함."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT fm.map_id, fm.jetson_id, fm.space_id, sp.space_name,
                               fm.map_name, fm.image_mime_type, fm.image_width,
                               fm.image_height, fm.created_at, fm.updated_at
                        FROM floor_map fm
                        LEFT JOIN ds_space sp ON fm.space_id = sp.space_id
                        WHERE fm.map_id = %s
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
                            s.sen_id,
                            s.sen_name,
                            s.sensor_type,
                            s.sen_locate,
                            s.model,
                            s.is_online,
                            ci.is_demo,
                            ci.demo_video_key,
                            latest.temp   AS latest_temp,
                            latest.humid  AS latest_humidity,
                            latest.time   AS latest_measured_at
                        FROM sensor_map_position p
                        JOIN sensor s ON p.sensor_id = s.sensor_id
                        LEFT JOIN camera_info ci ON ci.sen_id = s.sen_id
                        LEFT JOIN (
                            SELECT t.sen_id, t.temp, t.humid, t.time
                            FROM th_trans t
                            INNER JOIN (
                                SELECT sen_id, MAX(time) AS max_time
                                FROM th_trans
                                GROUP BY sen_id
                            ) latest_t ON t.sen_id = latest_t.sen_id
                                      AND t.time = latest_t.max_time
                        ) latest ON s.sen_id = latest.sen_id
                        WHERE p.map_id = %s
                        ORDER BY s.sen_name
                        """,
                        (map_id,),
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        if row.get("latest_measured_at") is not None:
                            val = row["latest_measured_at"]
                            if hasattr(val, "isoformat"):
                                row["latest_measured_at"] = val.isoformat()
                    return rows

        except Exception as e:
            logging.error("get_sensor_positions_by_map_id 오류: %s", e)
            return []

    def get_available_cctvs_for_map(
        self, space_id: int, map_id: Optional[int] = None
    ) -> list[dict]:
        """space_id 기준 배치 가능한 CCTV 목록을 반환한다.

        map_id를 전달하면 해당 맵에 이미 배치된 CCTV에 placed=1을 함께 반환한다.
        demo_camera 타입도 포함한다.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if map_id is not None:
                        cursor.execute(
                            """
                            SELECT
                                s.sen_id,
                                s.sensor_id,
                                s.sen_name,
                                s.sen_locate,
                                s.sensor_type,
                                s.is_online,
                                c.ip_address,
                                c.camera_id,
                                c.health,
                                IF(p.sensor_id IS NOT NULL, 1, 0) AS placed,
                                p.x_ratio,
                                p.y_ratio,
                                c.is_demo,
                                c.demo_video_key
                            FROM camera_info c
                            JOIN sensor s ON c.sen_id = s.sen_id
                            LEFT JOIN sensor_map_position p
                                ON p.sensor_id = s.sensor_id AND p.map_id = %s
                            WHERE COALESCE(c.space_id, s.space_id) = %s
                              AND s.sensor_type IN ('camera', 'cctv', 'demo_camera')
                            ORDER BY s.sen_name
                            """,
                            (map_id, space_id),
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT
                                s.sen_id,
                                s.sensor_id,
                                s.sen_name,
                                s.sen_locate,
                                s.sensor_type,
                                s.is_online,
                                c.ip_address,
                                c.camera_id,
                                c.health,
                                0 AS placed,
                                NULL AS x_ratio,
                                NULL AS y_ratio,
                                c.is_demo,
                                c.demo_video_key
                            FROM camera_info c
                            JOIN sensor s ON c.sen_id = s.sen_id
                            WHERE COALESCE(c.space_id, s.space_id) = %s
                              AND s.sensor_type IN ('camera', 'cctv', 'demo_camera')
                            ORDER BY s.sen_name
                            """,
                            (space_id,),
                        )
                    return cursor.fetchall()

        except Exception as e:
            logging.error("get_available_cctvs_for_map 오류: %s", e)
            return []

    def register_demo_camera(
        self,
        space_id: int,
        jetson_id: int,
        name: str = "시연용 화재 CCTV",
        demo_video_key: str = "scenario3_fire",
    ) -> dict | None:
        """시연용 가상 CCTV를 sensor + camera_info에 등록한다.

        같은 space_id + demo_video_key 조합이 이미 있으면 기존 항목을 반환한다.
        """
        sensor_id = f"demo-camera-{demo_video_key}-space-{space_id}"
        ip_address = f"demo://{demo_video_key}"

        try:
            with self._get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        conn.begin()

                        # 중복 확인
                        cursor.execute(
                            "SELECT sen_id FROM sensor WHERE sensor_id = %s LIMIT 1",
                            (sensor_id,),
                        )
                        existing = cursor.fetchone()
                        if existing:
                            return self.get_cctv_by_sen_id(existing["sen_id"])

                        # jetson 위치 정보 조회
                        cursor.execute(
                            "SELECT jetson_loc, space_id FROM jetson WHERE jetson_id = %s LIMIT 1",
                            (jetson_id,),
                        )
                        jetson = cursor.fetchone()
                        sen_locate = (jetson["jetson_loc"] if jetson else None) or "시연용"

                        cursor.execute(
                            """
                            INSERT INTO sensor (
                                sensor_id, jetson_id, sensor_type,
                                sen_name, sen_locate, model,
                                mqtt_topic, is_online,
                                last_seen_at, registered_at, created_at, updated_at,
                                space_id
                            ) VALUES (
                                %s, %s, 'demo_camera',
                                %s, %s, 'DEMO_FIRE_VIDEO',
                                NULL, 1,
                                NOW(), NOW(), NOW(), NOW(),
                                %s
                            )
                            """,
                            (sensor_id, jetson_id, name, sen_locate, space_id),
                        )
                        new_sen_id = cursor.lastrowid

                        cursor.execute(
                            """
                            INSERT INTO camera_info (
                                sen_id, ip_address, camera_id, camera_pw,
                                health, space_id, is_demo, demo_video_key
                            ) VALUES (
                                %s, %s, %s, NULL,
                                1, %s, 1, %s
                            )
                            """,
                            (new_sen_id, ip_address, demo_video_key, space_id, demo_video_key),
                        )

                    conn.commit()
                    return self.get_cctv_by_sen_id(new_sen_id)

                except Exception:
                    conn.rollback()
                    raise

        except Exception as e:
            logging.error("register_demo_camera 오류: %s", e)
            return None

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
    # 대시보드 요약
    # ------------------------------------------------------------------

    def get_recent_alerts_by_space_id(self, space_id: int, limit: int = 20) -> list[dict]:
        """space_id 기준 최근 알림 조회 (alert_event 테이블 기반)."""
        limit = min(max(1, limit), 100)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            ae.event_id,
                            ae.space_id,
                            ae.title,
                            ae.message,
                            ae.level,
                            ae.source,
                            ae.is_read,
                            ae.created_at,
                            COALESCE(s.sen_name, ae.sensor_id) AS camera_name
                        FROM alert_event ae
                        LEFT JOIN sensor s ON ae.camera_sen_id = s.sen_id
                        WHERE ae.space_id = %s
                        ORDER BY ae.created_at DESC
                        LIMIT %s
                        """,
                        (space_id, limit),
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        if row.get("created_at") and hasattr(row["created_at"], "strftime"):
                            row["created_at"] = row["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            for row in rows:
                if "is_read" in row:
                    row["is_read"] = bool(row["is_read"])
            logging.info(
                "[DASHBOARD] recent alerts space_id=%s count=%s", space_id, len(rows)
            )
            return rows
        except Exception as e:
            logging.error("get_recent_alerts_by_space_id 오류: %s", e)
            return []

    def get_watch_sensor_ids_by_space_id(self, space_id: int) -> list[str]:
        """space_id 기준 워치/밴드 타입 sensor_id 목록 반환."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT sensor_id
                        FROM sensor
                        WHERE space_id = %s
                          AND sensor_id IS NOT NULL
                          AND (
                            LOWER(sensor_type) LIKE %s
                            OR LOWER(sensor_type) LIKE %s
                            OR LOWER(sensor_type) LIKE %s
                          )
                        """,
                        (space_id, "%heart%", "%watch%", "%band%"),
                    )
                    return [row["sensor_id"] for row in cursor.fetchall() if row.get("sensor_id")]
        except Exception as e:
            logging.error("get_watch_sensor_ids_by_space_id 오류: %s", e)
            return []

    def mark_alert_as_read(self, event_id: int, space_id: int | None = None) -> bool:
        """alert_event 테이블의 is_read를 1로 업데이트한다."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    if space_id is not None:
                        cursor.execute(
                            "UPDATE alert_event SET is_read = 1 WHERE event_id = %s AND space_id = %s",
                            (event_id, space_id),
                        )
                    else:
                        cursor.execute(
                            "UPDATE alert_event SET is_read = 1 WHERE event_id = %s",
                            (event_id,),
                        )
                conn.commit()
            return True
        except Exception as e:
            logging.error("mark_alert_as_read 오류: %s", e)
            return False

    def get_dashboard_sensors_by_space_id(self, space_id: int) -> list[dict]:
        """space_id 기준 센서 목록 (camera/cctv/rtsp 계열 제외)."""
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
                            s.sen_locate,
                            s.model,
                            s.mqtt_topic,
                            s.is_online,
                            s.last_seen_at,
                            s.space_id
                        FROM sensor s
                        WHERE s.space_id = %s
                          AND LOWER(s.sensor_type) NOT LIKE '%%camera%%'
                          AND LOWER(s.sensor_type) NOT LIKE '%%cctv%%'
                          AND LOWER(s.sensor_type) NOT LIKE '%%rtsp%%'
                        ORDER BY s.sen_name
                        """,
                        (space_id,),
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        if row.get("last_seen_at") and hasattr(row["last_seen_at"], "strftime"):
                            row["last_seen_at"] = row["last_seen_at"].strftime("%Y-%m-%d %H:%M:%S")
                    return rows
        except Exception as e:
            logging.error("get_dashboard_sensors_by_space_id 오류: %s", e)
            return []

    def get_dashboard_cctvs_by_space_id(self, space_id: int) -> list[dict]:
        """space_id 기준 CCTV 목록 (camera_info + sensor JOIN)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            ci.sen_id,
                            ci.ip_address,
                            ci.camera_id,
                            ci.health,
                            COALESCE(ci.space_id, s.space_id) AS space_id,
                            s.sen_name,
                            s.sensor_id,
                            s.is_online
                        FROM camera_info ci
                        LEFT JOIN sensor s ON ci.sen_id = s.sen_id
                        WHERE COALESCE(ci.space_id, s.space_id) = %s
                        ORDER BY s.sen_name
                        """,
                        (space_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_dashboard_cctvs_by_space_id 오류: %s", e)
            return []

    def get_dashboard_workers_by_space_id(self, space_id: int) -> list[dict]:
        """worker.sen_id -> sensor.sen_id -> sensor.space_id 기준 작업자 목록.
        worker 테이블에 space_id가 없으므로 센서 매핑 기준으로 집계."""
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
                            s.sen_name AS sensor_name,
                            s.sensor_type,
                            s.space_id
                        FROM worker w
                        JOIN sensor s ON w.sen_id = s.sen_id
                        WHERE s.space_id = %s
                          AND w.is_manager = 0
                        ORDER BY w.name
                        """,
                        (space_id,),
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_dashboard_workers_by_space_id 오류: %s", e)
            return []

    def get_dashboard_summary_by_space_id(self, space_id: int) -> Optional[dict]:
        """space_id 기준 대시보드 요약 데이터를 반환한다."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # 공간 정보
                    cursor.execute(
                        """
                        SELECT space_id, space_name, hazard_type, is_hazard
                        FROM ds_space
                        WHERE space_id = %s
                        LIMIT 1
                        """,
                        (space_id,),
                    )
                    space = cursor.fetchone()

                    # 활성 Jetson 정보 (가장 최근 등록 기준)
                    cursor.execute(
                        """
                        SELECT jetson_id, jetson_wp
                        FROM jetson
                        WHERE space_id = %s AND jetson_status = 1
                        ORDER BY jetson_id DESC
                        LIMIT 1
                        """,
                        (space_id,),
                    )
                    jetson = cursor.fetchone()

                    # 센서 수 (camera/cctv/rtsp 계열 제외)
                    # 주의: pymysql cursor.execute()에서 SQL LIKE의 %는 %%로 이스케이프 필요
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS cnt FROM sensor
                        WHERE space_id = %s
                          AND LOWER(sensor_type) NOT LIKE '%%camera%%'
                          AND LOWER(sensor_type) NOT LIKE '%%cctv%%'
                          AND LOWER(sensor_type) NOT LIKE '%%rtsp%%'
                        """,
                        (space_id,),
                    )
                    sensor_total = cursor.fetchone()["cnt"]

                    # CCTV 수 — camera_info.space_id 우선, NULL이면 sensor.space_id로 보완
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM camera_info ci
                        LEFT JOIN sensor s ON ci.sen_id = s.sen_id
                        WHERE COALESCE(ci.space_id, s.space_id) = %s
                        """,
                        (space_id,),
                    )
                    cctv_total = cursor.fetchone()["cnt"]

                    # 작업자 수 — worker 테이블에 space_id가 없으므로
                    # worker.sen_id -> sensor.sen_id -> sensor.space_id 기준으로 집계
                    # sen_id가 NULL인 작업자(미배정)는 해당 공간 카운트에서 제외
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM worker w
                        JOIN sensor s ON w.sen_id = s.sen_id
                        WHERE s.space_id = %s
                          AND w.is_manager = 0
                        """,
                        (space_id,),
                    )
                    worker_total = cursor.fetchone()["cnt"]

                    cursor.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM alert_event
                        WHERE space_id = %s
                          AND is_read = 0
                        """,
                        (space_id,),
                    )
                    danger_alert_count = cursor.fetchone()["cnt"]

                    return {
                        "space_id": space_id,
                        "space_name": space["space_name"] if space else None,
                        "jetson_id": jetson["jetson_id"] if jetson else None,
                        "jetson_name": jetson["jetson_wp"] if jetson else None,
                        "danger_alert_count": danger_alert_count,
                        "sensor_total": sensor_total,
                        "cctv_total": cctv_total,
                        "worker_total": worker_total,
                    }

        except Exception as e:
            logging.error("get_dashboard_summary_by_space_id 오류: %s", e)
            return None

    # ------------------------------------------------------------------
    # ds_space (공정/구역) CRUD
    # ------------------------------------------------------------------

    def get_all_spaces(self) -> list[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT space_id, space_name, hazard_type, is_hazard
                        FROM ds_space
                        ORDER BY space_id
                        """
                    )
                    return cursor.fetchall()
        except Exception as e:
            logging.error("get_all_spaces 오류: %s", e)
            return []

    def get_space_by_id(self, space_id: int) -> Optional[dict]:
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT space_id, space_name, hazard_type, is_hazard FROM ds_space WHERE space_id = %s",
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
