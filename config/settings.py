from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    jetson_db_url: str = "mysql+pymysql://user:password@127.0.0.1:3306/ON_SAFE"
    corporate_db_url: str = ""

    # MQTT
    mqtt_broker_host: str = "localhost"
    mqtt_broker_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_sensor_data_topics: list = ["sensor/+/+", "sensors/+/data"]

    # vLLM
    vllm_base_url: str = "http://localhost:8000/v1"
    vllm_model: str = "Qwen/Qwen2.5-VL-7B-Instruct"
    vllm_api_key: str = "token-none"

    # Camera / YOLO
    frame_buffer_minutes: int = 10
    yolo_model_path: str = "yolov8n.pt"
    yolo_confidence: float = 0.5
    cctv_rtsp_url: str = ""

    # Fire pipeline (CCTV 화재 감지 전용)
    fire_pipeline_enabled: bool = True
    fire_pipeline_yolo_model_path: str = "0507_best.pt"
    fire_pipeline_vllm_base_url: str = ""
    fire_pipeline_vllm_model: str = ""
    fire_pipeline_vllm_api_key: str = ""
    fire_pipeline_rtsp_path: str = "/stream"

    # Detection defaults
    default_temp_threshold: float = 35.0
    default_heartrate_threshold: int = 120

    # Firebase
    firebase_credentials_path: str = "firebase-credentials.json"

    # DB Sync
    db_sync_interval_seconds: int = 300

    # MariaDB (기존 Jetson DB)
    mariadb_host: str = "127.0.0.1"
    mariadb_user: str = "root"
    mariadb_password: str = "ekthf123"
    mariadb_db_name: str = "ON_SAFE"
    mariadb_port: int = 3306

    # mDNS Jetson 자기 방송
    mdns_service_type: str = "_jetsonhub._tcp.local."
    mdns_service_name: str = "OnSafe Jetson"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    debug: bool = False

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug(cls, value):
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"release", "prod", "production", "false", "0", "no", "off"}:
                return False
            if normalized in {"debug", "dev", "development", "true", "1", "yes", "on"}:
                return True
        return value


settings = Settings()
