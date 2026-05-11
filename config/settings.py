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
    vllm_base_url: str = "http://localhost:1111/v1"
    vllm_model: str = "/media/ds/DATA/models/Qwen2.5-VL-3B"
    vllm_api_key: str = "test-key"
    vllm_autostart: bool = True
    vllm_venv_activate: str = "~/vllm-jetson-v092-torch27/bin/activate"
    vllm_workdir: str = "~/vllm-0.9.2-src"
    vllm_serve_model_path: str = "/media/ds/DATA/models/Qwen2.5-VL-3B"
    vllm_host: str = "0.0.0.0"
    vllm_port: int = 1111
    vllm_dtype: str = "float16"
    vllm_gpu_memory_utilization: float = 0.5
    vllm_max_model_len: int = 4096
    vllm_max_num_seqs: int = 3
    vllm_max_num_batched_tokens: int = 4096
    vllm_limit_mm_per_prompt: str = "image=1,video=0"
    vllm_startup_timeout_seconds: int = 600
    vllm_startup_poll_seconds: float = 3.0
    vllm_log_path: str = "logs/vllm_server.log"
    vllm_progress_path: str = "logs/vllm_startup_progress.json"

    # Camera / YOLO
    frame_buffer_minutes: int = 10
    frame_buffer_seconds: int = 10
    yolo_model_path: str = "0507_best.engine"
    yolo_confidence: float = 0.5
    yolo_preload_on_startup: bool = True
    yolo_warmup_on_startup: bool = True
    cctv_rtsp_url: str = ""

    # Fire pipeline (CCTV 화재 감지 전용)
    fire_pipeline_enabled: bool = True
    fire_pipeline_yolo_model_path: str = "0507_best.engine"
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
