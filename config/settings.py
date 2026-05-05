from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    jetson_db_url: str = "sqlite+aiosqlite:///./jetson.db"
    corporate_db_url: str = ""

    # MQTT
    mqtt_broker_host: str = "localhost"
    mqtt_broker_port: int = 1883
    mqtt_username: str = ""
    mqtt_password: str = ""

    # vLLM
    vllm_base_url: str = "http://localhost:8000/v1"
    vllm_model: str = "Qwen/Qwen2.5-VL-7B-Instruct"
    vllm_api_key: str = "token-none"

    # Camera / YOLO
    frame_buffer_minutes: int = 10
    yolo_model_path: str = "yolov8n.pt"
    yolo_confidence: float = 0.5

    # Detection defaults
    default_temp_threshold: float = 35.0
    default_heartrate_threshold: int = 120

    # Firebase
    firebase_credentials_path: str = "firebase-credentials.json"

    # DB Sync
    db_sync_interval_seconds: int = 300

    # MariaDB (기존 Jetson DB)
    mariadb_host: str = "127.0.0.1"
    mariadb_user: str = "myuser"
    mariadb_password: str = "mypassword"
    mariadb_db_name: str = "mydb"
    mariadb_port: int = 3306

    # mDNS Jetson 자기 방송
    mdns_service_name: str = "DS_Safer_Jetson._jetsonhub._tcp.local."
    mdns_service_type: str = "_jetsonhub._tcp.local."

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    debug: bool = False


settings = Settings()
