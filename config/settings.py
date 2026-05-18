from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
    vllm_gpu_memory_utilization: float = 0.7
    vllm_max_model_len: int = 4096
    vllm_max_num_seqs: int = 4
    vllm_max_num_batched_tokens: int = 8192
    vllm_limit_mm_per_prompt: str = "image=1,video=0"
    vllm_startup_timeout_seconds: int = 600
    vllm_startup_poll_seconds: float = 3.0
    vllm_log_path: str = "logs/vllm_server.log"
    vllm_progress_path: str = "logs/vllm_startup_progress.json"
    vlm_timeshare_max_concurrent: int = 2
    vlm_timeshare_cooldown_seconds: float = 0.1

    # Camera / YOLO
    frame_buffer_minutes: int = 10
    frame_buffer_seconds: int = 10
    frame_buffer_sample_interval_seconds: float = 0.5
    rtsp_reconnect_delay_seconds: float = 2.0
    yolo_model_path: str = "0507_best.engine"
    yolo_confidence: float = 0.5
    yolo_preload_on_startup: bool = True
    yolo_warmup_on_startup: bool = True
    cctv_rtsp_url: str = ""
    cctv_vlm_yolo_context_seconds: float = 10.0
    cctv_vlm_yolo_context_max_frames: int = 10
    cctv_vlm_yolo_context_frame_dir: str = "cctv_vlm_yolo_frames"
    cctv_vlm_yolo_context_saved_frames_keep_count: int = 240
    cctv_vlm_yolo_context_prompt_max_chars: int = 420
    cctv_vlm_request_image_max_side: int = 512

    # Fire pipeline (CCTV 화재 감지 전용)
    fire_pipeline_enabled: bool = False
    fire_pipeline_yolo_model_path: str = "0507_best.engine"
    fire_pipeline_vllm_base_url: str = ""
    fire_pipeline_vllm_model: str = ""
    fire_pipeline_vllm_api_key: str = ""
    fire_pipeline_rtsp_path: str = "/stream1"
    fire_pipeline_sample_gap_seconds: float = 0.5
    fire_pipeline_vlm_gap_seconds: float = 10.0
    fire_pipeline_buffer_startup_wait_seconds: float = 5.0
    fire_pipeline_buffer_recent_seconds: float = 1.5
    fire_pipeline_buffer_empty_sleep_seconds: float = 0.1
    fire_pipeline_buffer_duplicate_sleep_seconds: float = 0.05
    fire_pipeline_buffer_wait_log_interval_seconds: float = 30.0
    fire_pipeline_normalized_keep_count: int = 3
    fire_pipeline_queue_size: int = 3
    fire_pipeline_abnormal_missing_limit: int = 4
    fire_pipeline_validation_compare_height: int = 360
    fire_pipeline_vllm_timeout_seconds: int = 30
    fire_pipeline_validation_max_tokens: int = 128
    fire_pipeline_validation_prompt_max_chars: int = 2600
    fire_pipeline_analysis_max_tokens: int = 128
    fire_pipeline_analysis_prompt_max_chars: int = 3000

    # Detection defaults
    default_temp_threshold: float = 35.0
    default_humidity_threshold: float = 80.0
    default_heartrate_threshold: int = 120
    sensor_register_interval_ms: int = 5000
    watch_pipeline_interval_seconds: int = 30
    watch_hr_stale_seconds: int = 15
    watch_alert_duration_ms: int = 5000
    watch_alert_reset_after_ms: int = 5000
    watch_camera_vlm_session_seconds: int = 120
    watch_camera_vlm_analysis_interval_seconds: int = 30
    camera_vlm_request_timeout_seconds: int = 90
    camera_vlm_prompt_max_chars: int = 5600
    camera_vlm_retry_prompt_max_chars: int = 1100
    camera_vlm_max_tokens: int = 256
    rest_forced_rest_work_minutes: int = 120
    rest_default_age: int = 40
    rest_default_gender: int = 1
    rest_default_height_cm: float = 170.0
    rest_default_weight_kg: float = 70.0
    rest_default_work_duration_min: int = 0
    temperature_vlm_threshold: float = 38.0
    temperature_vlm_check_interval_seconds: int = 10
    temperature_vlm_session_seconds: int = 120
    temperature_vlm_analysis_interval_seconds: int = 30
    temperature_vlm_stale_seconds: int = 60

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
    log_profile: str = "default"

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
