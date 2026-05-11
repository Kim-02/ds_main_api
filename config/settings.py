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

    # ── MQTT 센서 데이터 토픽 ────────────────────────────────────────────────
    # sensor_service.py 가 구독할 추가 토픽 패턴 목록.
    # 기존 sensors/+/status, sensors/+/telemetry 는 코드에 고정 구독됩니다.
    # 이 설정은 향후 동적 구독 확장 시 참고용으로 사용합니다.
    mqtt_sensor_data_topics: list = ["sensor/+/+", "sensors/+/data"]

    # ── Fire pipeline (CCTV 화재 감지 전용) ─────────────────────────────────
    #
    # [추가 이유]
    # cctv/api/service.py 의 build_rtsp_url() 이 settings.fire_pipeline_rtsp_path 를
    # 참조하는데, 이 항목이 Settings 클래스에 없어 AttributeError 가 발생했습니다.
    # CCTV 등록 API(POST /api/v1/cctv/cameras/register) 호출 시 500 에러의 원인입니다.
    #
    # [각 항목 설명]
    # fire_pipeline_enabled      : True 이면 카메라 등록 시 fire pipeline 자동 시작
    # fire_pipeline_yolo_model_path : 화재 감지 전용 YOLO 모델 파일 경로
    # fire_pipeline_vllm_base_url   : fire pipeline 전용 vLLM 서버 주소
    #                                 비워두면 공통 vllm_base_url 을 사용합니다.
    # fire_pipeline_vllm_model      : fire pipeline 전용 vLLM 모델 이름
    #                                 비워두면 공통 vllm_model 을 사용합니다.
    # fire_pipeline_vllm_api_key    : fire pipeline 전용 vLLM API 키
    #                                 비워두면 공통 vllm_api_key 를 사용합니다.
    # fire_pipeline_rtsp_path       : IP 카메라 등록 시 RTSP URL 에 붙이는 경로
    #                                 카메라 브랜드마다 경로가 다르므로 .env 로 조정하세요.
    #   예) Hikvision : /Streaming/Channels/101
    #       Dahua     : /cam/realmonitor?channel=1&subtype=0
    #       일반(기본) : /stream
    fire_pipeline_enabled: bool = True
    fire_pipeline_yolo_model_path: str = "/media/ds/DATA/yolo_final/0507_best.engine"
    fire_pipeline_vllm_base_url: str = ""
    fire_pipeline_vllm_model: str = ""
    fire_pipeline_vllm_api_key: str = ""
    fire_pipeline_rtsp_path: str = "/stream1"


settings = Settings()
