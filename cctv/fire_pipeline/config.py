"""Fire pipeline 설정 — export_final.ExportFinalConfig를 DS_MAIN_API에 맞게 적용."""


class FirePipelineConfig:
    """카메라별 fire pipeline 설정.

    rtsp_url: 카메라 등록 시 빌드된 RTSP 주소 (예: rtsp://user:pw@192.168.1.1/stream)
    model_path: 화재 감지 전용 YOLO 모델 경로 (.env의 fire_pipeline_yolo_model_path 또는 기본값)
    """

    def __init__(self, rtsp_url: str = "", model_path: str = ""):
        # 설정 파일에서 기본값을 읽어옴 — import 오류 방지를 위해 지연 import
        try:
            from config import settings as _s

            _vllm_base_url = getattr(_s, "fire_pipeline_vllm_base_url", "") or _s.vllm_base_url
            _vllm_api_key = getattr(_s, "fire_pipeline_vllm_api_key", "") or _s.vllm_api_key
            _vllm_model = getattr(_s, "fire_pipeline_vllm_model", "") or _s.vllm_model
            _model_path = getattr(_s, "fire_pipeline_yolo_model_path", "0507_best.pt")
            _yolo_confidence = getattr(_s, "yolo_confidence", 0.5)
            _sample_gap = getattr(_s, "fire_pipeline_sample_gap_seconds", 0.5)
            _vlm_gap = getattr(_s, "fire_pipeline_vlm_gap_seconds", 10.0)
            _buffer_startup_wait = getattr(_s, "fire_pipeline_buffer_startup_wait_seconds", 5.0)
            _buffer_recent_seconds = getattr(_s, "fire_pipeline_buffer_recent_seconds", 1.5)
            _buffer_empty_sleep_seconds = getattr(_s, "fire_pipeline_buffer_empty_sleep_seconds", 0.1)
            _buffer_duplicate_sleep_seconds = getattr(_s, "fire_pipeline_buffer_duplicate_sleep_seconds", 0.05)
            _buffer_wait_log_interval_seconds = getattr(_s, "fire_pipeline_buffer_wait_log_interval_seconds", 30.0)
            _normalized_keep_count = getattr(_s, "fire_pipeline_normalized_keep_count", 3)
            _queue_size = getattr(_s, "fire_pipeline_queue_size", 3)
            _abnormal_missing_limit = getattr(_s, "fire_pipeline_abnormal_missing_limit", 4)
            _validation_compare_height = getattr(_s, "fire_pipeline_validation_compare_height", 360)
            _vllm_timeout = getattr(_s, "fire_pipeline_vllm_timeout_seconds", 30)
            _validation_max_tokens = getattr(_s, "fire_pipeline_validation_max_tokens", 128)
            _validation_prompt_max_chars = getattr(_s, "fire_pipeline_validation_prompt_max_chars", 2600)
            _analysis_max_tokens = getattr(_s, "fire_pipeline_analysis_max_tokens", 128)
            _analysis_prompt_max_chars = getattr(_s, "fire_pipeline_analysis_prompt_max_chars", 3000)
        except Exception:
            _vllm_base_url = "http://localhost:1111/v1"
            _vllm_api_key = "test-key"
            _vllm_model = "/media/ds/DATA/models/Qwen2.5-VL-3B"
            _model_path = "0507_best.engine"
            _yolo_confidence = 0.5
            _sample_gap = 0.5
            _vlm_gap = 10.0
            _buffer_startup_wait = 5.0
            _buffer_recent_seconds = 1.5
            _buffer_empty_sleep_seconds = 0.1
            _buffer_duplicate_sleep_seconds = 0.05
            _buffer_wait_log_interval_seconds = 30.0
            _normalized_keep_count = 3
            _queue_size = 3
            _abnormal_missing_limit = 4
            _validation_compare_height = 360
            _vllm_timeout = 30
            _validation_max_tokens = 128
            _validation_prompt_max_chars = 2600
            _analysis_max_tokens = 128
            _analysis_prompt_max_chars = 3000

        self.video_source = rtsp_url
        self.camera_id = None
        self.model_path = model_path or _model_path

        # 프레임·요약 저장 폴더 — 기존 data/frames/ 와 분리
        self.frame_folder = "cctv_fire_frames"
        self.summary_folder = "cctv_fire_summaries"

        self.sample_gap = float(_sample_gap)
        self.buffer_startup_wait_seconds = float(_buffer_startup_wait)
        self.buffer_recent_seconds = float(_buffer_recent_seconds)
        self.buffer_empty_sleep_seconds = float(_buffer_empty_sleep_seconds)
        self.buffer_duplicate_sleep_seconds = float(_buffer_duplicate_sleep_seconds)
        self.buffer_wait_log_interval_seconds = float(_buffer_wait_log_interval_seconds)
        self.vlm_gap = float(_vlm_gap)
        self.vlm_frame_count = int(self.vlm_gap / self.sample_gap)
        self.normalized_keep_count = int(_normalized_keep_count)
        self.frame_keep_count = self.vlm_frame_count * 2
        self.queue_size = int(_queue_size)

        self.detect_classes = ["person", "fire", "smoke"]
        self.danger_classes = ["fire", "smoke"]
        self.trigger_classes = ["fire", "smoke"]
        self.immediate_trigger_classes = ["fire", "smoke"]
        self.yolo_confidence = _yolo_confidence
        self.abnormal_missing_limit = int(_abnormal_missing_limit)

        # API 통합 시 기존 파일 유지 (clean_on_start=False)
        # 결과 출력은 on_result 콜백으로 처리 (print_result=False)
        self.clean_on_start = False
        self.print_status = False
        self.print_result = False

        self.validation_compare_height = int(_validation_compare_height)

        # vLLM 설정 — .env의 fire_pipeline_vllm_* 키 우선, 없으면 공통 vllm_* 사용
        # CONFLICT 주의: 공통 vLLM 서버와 fire pipeline vLLM 서버가 다를 경우
        #   .env에 fire_pipeline_vllm_base_url / fire_pipeline_vllm_model 를 별도로 설정하세요.
        self.vllm_base_url = _vllm_base_url
        self.vllm_api_key = _vllm_api_key
        self.vllm_model = _vllm_model
        self.vllm_timeout = int(_vllm_timeout)

        self.validation_vlm_max_tokens = int(_validation_max_tokens)
        self.validation_vlm_temperature = 0.0
        self.validation_prompt_max_chars = int(_validation_prompt_max_chars)

        self.analysis_vlm_max_tokens = int(_analysis_max_tokens)
        self.analysis_vlm_temperature = 0.2
        self.analysis_prompt_max_chars = int(_analysis_prompt_max_chars)
        self.analysis_stream = True
        self.typing_fallback_delay = 0.0
