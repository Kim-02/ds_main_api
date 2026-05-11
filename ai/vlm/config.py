from config import settings


class VlmPipelineConfig:
    """프로젝트 settings를 기반으로 VLM 파이프라인 설정을 구성한다."""

    def __init__(self, rtsp_url: str = ""):
        # 영상 소스 (RTSP URL 또는 파일 경로)
        self.video_source = rtsp_url

        # YOLO 모델
        self.model_path = settings.yolo_model_path

        # 프레임/요약 저장 폴더
        self.frame_folder = "vlm_frames"
        self.summary_folder = "vlm_summaries"

        # 샘플링 간격
        self.sample_gap = settings.fire_pipeline_sample_gap_seconds
        self.vlm_gap = settings.fire_pipeline_vlm_gap_seconds
        self.vlm_frame_count = int(self.vlm_gap / self.sample_gap)
        self.normalized_keep_count = settings.fire_pipeline_normalized_keep_count
        self.frame_keep_count = self.vlm_frame_count * 2
        self.queue_size = settings.fire_pipeline_queue_size

        # YOLO는 person/fire/smoke만 사용하고, 위험 판단은 fire/smoke만 사용
        self.detect_classes = ["person", "fire", "smoke"]
        self.danger_classes = ["fire", "smoke"]
        self.trigger_classes = ["fire", "smoke"]
        self.immediate_trigger_classes = ["fire", "smoke"]
        self.yolo_confidence = settings.yolo_confidence
        self.abnormal_missing_limit = settings.fire_pipeline_abnormal_missing_limit

        # 기타
        self.clean_on_start = True
        self.print_status = False
        self.print_result = False         # 결과는 on_result 콜백으로 전달
        self.validation_compare_height = settings.fire_pipeline_validation_compare_height

        # vLLM 연결 정보
        self.vllm_base_url = settings.vllm_base_url
        self.vllm_api_key = settings.vllm_api_key
        self.vllm_model = settings.vllm_model
        self.vllm_timeout = settings.fire_pipeline_vllm_timeout_seconds

        # VLM 파라미터
        self.validation_vlm_max_tokens = settings.fire_pipeline_validation_max_tokens
        self.validation_vlm_temperature = 0.0
        self.validation_prompt_max_chars = settings.fire_pipeline_validation_prompt_max_chars

        self.analysis_vlm_max_tokens = settings.fire_pipeline_analysis_max_tokens
        self.analysis_vlm_temperature = 0.2
        self.analysis_prompt_max_chars = settings.fire_pipeline_analysis_prompt_max_chars
        self.analysis_stream = True
        self.typing_fallback_delay = 0.0
