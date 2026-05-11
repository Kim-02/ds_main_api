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
        self.sample_gap = 0.5                                        # 초
        self.vlm_gap = 10                                            # 초
        self.vlm_frame_count = int(self.vlm_gap / self.sample_gap)
        self.normalized_keep_count = 3
        self.frame_keep_count = self.vlm_frame_count * 2
        self.queue_size = 3

        # YOLO는 person/fire/smoke만 사용하고, 위험 판단은 fire/smoke만 사용
        self.detect_classes = ["person", "fire", "smoke"]
        self.danger_classes = ["fire", "smoke"]
        self.trigger_classes = ["fire", "smoke"]
        self.immediate_trigger_classes = ["fire", "smoke"]
        self.yolo_confidence = settings.yolo_confidence
        self.abnormal_missing_limit = 4

        # 기타
        self.clean_on_start = True
        self.print_status = False
        self.print_result = False         # 결과는 on_result 콜백으로 전달
        self.validation_compare_height = 360

        # vLLM 연결 정보
        self.vllm_base_url = settings.vllm_base_url
        self.vllm_api_key = settings.vllm_api_key
        self.vllm_model = settings.vllm_model
        self.vllm_timeout = 30

        # VLM 파라미터
        self.validation_vlm_max_tokens = 128
        self.validation_vlm_temperature = 0.0
        self.validation_prompt_max_chars = 2600

        self.analysis_vlm_max_tokens = 128
        self.analysis_vlm_temperature = 0.2
        self.analysis_prompt_max_chars = 3000
        self.analysis_stream = True
        self.typing_fallback_delay = 0.0
