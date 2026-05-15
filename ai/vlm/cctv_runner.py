"""CCTV RTSP 스트림에 autoregressive VLM 파이프라인을 연결하고 라이프사이클을 관리한다."""
import logging
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class CctvVlmRunner:
    """autoregressive VLM 파이프라인을 백그라운드 스레드로 실행하고 결과를 콜백으로 전달한다."""

    def __init__(self, rtsp_url: str, on_result: Callable[[str], None]):
        self.rtsp_url = rtsp_url
        self.on_result = on_result
        self._thread: Optional[threading.Thread] = None
        self._pipeline = None

    def start(self):
        from ai.vlm.config import VlmPipelineConfig
        from ai.vlm.pipeline import VlmPipeline

        config = VlmPipelineConfig(rtsp_url=self.rtsp_url)
        self._pipeline = VlmPipeline(config, on_result=self.on_result)

        self._thread = threading.Thread(
            target=self._pipeline.start,
            name="cctv-vlm-pipeline",
            daemon=True,
        )
        self._thread.start()
        logger.info("[CctvVlm] autoregressive VLM 파이프라인 시작 source=%s", self.rtsp_url)

    def stop(self):
        if self._pipeline is not None:
            self._pipeline.running = False
        logger.info("[CctvVlm] autoregressive VLM 파이프라인 종료")
