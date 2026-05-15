"""CCTV 영상 → YOLO → VLM 안전 분석 파이프라인.

export_final/pipeline.py 기반, 임포트 경로를 프로젝트 구조에 맞게 수정.
"""
import os
import queue
import logging
import threading

from ai.vlm.fire_pipeline.detector import YoloDetector, get_count_text, has_any_class
from ai.vlm.fire_pipeline.normalizer import WindowNormalizer
from ai.vlm.fire_pipeline.prompt_builder import make_compact_vlm_text
from ai.vlm.fire_pipeline.storage import RollingStorage
from ai.vlm.fire_pipeline.video_source import VideoSource
from ai.vlm.analysis import SafetyAnalysisVlm
from ai.vlm.validator import MovementValidator

logger = logging.getLogger(__name__)


def _put_latest(q, item):
    if q.full():
        try:
            q.get_nowait()
            q.task_done()
        except queue.Empty:
            pass
    q.put(item)


class VlmPipeline:
    def __init__(self, config, on_result=None, detector_factory=None):
        self.config = config
        self.on_result = on_result
        self.detector_factory = detector_factory

        self.running = True
        self.yolo_queue = queue.Queue(maxsize=config.queue_size)
        self.validation_queue = queue.Queue(maxsize=1)
        self.analysis_queue = queue.Queue(maxsize=1)

        self.abnormal_active = False
        self.abnormal_missing_count = 0
        self.request_number = 1
        self.vlm_cycle_busy = False
        self.vlm_cycle_lock = threading.Lock()

        self.latest_result = ""
        self.latest_summary = None
        self.latest_lock = threading.Lock()

        self.storage = RollingStorage(
            config.frame_folder,
            config.summary_folder,
            config.frame_keep_count,
            config.normalized_keep_count,
        )
        self.normalizer = WindowNormalizer(
            config.vlm_frame_count,
            config.normalized_keep_count,
            config.danger_classes,
        )

    def start(self):
        if self.config.clean_on_start:
            self.storage.clean_start_files()
            self._clean_validation_images()

        yolo_thread = threading.Thread(target=self._yolo_worker, name="vlm-yolo", daemon=True)
        validation_thread = threading.Thread(target=self._validation_worker, name="vlm-validation", daemon=True)
        analysis_thread = threading.Thread(target=self._analysis_worker, name="vlm-analysis", daemon=True)

        yolo_thread.start()
        validation_thread.start()
        analysis_thread.start()

        try:
            self._capture_loop()
        finally:
            self.running = False
            self.yolo_queue.put(None)
            yolo_thread.join()
            self.validation_queue.put(None)
            validation_thread.join()
            self.analysis_queue.put(None)
            analysis_thread.join()

    def _clean_validation_images(self):
        if not os.path.exists(self.config.summary_folder):
            return
        for name in os.listdir(self.config.summary_folder):
            if name.startswith("validation_compare_"):
                os.remove(os.path.join(self.config.summary_folder, name))

    def _capture_loop(self):
        video = VideoSource(self.config.video_source)

        if not video.open():
            raise RuntimeError("영상을 열 수 없습니다: " + str(self.config.video_source))

        next_sample_time = 0
        frame_number = 1

        print(f"[VlmPipeline] 캡처 시작 source={self.config.video_source}")

        try:
            while self.running:
                ret, frame = video.read()

                if not ret:
                    break

                current_time = video.get_time()

                if current_time >= next_sample_time:
                    _put_latest(self.yolo_queue, (frame_number, current_time, frame.copy()))
                    frame_number += 1
                    next_sample_time += self.config.sample_gap
        finally:
            video.release()
            print("[VlmPipeline] 캡처 종료")

    # ── YOLO ─────────────────────────────────────────────────────────────────

    def _make_detector(self):
        if self.detector_factory is not None:
            return self.detector_factory()
        return YoloDetector(
            self.config.model_path,
            detect_classes=self.config.detect_classes,
            confidence=self.config.yolo_confidence,
        )

    def _yolo_worker(self):
        detector = self._make_detector()

        while True:
            item = self.yolo_queue.get()

            if item is None:
                self.yolo_queue.task_done()
                break

            frame_number, frame_time, frame = item
            detections, _analyzed = detector.detect(frame)
            frame_path = self.storage.save_frame(frame_number, frame)
            self._handle_yolo_result(frame_number, frame_time, frame_path, detections)
            self.yolo_queue.task_done()

    def _handle_yolo_result(self, frame_number, frame_time, frame_path, detections):
        trigger_now = has_any_class(detections, self.config.trigger_classes)
        immediate_now = has_any_class(detections, self.config.immediate_trigger_classes)

        frame_data = {
            "frame_number": frame_number,
            "time": round(frame_time, 2),
            "image_path": frame_path,
            "detections": detections,
        }

        completed_summary = self.normalizer.add_frame(frame_data)

        if completed_summary is not None:
            self._set_latest_summary(completed_summary)

        self._update_abnormal_state(trigger_now, frame_time)

        if immediate_now:
            summary, history = self.normalizer.make_history_with_live_summary(completed_summary)
            self._submit_vlm_if_idle(summary, history, frame_time)

    def _update_abnormal_state(self, trigger_now, frame_time):
        if not self.abnormal_active and trigger_now:
            self.abnormal_active = True
            self.abnormal_missing_count = 0
            print(f"[VlmPipeline] 이상상황 감지 t={round(frame_time, 2)}s")

        if not self.abnormal_active:
            return

        if trigger_now:
            self.abnormal_missing_count = 0
        else:
            self.abnormal_missing_count += 1

        if self.abnormal_missing_count >= self.config.abnormal_missing_limit:
            self.abnormal_active = False
            self.abnormal_missing_count = 0
            print(f"[VlmPipeline] 이상상황 종료 t={round(frame_time, 2)}s")

    # ── VLM 요청 ─────────────────────────────────────────────────────────────

    def _submit_vlm_if_idle(self, summary, history, frame_time):
        if summary is None:
            return False

        with self.vlm_cycle_lock:
            if self.vlm_cycle_busy:
                return False
            self.vlm_cycle_busy = True
            request_number = self.request_number
            self.request_number += 1

        self._set_latest_summary(summary)
        self.validation_queue.put((request_number, summary, list(history)))
        print(f"[VlmPipeline] VLM 분석 요청 #{request_number} t={round(frame_time, 2)}s")
        return True

    def _finish_vlm_cycle(self):
        with self.vlm_cycle_lock:
            self.vlm_cycle_busy = False

    # ── Validation ───────────────────────────────────────────────────────────

    def _validation_worker(self):
        validator = MovementValidator(self.config)

        while True:
            item = self.validation_queue.get()

            if item is None:
                self.validation_queue.task_done()
                break

            request_number, summary, history = item
            validation = validator.validate(summary, request_number, history)
            summary["vlm_validation"] = validation
            summary["comparison_image_path"] = validation.get("comparison_image_path", "")
            self._set_latest_summary(summary)

            self.analysis_queue.put((request_number, summary, history, validation))
            self.validation_queue.task_done()

    # ── Analysis ─────────────────────────────────────────────────────────────

    def _analysis_worker(self):
        analyzer = SafetyAnalysisVlm(self.config)

        while True:
            item = self.analysis_queue.get()

            if item is None:
                self.analysis_queue.task_done()
                break

            request_number, summary, history, validation = item

            try:
                normalized_text = make_compact_vlm_text(history)
                self.storage.save_summary(request_number, summary, normalized_text)

                image_path = summary.get("comparison_image_path", "") or self._get_latest_image(summary)
                answer = analyzer.analyze(normalized_text, validation, image_path, None)
                self._emit_result(answer)
            except Exception as error:
                print(f"[VlmPipeline] 안전 분석 실패: {error}")
            finally:
                self.analysis_queue.task_done()
                self._finish_vlm_cycle()

    def _emit_result(self, answer):
        self._set_latest_result(answer)
        logger.info("[VLM TEXT] event_type=legacy_cctv_vlm text=%s", answer)

        if self.on_result is not None:
            self.on_result(answer)

        if self.config.print_result:
            print(answer, flush=True)

    # ── 상태 관리 ─────────────────────────────────────────────────────────────

    def _set_latest_summary(self, summary):
        with self.latest_lock:
            self.latest_summary = summary

    def _set_latest_result(self, text):
        with self.latest_lock:
            self.latest_result = text

    def get_latest_result(self):
        with self.latest_lock:
            return self.latest_result

    def _get_latest_image(self, summary):
        for frame in reversed(summary.get("frames", [])):
            if "image_path" in frame:
                return frame["image_path"]
        return ""
