"""Fire detection pipeline — 카메라별 CCTV YOLO/autoregressive VLM 파이프라인."""
import os
import queue
import logging
import threading
import time

from ai.vlm.fire_pipeline.detector import YoloDetector, get_count_text, has_any_class
from ai.vlm.fire_pipeline.normalizer import WindowNormalizer
from ai.vlm.fire_pipeline.prompt_builder import make_compact_vlm_text
from ai.vlm.fire_pipeline.storage import RollingStorage
from ai.vlm.fire_pipeline.video_source import VideoSource

from cctv.fire_pipeline.analysis_vlm import SafetyAnalysisVlm
from cctv.fire_pipeline.movement_validator import MovementValidator

logger = logging.getLogger(__name__)


def put_latest(item_queue, item):
    if item_queue.full():
        try:
            item_queue.get_nowait()
            item_queue.task_done()
        except queue.Empty:
            pass

    item_queue.put(item)


class ExportFinalPipeline:
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
            config.normalized_keep_count
        )
        self.normalizer = WindowNormalizer(
            config.vlm_frame_count,
            config.normalized_keep_count,
            config.danger_classes
        )
        self.yolo_summary_interval_seconds = float(getattr(config, "yolo_summary_interval_seconds", 10.0))
        self.yolo_summary_started_at = time.monotonic()
        self.yolo_summary_frames = 0
        self.yolo_summary_fire_frames = 0
        self.yolo_summary_smoke_frames = 0
        self.yolo_summary_person_frames = 0
        self.yolo_summary_last_detections = "none"

    def start(self):
        if self.config.clean_on_start:
            self.storage.clean_start_files()
            self.clean_validation_images()

        camera_suffix = ""
        if self.config.camera_id is not None:
            camera_suffix = "-cam" + str(self.config.camera_id)

        yolo_thread = threading.Thread(target=self.yolo_worker, name="fire-pl-yolo" + camera_suffix)
        validation_thread = threading.Thread(
            target=self.validation_worker,
            name="fire-pl-validation-vlm" + camera_suffix,
        )
        analysis_thread = threading.Thread(
            target=self.analysis_worker,
            name="fire-pl-analysis-vlm" + camera_suffix,
        )

        logger.info(
            "FirePipeline started camera_id=%s source=%s model=%s sample_gap=%s trigger_classes=%s",
            self.config.camera_id,
            self.config.video_source,
            self.config.model_path,
            self.config.sample_gap,
            self.config.trigger_classes,
        )
        self.log_frame_buffer_status("before analysis")

        yolo_thread.start()
        validation_thread.start()
        analysis_thread.start()

        try:
            self.run_capture_loop()
        finally:
            self.running = False
            logger.info("FirePipeline stopped camera_id=%s", self.config.camera_id)
            self.yolo_queue.put(None)
            yolo_thread.join()

            self.validation_queue.put(None)
            validation_thread.join()

            self.analysis_queue.put(None)
            analysis_thread.join()

    def clean_validation_images(self):
        if os.path.exists(self.config.summary_folder) == False:
            return

        for file_name in os.listdir(self.config.summary_folder):
            if file_name.startswith("validation_compare_"):
                os.remove(os.path.join(self.config.summary_folder, file_name))

    def log_status(self, *items):
        if self.config.print_status:
            print(*items, flush=True)

    def run_capture_loop(self):
        if getattr(self.config, "camera_id", None) is not None:
            if self.run_buffer_capture_loop():
                return
            logger.warning(
                "[FirePipeline] frame buffer unavailable, direct video fallback camera_id=%s source=%s",
                self.config.camera_id,
                self.config.video_source,
            )

        video = VideoSource(self.config.video_source)

        if video.open() == False:
            raise RuntimeError("영상을 열 수 없습니다: " + str(self.config.video_source))

        next_sample_time = 0
        frame_number = 1

        self.log_status("fire_pipeline 시작:", self.config.video_source)
        self.log_status("YOLO 모델:", self.config.model_path)
        logger.info(
            "[FirePipeline] direct capture loop started camera_id=%s source=%s",
            self.config.camera_id,
            self.config.video_source,
        )

        try:
            while self.running:
                ret, frame = video.read()

                if ret == False:
                    break

                current_time = video.get_time()

                if current_time >= next_sample_time:
                    put_latest(
                        self.yolo_queue,
                        (frame_number, current_time, frame.copy())
                    )
                    if frame_number == 1 or frame_number % 20 == 0:
                        logger.debug(
                            "[FirePipeline] queued direct frame camera_id=%s frame=%s time=%.2f queue=%s",
                            self.config.camera_id,
                            frame_number,
                            current_time,
                            self.yolo_queue.qsize(),
                        )
                    frame_number = frame_number + 1
                    next_sample_time = next_sample_time + self.config.sample_gap
        finally:
            video.release()

    def run_buffer_capture_loop(self):
        try:
            from cctv.buffer import get_buffer_status, get_recent_frames
        except Exception as exc:
            self.log_status("[BUFFER] cctv.buffer 사용 불가, RTSP로 전환:", exc)
            return False

        camera_id = int(self.config.camera_id)
        startup_wait = float(getattr(self.config, "buffer_startup_wait_seconds", 5.0))
        started_at = time.monotonic()
        origin_timestamp = None
        last_timestamp = 0.0
        last_empty_log_time = 0.0
        next_sample_time = 0.0
        frame_number = 1

        self.log_status("fire_pipeline 버퍼 입력 시작:", camera_id)
        self.log_frame_buffer_status("capture loop start")

        while self.running:
            frames = get_recent_frames(
                camera_id,
                seconds=max(self.config.sample_gap * 3, self.config.buffer_recent_seconds)
            )
            if not frames:
                current_monotonic = time.monotonic()
                if (
                    current_monotonic - started_at >= startup_wait
                    and current_monotonic - last_empty_log_time
                    >= self.config.buffer_wait_log_interval_seconds
                ):
                    status = get_buffer_status(camera_id)
                    logger.warning(
                        "[FirePipeline] waiting for frame buffer camera_id=%s source=%s status=%s",
                        camera_id,
                        self.config.video_source,
                        status,
                    )
                    if (
                        status.get("latest_error") == "buffer_not_started"
                        and self._is_local_video_source()
                    ):
                        logger.warning(
                            "[FirePipeline] frame buffer not started for local video; "
                            "fallback to direct capture camera_id=%s source=%s",
                            camera_id,
                            self.config.video_source,
                        )
                        return False
                    last_empty_log_time = current_monotonic

                time.sleep(self.config.buffer_empty_sleep_seconds)
                continue

            latest = frames[-1]
            source_timestamp = float(latest["timestamp"])
            if source_timestamp <= last_timestamp:
                time.sleep(self.config.buffer_duplicate_sleep_seconds)
                continue

            last_timestamp = source_timestamp
            if origin_timestamp is None:
                origin_timestamp = source_timestamp

            current_time = source_timestamp - origin_timestamp
            if current_time >= next_sample_time:
                put_latest(
                    self.yolo_queue,
                    (frame_number, current_time, latest["frame"].copy())
                )
                if frame_number == 1 or frame_number % 20 == 0:
                    logger.debug(
                        "[FirePipeline] frame buffer consumed camera_id=%s frame=%s time=%.2f buffer_frames=%s queue=%s",
                        camera_id,
                        frame_number,
                        current_time,
                        len(frames),
                        self.yolo_queue.qsize(),
                    )
                frame_number = frame_number + 1
                next_sample_time = next_sample_time + self.config.sample_gap

            time.sleep(self.config.buffer_duplicate_sleep_seconds)

        return True

    def _is_local_video_source(self):
        source = str(self.config.video_source or "")
        if source.startswith("rtsp://"):
            return False
        return os.path.isfile(source)

    def set_latest_summary(self, summary):
        with self.latest_lock:
            self.latest_summary = summary

    def set_latest_result(self, text):
        with self.latest_lock:
            self.latest_result = text

    def get_latest_result(self):
        with self.latest_lock:
            return self.latest_result

    def make_detector(self):
        if self.detector_factory is not None:
            return self.detector_factory()

        return YoloDetector(
            self.config.model_path,
            detect_classes=self.config.detect_classes,
            confidence=self.config.yolo_confidence,
        )

    def log_frame_buffer_status(self, phase):
        if getattr(self.config, "camera_id", None) is None:
            return
        try:
            from cctv.buffer import get_buffer_status
            status = get_buffer_status(int(self.config.camera_id))
        except Exception as exc:
            status = {"error": str(exc)}
        logger.info(
            "[FirePipeline] frame buffer status %s camera_id=%s status=%s",
            phase,
            self.config.camera_id,
            status,
        )

    def record_yolo_summary(self, detections):
        self.yolo_summary_frames += 1
        if self._has_detection_class(detections, "fire"):
            self.yolo_summary_fire_frames += 1
        if self._has_detection_class(detections, "smoke"):
            self.yolo_summary_smoke_frames += 1
        if self._has_detection_class(detections, "person"):
            self.yolo_summary_person_frames += 1

        self.yolo_summary_last_detections = get_count_text(detections)
        elapsed = time.monotonic() - self.yolo_summary_started_at
        if elapsed >= self.yolo_summary_interval_seconds:
            logger.info(
                "[YOLO_SUMMARY] camera_id=%s interval=%.0fs frames=%s fire_frames=%s smoke_frames=%s person_frames=%s last_detections=%s",
                self.config.camera_id,
                elapsed,
                self.yolo_summary_frames,
                self.yolo_summary_fire_frames,
                self.yolo_summary_smoke_frames,
                self.yolo_summary_person_frames,
                self.yolo_summary_last_detections,
            )
            self.yolo_summary_started_at = time.monotonic()
            self.yolo_summary_frames = 0
            self.yolo_summary_fire_frames = 0
            self.yolo_summary_smoke_frames = 0
            self.yolo_summary_person_frames = 0
            self.yolo_summary_last_detections = "none"

    def _has_detection_class(self, detections, class_name):
        for detection in detections:
            if detection.get("class") == class_name:
                return True
        return False

    def yolo_worker(self):
        logger.info(
            "[YOLO] worker start camera_id=%s model=%s",
            self.config.camera_id,
            self.config.model_path,
        )
        try:
            detector = self.make_detector()
        except Exception:
            logger.exception(
                "[YOLO] worker failed during detector init camera_id=%s model=%s",
                self.config.camera_id,
                self.config.model_path,
            )
            self.running = False
            return
        logger.info(
            "[YOLO] worker ready camera_id=%s model=%s",
            self.config.camera_id,
            self.config.model_path,
        )

        while True:
            item = self.yolo_queue.get()

            if item is None:
                self.yolo_queue.task_done()
                break

            frame_number, frame_time, frame = item
            should_log_frame = frame_number == 1 or frame_number % 20 == 0
            if should_log_frame:
                logger.debug(
                    "[YOLO] inference start camera_id=%s frame=%s time=%.2f",
                    self.config.camera_id,
                    frame_number,
                    frame_time,
                )
            try:
                detections, _analyzed = detector.detect(frame)
            except Exception:
                logger.exception(
                    "[YOLO] inference failed camera_id=%s frame=%s time=%.2f",
                    self.config.camera_id,
                    frame_number,
                    frame_time,
                )
                self.yolo_queue.task_done()
                continue
            frame_path = self.storage.save_frame(frame_number, frame)
            count_text = get_count_text(detections)
            if should_log_frame or detections:
                logger.debug(
                    "[YOLO] inference end camera_id=%s frame=%s time=%.2f detections=%s count=%s",
                    self.config.camera_id,
                    frame_number,
                    frame_time,
                    count_text,
                    len(detections),
                )
            self.record_yolo_summary(detections)

            self.log_status(
                "[YOLO]",
                str(round(frame_time, 2)) + "s",
                count_text
            )

            self.handle_yolo_result(frame_number, frame_time, frame_path, detections)
            self.yolo_queue.task_done()

    def handle_yolo_result(self, frame_number, frame_time, frame_path, detections):
        trigger_now = has_any_class(detections, self.config.trigger_classes)
        immediate_now = has_any_class(detections, self.config.immediate_trigger_classes)

        frame_data = {
            "frame_number": frame_number,
            "time": round(frame_time, 2),
            "image_path": frame_path,
            "detections": detections
        }

        completed_summary = self.normalizer.add_frame(frame_data)

        if completed_summary is not None:
            self.set_latest_summary(completed_summary)

        self.update_abnormal_state(trigger_now, frame_time)

        if immediate_now:
            summary, history = self.normalizer.make_history_with_live_summary(completed_summary)
            self.submit_vlm_if_idle(summary, history, frame_time)
        elif frame_number == 1 or frame_number % 20 == 0 or detections:
            logger.debug(
                "[FirePipeline] VLM trigger skipped camera_id=%s frame=%s time=%.2f reason=no_fire_or_smoke detections=%s",
                self.config.camera_id,
                frame_number,
                frame_time,
                get_count_text(detections),
            )

    def submit_vlm_if_idle(self, summary, history, frame_time):
        if summary is None:
            logger.debug(
                "[FirePipeline] VLM trigger skipped camera_id=%s time=%.2f reason=normalized_summary_not_ready",
                self.config.camera_id,
                frame_time,
            )
            self.log_status("[VLM] 이상상황 감지, 사용할 정규화 정보 없음")
            return False

        with self.vlm_cycle_lock:
            if self.vlm_cycle_busy:
                logger.debug(
                    "[FirePipeline] VLM trigger skipped camera_id=%s time=%.2f reason=vlm_cycle_busy",
                    self.config.camera_id,
                    frame_time,
                )
                self.log_status("[VLM] 분석 중이라 새 요청 생략:", round(frame_time, 2), "초")
                return False

            self.vlm_cycle_busy = True
            request_number = self.request_number
            self.request_number = self.request_number + 1

        self.set_latest_summary(summary)
        self.validation_queue.put((request_number, summary, list(history)))
        logger.info(
            "[FirePipeline] VLM request start camera_id=%s request=%s time=%.2f history_frames=%s",
            self.config.camera_id,
            request_number,
            frame_time,
            len(history or []),
        )
        self.log_status("[VLM] 즉시 분석 요청 #" + str(request_number), "time=" + str(round(frame_time, 2)) + "s")
        return True

    def finish_vlm_cycle(self):
        with self.vlm_cycle_lock:
            self.vlm_cycle_busy = False

    def update_abnormal_state(self, trigger_now, frame_time):
        if self.abnormal_active == False and trigger_now:
            self.abnormal_active = True
            self.abnormal_missing_count = 0
            self.log_status("[상태] 이상상황 감지:", round(frame_time, 2), "초")

        if self.abnormal_active == False:
            return

        if trigger_now:
            self.abnormal_missing_count = 0
        else:
            self.abnormal_missing_count = self.abnormal_missing_count + 1

        if self.abnormal_missing_count >= self.config.abnormal_missing_limit:
            self.abnormal_active = False
            self.abnormal_missing_count = 0
            self.log_status("[상태] 이상상황 미검출:", round(frame_time, 2), "초")

    def validation_worker(self):
        validator = MovementValidator(self.config)

        while True:
            item = self.validation_queue.get()

            if item is None:
                self.validation_queue.task_done()
                break

            request_number, summary, history = item
            logger.info(
                "[FirePipeline] VLM validation request start camera_id=%s request=%s frames=%s",
                self.config.camera_id,
                request_number,
                len(history or []),
            )
            validation = validator.validate(summary, request_number, history)
            logger.info(
                "[FirePipeline] VLM validation response received camera_id=%s request=%s movement_valid=%s person_direction=%s fire_direction=%s smoke_direction=%s",
                self.config.camera_id,
                request_number,
                validation.get("movement_valid"),
                validation.get("person_direction"),
                validation.get("fire_direction"),
                validation.get("smoke_direction"),
            )
            summary["vlm_validation"] = validation
            summary["comparison_image_path"] = validation.get("comparison_image_path", "")
            self.set_latest_summary(summary)

            self.analysis_queue.put((request_number, summary, history, validation))
            self.validation_queue.task_done()

    def analysis_worker(self):
        analyzer = SafetyAnalysisVlm(self.config)

        while True:
            item = self.analysis_queue.get()

            if item is None:
                self.analysis_queue.task_done()
                break

            request_number, summary, history, validation = item

            try:
                logger.info(
                    "[FirePipeline] VLM analysis request start camera_id=%s request=%s image=%s",
                    self.config.camera_id,
                    request_number,
                    summary.get("comparison_image_path") or self.get_summary_image_path(summary),
                )
                answer = self.run_safety_analysis(
                    analyzer,
                    request_number,
                    summary,
                    history,
                    validation
                )
                logger.info(
                    "[FirePipeline] VLM response received camera_id=%s request=%s chars=%s",
                    self.config.camera_id,
                    request_number,
                    len(str(answer or "")),
                )
                self.emit_result(answer, request_number=request_number)
            except Exception as error:
                logger.exception(
                    "[FirePipeline] VLM analysis failed camera_id=%s request=%s",
                    self.config.camera_id,
                    request_number,
                )
                self.log_status("[VLM] 최종 안전 분석 실패:", error)
            finally:
                self.analysis_queue.task_done()
                self.finish_vlm_cycle()

    def run_safety_analysis(self, analyzer, request_number, summary, history, validation):
        normalized_text = make_compact_vlm_text(history)
        self.storage.save_summary(request_number, summary, normalized_text)

        image_path = summary.get("comparison_image_path", "")

        if image_path == "":
            image_path = self.get_summary_image_path(summary)

        return analyzer.analyze(
            normalized_text,
            validation,
            image_path,
            None
        )

    def emit_result(self, answer, request_number=None):
        self.set_latest_result(answer)
        logger.info(
            "[VLM TEXT] event_type=fire_pipeline camera_id=%s text=%s",
            self.config.camera_id,
            answer,
        )

        if self.on_result is not None:
            self.on_result(answer, request_number=request_number)

        if self.config.print_result:
            print(answer, flush=True)

    def get_summary_image_path(self, summary):
        for frame in reversed(summary.get("frames", [])):
            if "image_path" in frame:
                return frame["image_path"]

        return ""
