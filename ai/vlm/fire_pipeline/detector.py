import logging

from ai.vlm.yolo_runtime import configure_ultralytics_runtime, ensure_tensorrt_available


configure_ultralytics_runtime()

from ultralytics import YOLO


logger = logging.getLogger(__name__)


def normalize_class_name(class_name):
    return str(class_name).strip().lower()


def get_yolo_data(result, frame_width, frame_height, detect_classes=None):
    data = []
    allowed_classes = None

    if detect_classes is not None:
        allowed_classes = set(normalize_class_name(name) for name in detect_classes)

    for box in result.boxes:
        class_id = int(box.cls[0])
        class_name = normalize_class_name(result.names[class_id])

        if allowed_classes is not None and class_name not in allowed_classes:
            continue

        confidence = float(box.conf[0])

        x1, y1, x2, y2 = box.xyxy[0].tolist()

        x1 = x1 / frame_width
        y1 = y1 / frame_height
        x2 = x2 / frame_width
        y2 = y2 / frame_height

        center_x = (x1 + x2) / 2
        center_y = (y1 + y2) / 2

        data.append({
            "class": class_name,
            "confidence": round(confidence, 3),
            "box": [
                round(x1, 3),
                round(y1, 3),
                round(x2, 3),
                round(y2, 3)
            ],
            "center": [
                round(center_x, 3),
                round(center_y, 3)
            ]
        })

    return data


def has_class(detections, class_name):
    for detection in detections:
        if detection["class"] == class_name:
            return True

    return False


def has_any_class(detections, class_names):
    for detection in detections:
        if detection["class"] in class_names:
            return True

    return False


def get_count_text(detections):
    count_by_class = {}

    for detection in detections:
        class_name = detection["class"]

        if class_name not in count_by_class:
            count_by_class[class_name] = 0

        count_by_class[class_name] += 1

    text_items = []

    for class_name in count_by_class:
        text_items.append(class_name + "=" + str(count_by_class[class_name]))

    if len(text_items) == 0:
        return "none"

    return ", ".join(text_items)


class YoloDetector:
    def __init__(self, model_path, detect_classes=None, confidence=None):
        logger.info(
            "[YOLO] detector init start model=%s detect_classes=%s confidence=%s",
            model_path,
            detect_classes,
            confidence,
        )
        ensure_tensorrt_available(model_path)
        self.model = YOLO(model_path, task="detect")
        self.detect_classes = None
        self.confidence = confidence
        self.class_ids = None

        if detect_classes is not None:
            self.detect_classes = [normalize_class_name(name) for name in detect_classes]
            self.class_ids = self._resolve_class_ids()

        logger.info(
            "[YOLO] detector init complete model=%s resolved_class_ids=%s names=%s",
            model_path,
            self.class_ids,
            getattr(self.model, "names", None),
        )

    def _resolve_class_ids(self):
        names = getattr(self.model, "names", {})
        items = names.items() if isinstance(names, dict) else enumerate(names)
        wanted = set(self.detect_classes or [])
        class_ids = []

        for class_id, class_name in items:
            if normalize_class_name(class_name) in wanted:
                class_ids.append(int(class_id))

        return class_ids

    def detect(self, frame):
        kwargs = {"verbose": False}

        if self.confidence is not None:
            kwargs["conf"] = self.confidence

        if self.class_ids:
            kwargs["classes"] = self.class_ids

        result = self.model(frame, **kwargs)[0]
        analyzed_frame = result.plot()

        frame_height, frame_width = frame.shape[:2]
        detections = get_yolo_data(
            result,
            frame_width,
            frame_height,
            detect_classes=self.detect_classes,
        )

        return detections, analyzed_frame
