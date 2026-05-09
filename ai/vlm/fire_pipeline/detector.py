from ultralytics import YOLO


def get_yolo_data(result, frame_width, frame_height):
    data = []

    for box in result.boxes:
        class_id = int(box.cls[0])
        class_name = result.names[class_id]
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
    def __init__(self, model_path):
        self.model = YOLO(model_path)

    def detect(self, frame):
        result = self.model(frame, verbose=False)[0]
        analyzed_frame = result.plot()

        frame_height, frame_width = frame.shape[:2]
        detections = get_yolo_data(result, frame_width, frame_height)

        return detections, analyzed_frame
