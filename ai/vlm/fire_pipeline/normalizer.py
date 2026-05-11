from collections import deque


def distance(a, b):
    x = a[0] - b[0]
    y = a[1] - b[1]
    return (x * x + y * y) ** 0.5


RECENT_VISIBLE_SECONDS = 1.5


def get_direction(delta_x, delta_y):
    direction = []

    if abs(delta_x) >= 0.03:
        if delta_x > 0:
            direction.append("right")
        else:
            direction.append("left")

    if abs(delta_y) >= 0.03:
        if delta_y > 0:
            direction.append("down")
        else:
            direction.append("up")

    if len(direction) == 0:
        return "stable"

    return "+".join(direction)


def get_area(box):
    width = max(box[2] - box[0], 0)
    height = max(box[3] - box[1], 0)
    return width * height


def get_position_name(center):
    x = center[0]
    y = center[1]

    if x < 0.33:
        horizontal = "left"
    elif x > 0.66:
        horizontal = "right"
    else:
        horizontal = "center"

    if y < 0.33:
        vertical = "top"
    elif y > 0.66:
        vertical = "bottom"
    else:
        vertical = "middle"

    return horizontal + "-" + vertical


def get_trend(first_value, last_value, margin):
    diff = last_value - first_value

    if diff > margin:
        return "growing"

    if diff < -margin:
        return "shrinking"

    return "stable"


def get_recent_frames(frames, seconds=RECENT_VISIBLE_SECONDS):
    if len(frames) == 0:
        return []

    last_time = frames[-1]["time"]
    recent_frames = []

    for frame in frames:
        if last_time - frame["time"] <= seconds:
            recent_frames.append(frame)

    if len(recent_frames) == 0:
        return [frames[-1]]

    return recent_frames


def average_detection(detections):
    count = len(detections)
    conf_sum = 0
    x_sum = 0
    y_sum = 0
    area_sum = 0

    for detection in detections:
        conf_sum = conf_sum + detection["confidence"]
        x_sum = x_sum + detection["center"][0]
        y_sum = y_sum + detection["center"][1]
        area_sum = area_sum + get_area(detection["box"])

    center = [
        round(x_sum / count, 3),
        round(y_sum / count, 3)
    ]

    return {
        "count": count,
        "average_confidence": round(conf_sum / count, 3),
        "center": center,
        "area": round(area_sum / count, 4),
        "position": get_position_name(center)
    }


def make_current_state(frames):
    if len(frames) == 0:
        return {}

    last_frame = frames[-1]
    by_class = {}

    for detection in last_frame.get("detections", []):
        class_name = detection["class"]

        if class_name not in by_class:
            by_class[class_name] = []

        by_class[class_name].append(detection)

    state = {}

    for class_name in by_class:
        state[class_name] = average_detection(by_class[class_name])

    return state


def make_danger_events(frames, danger_classes):
    events = {}
    window_end_time = frames[-1]["time"]

    for class_name in danger_classes:
        points = []

        for frame in frames:
            detections = []

            for detection in frame.get("detections", []):
                if detection["class"] == class_name:
                    detections.append(detection)

            if len(detections) == 0:
                continue

            point = average_detection(detections)
            point["time"] = frame["time"]
            points.append(point)

        if len(points) == 0:
            continue

        first = points[0]
        last = points[-1]
        recent_points = []

        for point in points:
            if window_end_time - point["time"] <= RECENT_VISIBLE_SECONDS:
                recent_points.append(point)

        if len(recent_points) >= 2:
            motion_start = recent_points[0]
            motion_end = recent_points[-1]
        else:
            motion_start = first
            motion_end = last

        delta_x = round(motion_end["center"][0] - motion_start["center"][0], 3)
        delta_y = round(motion_end["center"][1] - motion_start["center"][1], 3)
        first_strength = first["count"] * first["average_confidence"] * max(first["area"], 0.001)
        last_strength = last["count"] * last["average_confidence"] * max(last["area"], 0.001)

        events[class_name] = {
            "first_seen": first["time"],
            "last_seen": last["time"],
            "first_center": first["center"],
            "last_center": last["center"],
            "first_position": first["position"],
            "last_position": last["position"],
            "first_area": first["area"],
            "last_area": last["area"],
            "delta": [delta_x, delta_y],
            "direction": get_direction(delta_x, delta_y),
            "visible_now": window_end_time - last["time"] <= RECENT_VISIBLE_SECONDS,
            "motion_start_center": motion_start["center"],
            "motion_end_center": motion_end["center"],
            "trend": get_trend(first_strength, last_strength, 0.01),
            "points": points
        }

    return events


def make_event_order(danger_events):
    order = []

    for class_name in danger_events:
        order.append({
            "class": class_name,
            "first_seen": danger_events[class_name]["first_seen"],
            "position": danger_events[class_name]["first_position"]
        })

    order.sort(key=lambda item: item["first_seen"])
    return order


def find_nearest_danger(center, frame, danger_classes):
    nearest = None

    for detection in frame.get("detections", []):
        if detection["class"] not in danger_classes:
            continue

        danger_distance = distance(center, detection["center"])

        if nearest is None or danger_distance < nearest["distance"]:
            nearest = {
                "class": detection["class"],
                "distance": danger_distance,
                "center": detection["center"]
            }

    if nearest is None:
        return None

    return {
        "class": nearest["class"],
        "distance": round(nearest["distance"], 3),
        "center": nearest["center"]
    }


def add_person_to_track(track, frame, person, danger_classes):
    point = {
        "time": frame["time"],
        "center": person["center"],
        "confidence": person["confidence"],
        "nearest_danger": find_nearest_danger(person["center"], frame, danger_classes)
    }

    track["points"].append(point)
    track["last_center"] = person["center"]
    track["last_time"] = frame["time"]


def make_person_tracks(frames, danger_classes):
    tracks = []
    next_id = 1
    window_end_time = frames[-1]["time"]

    for frame in frames:
        persons = []

        for detection in frame.get("detections", []):
            if detection["class"] == "person":
                persons.append(detection)

        used_tracks = []

        for person in persons:
            best_track = None
            best_distance = 999

            for track in tracks:
                if track["id"] in used_tracks:
                    continue

                if frame["time"] - track["last_time"] > 2.0:
                    continue

                track_distance = distance(person["center"], track["last_center"])

                if track_distance < best_distance:
                    best_distance = track_distance
                    best_track = track

            if best_track is None or best_distance > 0.18:
                best_track = {
                    "id": next_id,
                    "points": [],
                    "last_center": person["center"],
                    "last_time": frame["time"]
                }
                next_id = next_id + 1
                tracks.append(best_track)

            add_person_to_track(best_track, frame, person, danger_classes)
            used_tracks.append(best_track["id"])

    result = []

    for track in tracks:
        points = track["points"]

        if len(points) == 0:
            continue

        start = points[0]["center"]
        end = points[-1]["center"]
        delta_x = round(end[0] - start[0], 3)
        delta_y = round(end[1] - start[1], 3)
        relation = make_person_danger_relation(points)
        visible_now = window_end_time - points[-1]["time"] <= RECENT_VISIBLE_SECONDS

        result.append({
            "id": track["id"],
            "seen_count": len(points),
            "start_time": points[0]["time"],
            "end_time": points[-1]["time"],
            "visible_now": visible_now,
            "age_seconds": round(window_end_time - points[-1]["time"], 2),
            "start": start,
            "end": end,
            "delta": [delta_x, delta_y],
            "direction": get_direction(delta_x, delta_y),
            "danger_relation": relation,
            "points": points
        })

    return result


def keep_visible_tracks(tracks):
    visible_tracks = []

    for track in tracks:
        if track.get("visible_now") == True:
            visible_tracks.append(track)

    return visible_tracks


def make_person_danger_relation(points):
    danger_points = []

    for point in points:
        if point["nearest_danger"] is not None:
            danger_points.append(point)

    if len(danger_points) == 0:
        return {
            "type": "no_danger_nearby"
        }

    first = danger_points[0]["nearest_danger"]
    last = danger_points[-1]["nearest_danger"]
    distance_delta = round(last["distance"] - first["distance"], 3)

    if distance_delta > 0.05:
        relation_type = "moving_away_from_" + last["class"]
    elif distance_delta < -0.05:
        relation_type = "approaching_" + last["class"]
    else:
        relation_type = "distance_stable_to_" + last["class"]

    return {
        "type": relation_type,
        "first_distance": first["distance"],
        "last_distance": last["distance"],
        "delta": distance_delta,
        "danger_class": last["class"]
    }


def make_cause_clues(danger_events, person_tracks):
    clues = []

    if "fire" in danger_events and "smoke" in danger_events:
        if danger_events["smoke"]["first_seen"] >= danger_events["fire"]["first_seen"]:
            clues.append("smoke_after_fire")

    for class_name in danger_events:
        if danger_events[class_name]["trend"] == "growing":
            clues.append(class_name + "_growing")

    for track in person_tracks:
        relation_type = track["danger_relation"].get("type", "")

        if relation_type.startswith("approaching_"):
            clues.append("person_" + str(track["id"]) + "_" + relation_type)

        if relation_type.startswith("moving_away_"):
            clues.append("person_" + str(track["id"]) + "_" + relation_type)

    return clues[:5]


def make_person_movement(frames):
    points = []

    for frame in frames:
        persons = []

        for detection in frame.get("detections", []):
            if detection["class"] == "person":
                persons.append(detection)

        if len(persons) == 0:
            continue

        center_x = 0
        center_y = 0

        for person in persons:
            center_x = center_x + person["center"][0]
            center_y = center_y + person["center"][1]

        center_x = round(center_x / len(persons), 3)
        center_y = round(center_y / len(persons), 3)

        points.append({
            "time": frame["time"],
            "count": len(persons),
            "center": [center_x, center_y]
        })

    if len(points) == 0:
        return {
            "found": False,
            "points": []
        }

    start = points[0]["center"]
    end = points[-1]["center"]
    delta_x = round(end[0] - start[0], 3)
    delta_y = round(end[1] - start[1], 3)

    return {
        "found": True,
        "start": start,
        "end": end,
        "delta": [delta_x, delta_y],
        "direction": get_direction(delta_x, delta_y),
        "points": points
    }


def make_summary(window, danger_classes):
    class_summary = {}
    cause_summary = {}
    copied_frames = []

    for frame_data in window:
        detections = frame_data["detections"]
        copied_frame = dict(frame_data)
        copied_frame["detections"] = list(detections)
        copied_frames.append(copied_frame)

        for detection in detections:
            class_name = detection["class"]

            if class_name not in class_summary:
                class_summary[class_name] = {
                    "count": 0,
                    "confidence_sum": 0,
                    "max_confidence": 0,
                    "x1_sum": 0,
                    "y1_sum": 0,
                    "x2_sum": 0,
                    "y2_sum": 0
                }

            class_summary[class_name]["count"] += 1
            class_summary[class_name]["confidence_sum"] += detection["confidence"]
            class_summary[class_name]["max_confidence"] = max(
                class_summary[class_name]["max_confidence"],
                detection["confidence"]
            )
            class_summary[class_name]["x1_sum"] += detection["box"][0]
            class_summary[class_name]["y1_sum"] += detection["box"][1]
            class_summary[class_name]["x2_sum"] += detection["box"][2]
            class_summary[class_name]["y2_sum"] += detection["box"][3]

        dangers = []
        objects = []

        for detection in detections:
            if detection["class"] in danger_classes:
                dangers.append(detection)
            else:
                objects.append(detection)

        for danger in dangers:
            for obj in objects:
                near_distance = distance(danger["center"], obj["center"])

                if near_distance < 0.35:
                    key = danger["class"] + "_near_" + obj["class"]

                    if key not in cause_summary:
                        cause_summary[key] = {
                            "danger": danger["class"],
                            "near_object": obj["class"],
                            "count": 0,
                            "closest_distance": near_distance
                        }

                    cause_summary[key]["count"] += 1

                    if near_distance < cause_summary[key]["closest_distance"]:
                        cause_summary[key]["closest_distance"] = near_distance

    normalized_classes = {}

    for class_name in class_summary:
        item = class_summary[class_name]
        count = item["count"]

        normalized_classes[class_name] = {
            "count": count,
            "average_confidence": round(item["confidence_sum"] / count, 3),
            "max_confidence": round(item["max_confidence"], 3),
            "average_box": [
                round(item["x1_sum"] / count, 3),
                round(item["y1_sum"] / count, 3),
                round(item["x2_sum"] / count, 3),
                round(item["y2_sum"] / count, 3)
            ]
        }

    cause_candidates = []

    for key in cause_summary:
        item = cause_summary[key]
        cause_candidates.append({
            "danger": item["danger"],
            "near_object": item["near_object"],
            "count": item["count"],
            "closest_distance": round(item["closest_distance"], 3)
        })

    cause_candidates.sort(key=lambda x: x["count"], reverse=True)

    recent_frames = get_recent_frames(copied_frames)
    current_state = make_current_state(copied_frames)
    danger_events = make_danger_events(copied_frames, danger_classes)
    event_order = make_event_order(danger_events)
    all_person_tracks = make_person_tracks(copied_frames, danger_classes)
    person_tracks = keep_visible_tracks(all_person_tracks)
    cause_clues = make_cause_clues(danger_events, person_tracks)

    return {
        "start_time": window[0]["time"],
        "end_time": window[-1]["time"],
        "frame_count": len(window),
        "classes": normalized_classes,
        "current_state": current_state,
        "danger_events": danger_events,
        "event_order": event_order,
        "cause_candidates": cause_candidates,
        "person_movement": make_person_movement(recent_frames),
        "person_tracks": person_tracks,
        "historical_person_tracks": all_person_tracks[:5],
        "cause_clues": cause_clues,
        "frames": copied_frames
    }


class WindowNormalizer:
    def __init__(self, window_size, keep_count, danger_classes):
        self.window_size = window_size
        self.keep_count = keep_count
        self.danger_classes = danger_classes
        self.window = deque(maxlen=window_size)
        self.summaries = []

    def clear_window(self):
        self.window.clear()

    def clear_all(self):
        self.window.clear()
        self.summaries.clear()

    def make_live_summary(self):
        if len(self.window) == 0:
            return None

        return make_summary(list(self.window), self.danger_classes)

    def make_history_with_live_summary(self, completed_summary=None):
        history = list(self.summaries)
        live_summary = completed_summary

        if live_summary is None:
            live_summary = self.make_live_summary()

        if live_summary is None:
            if len(history) == 0:
                return None, []

            return history[-1], history[-self.keep_count:]

        should_append = True

        if len(history) > 0:
            latest = history[-1]

            if latest.get("start_time") == live_summary.get("start_time"):
                if latest.get("end_time") == live_summary.get("end_time"):
                    should_append = False

        if should_append:
            history.append(live_summary)

        return live_summary, history[-self.keep_count:]

    def add_frame(self, frame_data):
        self.window.append(frame_data)

        if len(self.window) < self.window_size:
            return None

        summary = make_summary(list(self.window), self.danger_classes)

        self.summaries.append(summary)

        if len(self.summaries) > self.keep_count:
            self.summaries.pop(0)

        return summary
