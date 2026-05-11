name_ko = {
    "fire": "화재",
    "smoke": "연기",
    "person": "사람",
    "helmet": "헬멧"
}

danger_classes = ["fire", "smoke"]


def get_name(class_name):
    if class_name in name_ko:
        return name_ko[class_name]

    return class_name


def get_center_text(box):
    center_x = round((box[0] + box[2]) / 2, 3)
    center_y = round((box[1] + box[3]) / 2, 3)
    return "(" + str(center_x) + "," + str(center_y) + ")"


def make_class_text(class_name, item):
    text = (
        get_name(class_name)
        + " cnt=" + str(item["count"])
        + " avg=" + str(item["average_confidence"])
    )

    if "max_confidence" in item:
        text = text + " max=" + str(item["max_confidence"])

    if "average_box" in item:
        text = text + " center=" + get_center_text(item["average_box"])

    return text


def make_cause_text(cause):
    return (
        get_name(cause["danger"])
        + "-near-"
        + get_name(cause["near_object"])
        + " cnt=" + str(cause["count"])
        + " d=" + str(cause["closest_distance"])
    )


def make_timeline_text(summary):
    frames = summary.get("frames", [])

    if len(frames) == 0:
        return "timeline=none"

    selected_frames = []

    if len(frames) <= 3:
        selected_frames = frames
    else:
        selected_frames = [
            frames[0],
            frames[len(frames) // 2],
            frames[-1]
        ]

    items = []

    for frame in selected_frames:
        count_by_class = {}

        for detection in frame.get("detections", []):
            class_name = detection["class"]

            if class_name not in danger_classes:
                continue

            if class_name not in count_by_class:
                count_by_class[class_name] = 0

            count_by_class[class_name] += 1

        danger_items = []

        for class_name in danger_classes:
            if class_name in count_by_class:
                danger_items.append(get_name(class_name) + "=" + str(count_by_class[class_name]))

        if len(danger_items) == 0:
            danger_text = "danger=0"
        else:
            danger_text = ",".join(danger_items)

        items.append(str(frame["time"]) + "s:" + danger_text)

    return "timeline=" + " | ".join(items)


def make_current_state_text(summary):
    current_state = summary.get("current_state", {})

    if len(current_state) == 0:
        return "current=none"

    items = []

    for class_name in ["fire", "smoke", "person"]:
        if class_name not in current_state:
            continue

        item = current_state[class_name]
        items.append(
            get_name(class_name)
            + "=" + str(item["count"])
            + "@"
            + item["position"]
            + " conf="
            + str(item["average_confidence"])
        )

    if len(items) == 0:
        return "current=none"

    return "current: " + " ; ".join(items)


def make_event_order_text(summary):
    event_order = summary.get("event_order", [])
    danger_events = summary.get("danger_events", {})

    if len(event_order) == 0:
        return "events=none"

    items = []

    for event in event_order:
        class_name = event["class"]
        trend = "unknown"
        last_position = event["position"]

        if class_name in danger_events:
            trend = danger_events[class_name].get("trend", "unknown")
            last_position = danger_events[class_name].get("last_position", last_position)

        items.append(
            get_name(class_name)
            + " first="
            + str(event["first_seen"])
            + "s"
            + " from="
            + event["position"]
            + " to="
            + last_position
            + " trend="
            + trend
        )

    return "events: " + " -> ".join(items)


def make_danger_motion_text(summary):
    danger_events = summary.get("danger_events", {})

    if len(danger_events) == 0:
        return "danger_motion=none"

    items = []

    for class_name in danger_classes:
        if class_name not in danger_events:
            continue

        event = danger_events[class_name]

        if event.get("visible_now") == False:
            continue

        items.append(
            get_name(class_name)
            + " dir="
            + event.get("direction", "stable")
            + " from="
            + event.get("first_position", "unknown")
            + " to="
            + event.get("last_position", "unknown")
            + " trend="
            + event.get("trend", "unknown")
        )

    if len(items) == 0:
        return "danger_motion=none"

    return "danger_motion: " + " ; ".join(items)


def make_person_point_text(point):
    center = point["center"]
    return (
        str(point["time"])
        + "s("
        + str(center[0])
        + ","
        + str(center[1])
        + ",n="
        + str(point["count"])
        + ")"
    )


def make_track_point_text(point):
    center = point["center"]
    return (
        str(point["time"])
        + "s("
        + str(center[0])
        + ","
        + str(center[1])
        + ")"
    )


def make_person_track_text(track):
    points = track.get("points", [])

    if len(points) == 0:
        path = "none"
    elif len(points) <= 3:
        path_points = points
        path_items = []

        for point in path_points:
            path_items.append(make_track_point_text(point))

        path = "->".join(path_items)
    else:
        path_points = [
            points[0],
            points[len(points) // 2],
            points[-1]
        ]
        path_items = []

        for point in path_points:
            path_items.append(make_track_point_text(point))

        path = "->".join(path_items)

    relation = track.get("danger_relation", {})
    relation_type = relation.get("type", "unknown")

    distance_text = ""

    if "first_distance" in relation and "last_distance" in relation:
        distance_text = (
            " d="
            + str(relation["first_distance"])
            + "->"
            + str(relation["last_distance"])
        )

    return (
        "p"
        + str(track["id"])
        + " "
        + path
        + " dir="
        + track.get("direction", "unknown")
        + " relation="
        + relation_type
        + " visible="
        + str(track.get("visible_now", True))
        + distance_text
    )


def make_person_tracks_text(summary):
    tracks = summary.get("person_tracks", [])

    if len(tracks) == 0:
        return "person_tracks=none"

    items = []

    for track in tracks[:3]:
        items.append(make_person_track_text(track))

    return "person_tracks: " + " ; ".join(items)


def make_person_path_text(summary):
    movement = summary.get("person_movement", {})

    if movement.get("found") != True:
        return "person_path=none"

    points = movement.get("points", [])

    if len(points) == 0:
        return "person_path=none"

    if len(points) <= 3:
        selected_points = points
    else:
        selected_points = [
            points[0],
            points[len(points) // 2],
            points[-1]
        ]

    point_items = []

    for point in selected_points:
        point_items.append(make_person_point_text(point))

    return (
        "person_path="
        + " -> ".join(point_items)
        + " delta="
        + str(movement.get("delta", []))
        + " dir="
        + str(movement.get("direction", "unknown"))
    )


def make_cause_clues_text(summary):
    clues = summary.get("cause_clues", [])

    if len(clues) == 0:
        return "cause_clues=none"

    return "cause_clues: " + " ; ".join(clues[:5])


def get_danger_center_x(summary):
    total_count = 0
    center_sum = 0
    current_state = summary.get("current_state", {})

    for class_name in danger_classes:
        if class_name not in current_state:
            continue

        item = current_state[class_name]

        if "center" not in item:
            continue

        count = item.get("count", 1)
        center_sum = center_sum + item["center"][0] * count
        total_count = total_count + count

    if total_count > 0:
        return center_sum / total_count

    total_count = 0
    center_sum = 0

    for class_name in danger_classes:
        if class_name not in summary.get("classes", {}):
            continue

        item = summary["classes"][class_name]

        if "average_box" not in item:
            continue

        box = item["average_box"]
        count = item["count"]
        center_x = (box[0] + box[2]) / 2

        center_sum = center_sum + center_x * count
        total_count = total_count + count

    if total_count == 0:
        return None

    return center_sum / total_count


def make_evacuation_hint_text(summary):
    center_x = get_danger_center_x(summary)

    if center_x is None:
        return "evacuation_hint=none"

    if center_x < 0.5:
        return "evacuation_hint=fire_side=left,evacuate=right"

    return "evacuation_hint=fire_side=right,evacuate=left"


def make_one_summary_text(summary):
    lines = []

    lines.append(
        "time="
        + str(summary["start_time"])
        + "-"
        + str(summary["end_time"])
        + "s"
        + " frames="
        + str(summary["frame_count"])
    )

    lines.append(make_current_state_text(summary))
    lines.append(make_danger_motion_text(summary))
    lines.append(make_event_order_text(summary))

    danger_items = []

    for class_name in danger_classes:
        if class_name in summary["classes"]:
            danger_items.append(make_class_text(class_name, summary["classes"][class_name]))

    if len(danger_items) == 0:
        lines.append("danger=none")
    else:
        lines.append("danger: " + " ; ".join(danger_items))

    object_items = []

    for class_name in summary["classes"]:
        if class_name in danger_classes:
            continue

        object_items.append(make_class_text(class_name, summary["classes"][class_name]))

        if len(object_items) >= 3:
            break

    if len(object_items) > 0:
        lines.append("objects: " + " ; ".join(object_items))

    lines.append(make_person_path_text(summary))
    lines.append(make_person_tracks_text(summary))
    lines.append(make_evacuation_hint_text(summary))
    lines.append(make_cause_clues_text(summary))

    cause_items = []

    for cause in summary.get("cause_candidates", [])[:3]:
        cause_items.append(make_cause_text(cause))

    if len(cause_items) > 0:
        lines.append("causes: " + " ; ".join(cause_items))
    else:
        lines.append("causes=none")

    lines.append(make_timeline_text(summary))

    return "\n".join(lines)


def make_past_reference_text(summary):
    lines = []
    lines.append(
        "time="
        + str(summary["start_time"])
        + "-"
        + str(summary["end_time"])
        + "s"
    )
    lines.append(make_current_state_text(summary))
    lines.append(make_danger_motion_text(summary))
    lines.append(make_cause_clues_text(summary))
    return "\n".join(lines)


def make_compact_vlm_text(summaries, max_summaries=3):
    if isinstance(summaries, dict):
        summaries = [summaries]

    summaries = summaries[-max_summaries:]

    lines = []
    lines.append("[YOLO compact history]")
    lines.append("mode=current-first normalized context")
    lines.append("coord=normalized 0~1, center=(x,y), cnt=detection count")
    lines.append("priority=current image and current_state first")
    lines.append("past_reference=weak clue only; ignore disappeared persons/objects unless visible now")

    if len(summaries) == 0:
        return "\n".join(lines)

    past_summaries = summaries[:-1]

    for index, summary in enumerate(past_summaries, start=1):
        lines.append("")
        lines.append("#past_reference_" + str(index))
        lines.append(make_past_reference_text(summary))

    lines.append("")
    lines.append("#current_summary")
    lines.append(make_one_summary_text(summaries[-1]))

    return "\n".join(lines)


def make_vlm_text(data):
    return make_compact_vlm_text(data)


def limit_text(text, max_chars):
    if len(text) <= max_chars:
        return text

    marker = "\n...[truncated]"
    return text[:max_chars - len(marker)] + marker
