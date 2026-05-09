import json
import os

import cv2
import numpy as np

from ai.vlm.client import OpenAiCompatibleVlm


def limit_text(text, max_chars):
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[:max_chars - len(marker)] + marker


def get_first_last_frames(summary):
    frames = summary.get("frames", [])
    if not frames:
        return None, None
    return frames[0], frames[-1]


def make_detection_payload(frame):
    if frame is None:
        return {}
    detections = [
        {
            "c": d.get("class", ""),
            "conf": d.get("confidence", 0),
            "box": d.get("box", []),
            "center": d.get("center", []),
        }
        for d in frame.get("detections", [])
    ]
    return {"time": frame.get("time", 0), "detections": detections}


def compact_track(track):
    relation = track.get("danger_relation", {})
    result = {
        "id": track.get("id", 0),
        "dir": track.get("direction", "unknown"),
        "start": track.get("start", []),
        "end": track.get("end", []),
        "relation": relation.get("type", "unknown"),
    }
    if "first_distance" in relation and "last_distance" in relation:
        result["d"] = [relation["first_distance"], relation["last_distance"]]
    return result


def compact_danger_events(events):
    compact = {}
    for class_name in ["fire", "smoke", "spark"]:
        if class_name not in events:
            continue
        item = events[class_name]
        compact[class_name] = {
            "first": item.get("first_seen", 0),
            "last": item.get("last_seen", 0),
            "from": item.get("first_position", "unknown"),
            "to": item.get("last_position", "unknown"),
            "dir": item.get("direction", "stable"),
            "trend": item.get("trend", "unknown"),
        }
    return compact


def build_validation_payload(summary):
    first_frame, last_frame = get_first_last_frames(summary)
    return {
        "win": [summary.get("start_time", 0), summary.get("end_time", 0)],
        "first": make_detection_payload(first_frame),
        "last": make_detection_payload(last_frame),
        "tracks": [compact_track(t) for t in summary.get("person_tracks", [])[:3]],
        "events": compact_danger_events(summary.get("danger_events", {})),
        "clues": summary.get("cause_clues", [])[:4],
    }


def make_compare_image(first_path, last_path, output_path, max_height=360):
    first = cv2.imread(first_path)
    last = cv2.imread(last_path)

    if first is None or last is None:
        return ""

    height = min(min(first.shape[0], last.shape[0]), max_height)
    first = cv2.resize(first, (int(first.shape[1] * height / first.shape[0]), height))
    last = cv2.resize(last, (int(last.shape[1] * height / last.shape[0]), height))

    divider = np.full((height, 6, 3), 80, dtype=np.uint8)
    combined = np.hstack([first, divider, last])
    cv2.putText(combined, "FIRST", (20, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 255, 255), 3)
    cv2.putText(combined, "LAST", (first.shape[1] + 35, 45), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 255, 255), 3)
    cv2.imwrite(output_path, combined)
    return output_path


def build_validation_prompt(payload, max_chars):
    payload_text = json.dumps(payload, ensure_ascii=False)
    prefix = (
        "왼쪽=첫 프레임, 오른쪽=마지막 프레임입니다. 현재 보이는 person/fire/smoke/spark box를 우선 비교해 이동 방향과 위험을 검증하세요.\n"
        "마지막 프레임에 보이지 않는 사람/객체는 현재 이동 판단에서 제외하세요.\n"
        "반드시 JSON만 출력하세요. 값은 짧게 쓰세요.\n"
        '{"movement_valid":true,"person_direction":"left|right|up|down|stable|unknown","corrected_direction":"left|right|up|down|stable|unknown","fire_direction":"left|right|up|down|stable|unknown","smoke_direction":"left|right|up|down|stable|unknown","spark_direction":"left|right|up|down|stable|unknown","risk_level":"low|medium|high|unknown","situation":"짧게","reason_prediction":"짧게","evidence":"짧게"}\n'
        "data="
    )
    allowed = max_chars - len(prefix)
    return limit_text(prefix + limit_text(payload_text, max(0, allowed)), max_chars)


def normalize_validation_result(data):
    if not isinstance(data, dict):
        data = {}
    movement_valid = data.get("movement_valid", False) is True
    return {
        "movement_valid": movement_valid,
        "person_direction": str(data.get("person_direction", "unknown")),
        "corrected_direction": str(data.get("corrected_direction", data.get("person_direction", "unknown"))),
        "fire_direction": str(data.get("fire_direction", "unknown")),
        "smoke_direction": str(data.get("smoke_direction", "unknown")),
        "spark_direction": str(data.get("spark_direction", "unknown")),
        "risk_level": str(data.get("risk_level", "unknown")),
        "situation": str(data.get("situation", ""))[:160],
        "reason_prediction": str(data.get("reason_prediction", ""))[:160],
        "evidence": str(data.get("evidence", ""))[:160],
    }


class MovementValidator:
    def __init__(self, config, client=None):
        if client is None:
            client = OpenAiCompatibleVlm(
                config.vllm_base_url,
                config.vllm_api_key,
                config.vllm_model,
                config.vllm_timeout,
            )
        self.client = client
        self.config = config

    def validate(self, summary, request_number):
        payload = build_validation_payload(summary)
        first_frame, last_frame = get_first_last_frames(summary)
        first_path = first_frame.get("image_path", "") if first_frame else ""
        last_path = last_frame.get("image_path", "") if last_frame else ""

        compare_path = os.path.join(
            self.config.summary_folder,
            "validation_compare_" + str(request_number).zfill(3) + ".jpg",
        )
        compare_path = make_compare_image(
            first_path, last_path, compare_path,
            self.config.validation_compare_height,
        )
        prompt = build_validation_prompt(payload, self.config.validation_prompt_max_chars)

        try:
            data = self.client.request_json(
                prompt, compare_path,
                self.config.validation_vlm_max_tokens,
                self.config.validation_vlm_temperature,
            )
            validation = normalize_validation_result(data)
        except Exception as error:
            validation = {
                "movement_valid": False,
                "person_direction": "unknown",
                "corrected_direction": "unknown",
                "fire_direction": "unknown",
                "smoke_direction": "unknown",
                "spark_direction": "unknown",
                "risk_level": "unknown",
                "situation": "",
                "reason_prediction": "",
                "evidence": "validation_vlm_failed: " + str(error)[:100],
            }

        validation["comparison_image_path"] = compare_path
        return validation
