"""Notification payload helpers for socket-connected apps."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def make_hazard_alert_ws_payload(
    event_id: int | None,
    message: str,
    *,
    title: str = "온습도 위험 감지",
    level: str = "warning",
    space_id: int | None = None,
    jetson_id: int | None = None,
    camera_sen_id: int | None = None,
    sensor_id: str | None = None,
    camera_name: str = "",
    camera_loc: str = "",
    ev_code_name: str | None = None,
    source: str = "temperature_vlm",
    vibration: bool = True,
    led: bool = True,
    duration_ms: int = 3000,
    reset_after_ms: int = 5000,
    event_time: str | None = None,
    vlm_result: Any | None = None,
    hazard_material: str = "",
    hazard_warning: str = "",
    hazard_specific_action: str = "",
    evacuation_route: str = "",
    abnormal_behavior: str = "",
    detection_info: dict | None = None,
    person_movement: dict | None = None,
    environment_detections: dict | None = None,
) -> dict:
    """앱의 HazardAlert 모델과 1:1 매핑되는 WebSocket 알림 payload를 반환한다."""
    color_map = {"danger": "red", "warning": "orange", "info": "yellow"}
    color = color_map.get(level, "yellow")
    created_at = event_time or datetime.now().isoformat(timespec="seconds")
    return {
        "type": "hazard_alert",
        "event_id": event_id,
        "target_topic": "",
        "alert": True,
        "message": message,
        "title": title,
        "level": level,
        "source": source,
        "space_id": space_id,
        "jetson_id": jetson_id,
        "camera_sen_id": camera_sen_id,
        "sensor_id": sensor_id,
        "color": color,
        "vibration": vibration,
        "camera_name": camera_name,
        "camera_loc": camera_loc,
        "ev_code_name": ev_code_name or title,
        "event_time": created_at,
        "created_at": created_at,
        "led": led,
        "duration_ms": duration_ms,
        "reset_after_ms": reset_after_ms,
        "is_read": False,
        "vlm_result": vlm_result,
        "hazard_material": hazard_material,
        "hazard_warning": hazard_warning,
        "hazard_specific_action": hazard_specific_action,
        "evacuation_route": evacuation_route,
        "abnormal_behavior": abnormal_behavior,
        "detection_info": detection_info or {},
        "person_movement": person_movement or {},
        "environment_detections": environment_detections or {},
    }


def make_vlm_push_payload(
    event_type: str,
    title: str,
    result: Any,
    **extra: Any,
) -> dict:
    text = extract_vlm_text(result)
    payload = {
        "type": event_type,
        "push": True,
        "notification_type": "vlm_analysis",
        "title": title,
        "body": text or "autoregressive VLM 분석이 완료되었습니다.",
        "text": text,
        "result": result,
    }
    payload.update(extra)
    return payload


def extract_vlm_text(result: Any) -> str:
    if result is None:
        return ""

    if isinstance(result, str):
        return result.strip()

    if isinstance(result, dict):
        target_type = (result.get("target") or {}).get("type") if isinstance(result.get("target"), dict) else None

        if target_type == "site":
            return _extract_site_vlm_text(result)
        if target_type == "worker":
            return _extract_worker_vlm_text(result)

        if result:
            return json.dumps(result, ensure_ascii=False, default=str)
        return ""

    if isinstance(result, list):
        return json.dumps(result, ensure_ascii=False, default=str)

    return str(result)


def _extract_site_vlm_text(result: dict) -> str:
    """environment(site) 모드 VLM 결과를 관리자 알림 텍스트로 변환한다."""
    parts = []
    summary = str(result.get("summary") or "").strip()
    field_status = str(result.get("field_status") or "").strip()
    worker_movements = str(result.get("worker_movements") or "").strip()
    person_identification = _person_identification_text(result.get("person_identification"))
    health_risk_summary = str(result.get("health_risk_summary") or "").strip()
    recommended_actions = result.get("recommended_actions")

    if summary:
        parts.append(f"[현장] {summary}")
    if field_status and field_status != summary:
        parts.append(f"현장 상태: {field_status}")
    if worker_movements and worker_movements not in {"none", "unknown"}:
        parts.append(f"작업자 동향: {worker_movements}")
    if person_identification:
        parts.append(f"구분 단서: {person_identification}")
    if health_risk_summary:
        parts.append(f"건강 위험: {health_risk_summary}")
    if isinstance(recommended_actions, list):
        for action in recommended_actions[:2]:
            text = _action_text(action)
            if text:
                parts.append(f"조치: {text}")

    return " | ".join(parts) if parts else ""


def _extract_worker_vlm_text(result: dict) -> str:
    """worker_regression 모드 VLM 결과를 컴팩트 알림 텍스트로 변환한다.

    형식: [이름] 휴식권고/심박이상 | 행동: 비틀거림 | 질병: 고혈압, 고령 | 조치: ...
    """
    target = result.get("target") or {}
    worker_name = str(target.get("worker_name") or "").strip()
    rest_rec = str(result.get("rest_recommendation") or "").strip()
    health = str(result.get("health_considerations") or "").strip()
    recommended_actions = result.get("recommended_actions") or []

    parts = []

    # [이름] 휴식권고 레이블 or 심박이상
    rest_label = _rest_recommendation_label(rest_rec)
    header = f"[{worker_name}] {rest_label or '심박이상'}" if worker_name else (rest_label or "심박이상")
    parts.append(header)

    # 행동
    parts.append(f"행동: {behavior_label_from_vlm_result(result)}")

    # 질병 (건강 위험 요인) - 콜론 뒤 값만, 없으면 "없음"
    diseases = _risk_factors_from_health_text(health)
    no_risk = not diseases or "없" in diseases or len(diseases) > 25
    parts.append(f"질병: {'없음' if no_risk else diseases}")

    # 조치 (1개만)
    for action in recommended_actions:
        text = _action_text(action)
        if text:
            parts.append(f"조치: {text}")
            break

    return " | ".join(parts)


def format_person_identification_text(value) -> str:
    """VLM의 사람 구분 단서를 앱 알림용 한 줄로 압축한다."""
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        return "" if text.lower() in {"", "none", "unknown", "not_visible", "null"} else text
    if isinstance(value, list):
        people = value
        note = ""
        target_match = ""
    elif isinstance(value, dict):
        people = value.get("moving_people") or value.get("people") or []
        note = str(value.get("note") or "").strip()
        target_match = str(value.get("target_worker_match") or "").strip()
    else:
        return ""

    items = []
    if target_match and target_match not in {"unknown", "not_visible"}:
        items.append(f"대상매칭={target_match}")

    if isinstance(people, list):
        for person in people[:2]:
            if not isinstance(person, dict):
                text = str(person or "").strip()
                if text:
                    items.append(text)
                continue

            details = []
            for key in ("label", "clothing", "ppe", "position", "movement"):
                text = str(person.get(key) or "").strip()
                if text and text.lower() not in {"none", "unknown", "not_visible", "null"}:
                    details.append(text)
            if details:
                items.append(" / ".join(details))

    if note and note.lower() not in {"none", "unknown", "not_visible", "null"}:
        items.append(note)

    return "; ".join(dict.fromkeys(items))


def _person_identification_text(value) -> str:
    return format_person_identification_text(value)


def _rest_recommendation_label(text: str) -> str:
    if not text:
        return ""
    if "반드시" in text:
        return "즉시 휴식 필요"
    if "강한" in text:
        return "강한 휴식 권고"
    if "약한" in text:
        return "약한 휴식 권고"
    return ""


def _risk_factors_from_health_text(health: str) -> str:
    """'등록 건강 유의요인: 고령, 고혈압' → '고령, 고혈압'"""
    for separator in (":", "："):
        if separator in health:
            return health.split(separator, 1)[-1].strip()
    return health.strip() if health else ""


def _action_text(action: Any) -> str:
    if isinstance(action, str):
        return action.strip()
    if isinstance(action, dict):
        for key in ("action", "description", "text", "content", "message"):
            val = action.get(key)
            if val and isinstance(val, str):
                return val.strip()
        return " ".join(str(v) for v in action.values() if v).strip()
    return str(action).strip() if action else ""


def _behavior_label(abnormal: str, posture: str = "") -> str:
    """VLM abnormal_behavior + posture → 한국어 행동 레이블."""
    a = (abnormal or "").strip().lower()
    p = (posture or "").strip().lower()
    if a == "staggering":
        return "비틀거림"
    if a in {"falling", "slumping", "leaning"}:
        return "이상있음"
    if a == "crouching":
        return "앉아있음"
    if a == "normal":
        if p == "crouching":
            return "앉아있음"
        if p == "upright":
            return "서있음"
        return "이상없음"
    return "미확인"


def behavior_label_from_vlm_result(result: dict) -> str:
    """VLM result dict에서 행동 레이블을 반환한다."""
    abnormal = str(result.get("abnormal_behavior") or "").strip()
    obs = result.get("behavior_observation")
    posture = str(obs.get("posture") or "") if isinstance(obs, dict) else ""
    return _behavior_label(abnormal, posture)
