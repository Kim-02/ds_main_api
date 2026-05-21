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
    """worker_regression 모드 VLM 결과를 알림 텍스트로 변환한다.

    VLM이 생성한 행동 관찰·상황 묘사를 최대한 살리고 고정 레이블은 최소화한다.
    형식: [이름] {이상prefix — obs_detail} | {summary} | {action}
    """
    target = result.get("target") or {}
    worker_name = str(target.get("worker_name") or "").strip()
    abnormal = str(result.get("abnormal_behavior") or "").strip().lower()
    behavior_obs = result.get("behavior_observation") if isinstance(result.get("behavior_observation"), dict) else {}
    obs_detail = str(behavior_obs.get("detail") or "").strip()
    summary = str(result.get("summary") or "").strip()
    recommended_actions = result.get("recommended_actions") or []

    _blank_obs = "화면에서 자세·이동 패턴을 확인할 수 없습니다."

    parts = []

    # 1. [이름] — 식별자만 (고정 텍스트 최소화)
    if worker_name:
        parts.append(f"[{worker_name}]")

    # 2. 행동·상황 묘사 — VLM 관찰 텍스트 우선
    # 이상 행동이면 짧은 키워드 prefix + VLM 상세 묘사
    _abnormal_prefix = {
        "staggering": "비틀거림",
        "falling":    "낙상",
        "slumping":   "신체 처짐",
        "leaning":    "기댐",
        "crouching":  "쭈그림",
    }
    prefix = _abnormal_prefix.get(abnormal)
    obs_text = obs_detail if (obs_detail and obs_detail != _blank_obs) else ""

    if prefix and obs_text:
        parts.append(f"{prefix} — {obs_text}")
    elif prefix:
        parts.append(prefix)
    elif obs_text:
        parts.append(obs_text)

    # 3. VLM 요약 — 행동 묘사와 내용이 다를 때만 추가
    if summary and summary != obs_text and not _text_contained(summary, obs_text):
        parts.append(summary)

    # 4. VLM 생성 조치 — 레이블 없이 바로
    for action in recommended_actions:
        text = _action_text(action)
        if text:
            parts.append(text)
            break

    if not parts:
        rest_rec = str(result.get("rest_recommendation") or "").strip()
        return _rest_recommendation_label(rest_rec) or "심박이상"

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


def _text_contained(a: str, b: str) -> bool:
    """a의 핵심 내용이 b에 이미 포함되어 있는지 간단히 판단한다."""
    if not a or not b:
        return False
    a_short = a[:30].strip()
    return a_short in b or b[:30].strip() in a


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
