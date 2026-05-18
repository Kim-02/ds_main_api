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
    health_risk_summary = str(result.get("health_risk_summary") or "").strip()
    recommended_actions = result.get("recommended_actions")

    if summary:
        parts.append(f"[현장] {summary}")
    if field_status and field_status != summary:
        parts.append(f"현장 상태: {field_status}")
    if worker_movements and worker_movements not in {"none", "unknown"}:
        parts.append(f"작업자 동향: {worker_movements}")
    if health_risk_summary:
        parts.append(f"건강 위험: {health_risk_summary}")
    if isinstance(recommended_actions, list):
        for action in recommended_actions[:2]:
            text = _action_text(action)
            if text:
                parts.append(f"조치: {text}")

    return " | ".join(parts) if parts else ""


def _extract_worker_vlm_text(result: dict) -> str:
    """worker_regression 모드 VLM 결과를 관리자 알림 텍스트로 변환한다."""
    parts = []
    target = result.get("target") or {}
    worker_name = str(target.get("worker_name") or "").strip()
    rest_rec = str(result.get("rest_recommendation") or "").strip()
    summary = str(result.get("summary") or "").strip()
    health = str(result.get("health_considerations") or "").strip()
    abnormal = str(result.get("abnormal_behavior") or "").strip().lower()
    recommended_actions = result.get("recommended_actions") or []

    # ① 행동 이상 (비틀거림·낙상) 먼저
    if abnormal in {"staggering", "falling", "slumping", "leaning", "crouching"}:
        label = {
            "staggering": "비틀거림 감지",
            "falling": "낙상 감지",
            "slumping": "신체 축 처짐 감지",
            "leaning": "기댐 감지",
            "crouching": "쭈그림 감지",
        }.get(abnormal, abnormal)
        behavior_obs = result.get("behavior_observation")
        detail = ""
        if isinstance(behavior_obs, dict) and behavior_obs.get("detail"):
            detail = f" — {behavior_obs['detail']}"
        parts.append(f"[행동 이상] {label}{detail}")

    # ② 작업자명 + 휴식 권고 레벨
    rest_label = _rest_recommendation_label(rest_rec) or _rest_recommendation_label(summary)
    header = f"[{worker_name}] {rest_label}" if worker_name and rest_label else (summary or rest_label)
    if header:
        parts.append(header)

    # ③ 건강 위험 요인 (라벨 제거하고 값만)
    risk_text = _risk_factors_from_health_text(health)
    if risk_text:
        parts.append(f"위험요인: {risk_text}")

    # ④ 조치 (중복 제거, dict 평탄화, 최대 2개)
    seen: set[str] = set()
    action_count = 0
    for action in recommended_actions:
        text = _action_text(action)
        if text and text not in seen:
            seen.add(text)
            parts.append(f"조치: {text}")
            action_count += 1
            if action_count >= 2:
                break

    return " | ".join(parts) if parts else summary


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
