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

        parts = []
        for key in (
            "summary",
            "reason",
            "health_considerations",
            "worker_location",
            "rest_reason",
            "recommended_action",
            "recommended_actions",
            "situation",
            "evidence",
            "raw_text",
            "text",
        ):
            value = result.get(key)
            if isinstance(value, list):
                parts.extend(str(item).strip() for item in value if item)
            elif value and str(value).strip() not in {"none", "unknown", "not_visible"}:
                parts.append(str(value).strip())

        # 비틀거림·낙상 탐지 시 알림 텍스트에 명시
        abnormal = str(result.get("abnormal_behavior") or "").strip().lower()
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
            parts.insert(0, f"[행동 이상] {label}{detail}")

        if parts:
            return " | ".join(parts)
        return json.dumps(result, ensure_ascii=False, default=str)

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
            if action and str(action).strip():
                parts.append(f"조치: {action}")

    return " | ".join(parts) if parts else ""
