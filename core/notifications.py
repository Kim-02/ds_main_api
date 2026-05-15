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
        parts = []
        for key in ("summary", "recommended_action", "situation", "evidence", "raw_text", "text"):
            value = result.get(key)
            if value:
                parts.append(str(value).strip())
        if parts:
            return " / ".join(parts)
        return json.dumps(result, ensure_ascii=False, default=str)

    if isinstance(result, list):
        return json.dumps(result, ensure_ascii=False, default=str)

    return str(result)
