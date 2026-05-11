"""Notification payload helpers for socket-connected apps."""
from __future__ import annotations

import json
from typing import Any


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
        "body": text or "VLM 분석이 완료되었습니다.",
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
