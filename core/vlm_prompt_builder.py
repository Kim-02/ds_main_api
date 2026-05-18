"""Shared autoregressive VLM prompt builders.

두 판단 흐름은 분리하되 CCTV autoregressive VLM에 전달하는 기본 맥락,
YOLO 정규화 텍스트, JSON 응답 형식은 이 모듈에서 공통으로 만든다.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from config import settings
from core.cctv_vlm_context import public_yolo_context


HAZARD_RESPONSE_GUIDES = {
    "벤젠": "벤젠: 점화원 차단, 흡입·접촉 회피, 안전 시 환기, 상풍측 대피와 전문 대응 요청.",
    "benzene": "벤젠: 점화원 차단, 흡입·접촉 회피, 안전 시 환기, 상풍측 대피와 전문 대응 요청.",
    "나트륨": "나트륨: 물·습기 접촉 금지, 건조 격리, 물소화 금지, 금속화재 대응 인력 호출.",
    "sodium": "나트륨: 물·습기 접촉 금지, 건조 격리, 물소화 금지, 금속화재 대응 인력 호출.",
    "철강": "철강: 고온 표면·중량물·흄 주의, 작업자 이격, 보호구와 환기 상태 확인.",
}


def build_vlm_prompt(
    *,
    mode: str,
    camera: dict,
    trigger: dict,
    yolo_context: dict | None = None,
) -> str:
    if mode == "environment":
        return build_environment_health_prompt(camera, trigger, yolo_context)
    if mode == "worker_regression":
        return build_worker_regression_prompt(camera, trigger, yolo_context)
    raise ValueError(f"Unsupported VLM prompt mode: {mode}")


def build_common_autoregressive_vlm_prompt(camera: dict, yolo_context: dict | None) -> str:
    return (
        "분석 명칭: autoregressive VLM.\n"
        "반드시 모든 응답 필드 값을 한국어로 작성하세요. 영어 사용 금지.\n"
        "산업 안전 안내만 작성, 의료 진단 금지. 가능성/주의/관리자 확인 표현을 사용하세요.\n"
        "현재 CCTV 이미지 우선, YOLO 10초 정규화는 보조 근거. 화면에 보이지 않는 것은 확정하지 마세요.\n"
        f"현재시각={datetime.now().isoformat(timespec='seconds')}\n"
        f"카메라={json_dumps(compact_camera_for_prompt(camera))}\n"
        f"{format_yolo_context_for_prompt(yolo_context)}\n"
    )


def build_environment_health_prompt(camera: dict, trigger: dict, yolo_context: dict | None = None) -> str:
    context = compact_environment_trigger_for_prompt(camera, trigger)
    health = context.get("health_attention") or {}
    risk_factors = health.get("risk_factors") or {}
    action_hints = _risk_factor_action_hints(risk_factors)
    return (
        build_common_autoregressive_vlm_prompt(camera, yolo_context)
        + "판단유형=environment\n"
        "아래 필드를 각각 독립적으로 작성하세요. 필드 설명 문구는 출력하지 마세요.\n"
        "─ field_status: 현재 화면에서 보이는 현장 상태를 한 문장으로 묘사하세요.\n"
        "─ worker_movements: YOLO 10초 관찰 결과를 자연어로 작성하세요.\n"
        "  예) '2명 관찰, 10초간 안정적 정지 상태' / '1명 관찰, 불규칙 이동·비틀거림 의심'\n"
        "─ abnormal_behavior: staggering|falling|slumping|crouching|leaning|normal|not_visible 중 하나만 쓰세요.\n"
        "  - YOLO delta가 크거나 불규칙 → staggering, 이미지에서 쓰러짐 → falling, 정상 직립 → normal\n"
        "  - staggering 또는 falling 탐지 시 risk_level=high 이상, recommended_actions 맨 앞에 '즉시 현장 확인' 추가\n"
        "─ health_risk_summary: health_attention 데이터를 한 문장으로 요약하세요.\n"
        "─ recommended_actions: 건강 위험 요인별 즉시 실행 가능한 구체적 조치를 나열하세요.\n"
        f"  위험 요인별 권고 참고: {action_hints}\n"
        "  각 항목은 '무엇을 → 어떻게'가 명확한 한 문장으로 작성하세요.\n"
        "─ summary: 관리자가 한눈에 이해할 수 있는 한 줄 요약 (건강 위험 + 행동 관찰 포함)\n"
        "특정 작업자에게 문제가 발생했다고 단정하지 마세요. 이상행동이 명확하지 않으면 '확인 필요'로 표현하세요.\n"
        f"현장건강이벤트={json_dumps(context)}\n"
        "코드블록 없이 JSON 하나만 출력하세요.\n"
        f"응답형식={_common_response_schema(target_type='site')}"
    )


def build_worker_regression_prompt(camera: dict, trigger: dict, yolo_context: dict | None = None) -> str:
    context = compact_worker_regression_trigger_for_prompt(trigger)
    return (
        build_common_autoregressive_vlm_prompt(camera, yolo_context)
        + "판단유형=worker_regression\n"
        "관점: 특정 작업자 건강 위험도 판단입니다. 심박/기준심박/건강요인/체온 등 개인 건강 데이터만 보세요.\n"
        "작업장 온도/습도/열지수 등 환경 데이터는 이 판단에 사용하지 마세요.\n"
        "작업자를 화면에서 특정 못하면 worker_location=same_space_unknown, 화면상 증상은 단정 금지.\n"
        "회귀 결과가 약한휴식권고이면 risk_level은 최소 medium, 강한휴식권고는 high, 반드시 휴식은 critical로 보세요.\n"
        "휴식권고가 있으면 CCTV에 사람이 안 보여도 no_action을 쓰지 말고 작업자 휴식/관리자 확인 조치를 recommended_actions에 넣으세요.\n"
        "[비틀거림·신체 이상 탐지 - pose estimation 없이 VLM으로 판단]\n"
        "① 현재 이미지에서 사람의 자세를 보세요: 직립=upright, 기대거나 몸이 기울어짐=leaning, 쭈그리거나 구부러짐=crouching, 쓰러짐=fallen.\n"
        "② YOLO 10초 이력(person_movement.delta)에서 이동 패턴을 보세요: delta 값이 크거나 방향이 불규칙하면 staggering 가능성. 작고 일정하면 normal.\n"
        "③ abnormal_behavior 필드에 아래 중 하나를 반드시 채우세요:\n"
        "   'staggering'(비틀거림), 'falling'(쓰러짐·낙상), 'slumping'(축 처짐), 'crouching'(쭈그림), 'leaning'(기댐), 'normal'(정상), 'not_visible'(화면 미확인)\n"
        "④ staggering 또는 falling 탐지 시 risk_level을 최소 high로 올리고, recommended_actions에 즉각 현장 확인을 추가하세요.\n"
        "⑤ 화면에 사람이 없으면 abnormal_behavior='not_visible'로 쓰고, YOLO 이력만 참고해 판단하세요.\n"
        f"작업자회귀판단={json_dumps(context)}\n"
        "코드블록 없이 JSON 하나만 출력하세요.\n"
        f"응답형식={_common_response_schema(target_type='worker')}"
    )


def build_retry_prompt(camera: dict, trigger: dict, yolo_context: dict, *, mode: str = "worker_regression") -> str:
    max_chars = int(getattr(settings, "camera_vlm_retry_prompt_max_chars", 900) or 900)
    camera_text = json_dumps(compact_camera_for_prompt(camera))
    trigger_text = limit_text(json_dumps(trigger), 300)
    yolo_text = limit_text(str((yolo_context or {}).get("normalized_text") or ""), 280)
    hazard_type = extract_hazard_type(camera, trigger)
    target_type = "site" if mode == "environment" else "worker"
    prompt = (
        "분석 명칭: autoregressive VLM. 의료 진단 금지, 산업 안전 안내만 작성.\n"
        "반드시 모든 응답 필드 값을 한국어로 작성하세요. 영어 사용 금지.\n"
        "현재 이미지가 최우선, YOLO 이력은 보조 근거입니다. 보이지 않는 위험은 확정하지 마세요.\n"
        f"판단유형={mode}, target.type={target_type}. 코드블록 없이 JSON 하나만 답하세요.\n"
        f"응답형식={_common_response_schema(target_type=target_type)}\n"
        f"카메라={camera_text}\n등록위험물={hazard_type}\n이벤트={trigger_text}\nYOLO={yolo_text}"
    )
    return limit_text(prompt, max_chars)


def compact_environment_trigger_for_prompt(camera: dict, trigger: dict) -> dict:
    prediction = _prediction_or_trigger(trigger)
    health_attention = (
        prediction.get("health_attention")
        if isinstance(prediction.get("health_attention"), dict)
        else {}
    )

    return {
        "target": {
            "type": "site",
            "site_id": prediction.get("space_id") or camera.get("space_id"),
            "site_name": prediction.get("space_name") or camera.get("space_name"),
        },
        "health_attention": {
            "total_worker_count": health_attention.get("total_worker_count", 0),
            "attention_worker_count": health_attention.get("attention_worker_count", 0),
            "abnormal_hr_count": health_attention.get("abnormal_hr_count", 0),
            "risk_factors": health_attention.get("risk_factors", {}),
            "summary": health_attention.get("summary", "건강상 주의가 필요한 작업자 정보 없음"),
        },
        "triggered_at": prediction.get("triggered_at") or trigger.get("triggered_at"),
    }


def compact_worker_regression_trigger_for_prompt(trigger: dict) -> dict:
    prediction = _prediction_or_trigger(trigger)
    worker = prediction.get("worker") if isinstance(prediction.get("worker"), dict) else {}
    measurements = prediction.get("measurements") if isinstance(prediction.get("measurements"), dict) else {}
    health_profile = prediction.get("health_profile") if isinstance(prediction.get("health_profile"), dict) else {}

    return {
        "target": {
            "type": "worker",
            "site_id": worker.get("space_id") or measurements.get("space_id"),
            "site_name": worker.get("space_name") or measurements.get("space_name"),
            "worker_id": worker.get("dept_id") or trigger.get("worker_id"),
            "worker_name": worker.get("name"),
            "watch_sensor_id": worker.get("watch_sensor_id") or trigger.get("watch_sensor_id"),
        },
        "regression_result": {
            "result": prediction.get("result"),
            "reason": prediction.get("reason"),
            "rest_reason_detail": prediction.get("rest_reason_detail"),
            "risk_level_hint": _rest_result_to_risk_level(prediction.get("result")),
            "probabilities": prediction.get("probabilities"),
        },
        "biometrics": {
            "hr": measurements.get("hr"),
            "baseline_hr": measurements.get("baseline_hr") or prediction.get("baseline_hr"),
            "hr_delta_from_baseline": prediction.get("hr_delta_from_baseline"),
            "body_temperature_c": measurements.get("body_temperature_c"),
        },
        "health_profile": {
            "age": health_profile.get("age"),
            "gender": health_profile.get("gender"),
            "elderly_flag": health_profile.get("elderly_flag"),
            "heart_disease": health_profile.get("heart_disease"),
            "hypertension": health_profile.get("hypertension"),
            "other_disease": health_profile.get("other_disease"),
            "risk_factors": health_risk_factor_names(health_profile),
        },
        "triggered_at": trigger.get("triggered_at"),
    }


def format_yolo_context_for_prompt(yolo_context: dict | None) -> str:
    if not yolo_context:
        return "YOLO정규화메타={}\nYOLO탐지요약={}\nYOLO정규화텍스트=\n[YOLO compact history]\nmissing"

    public = public_yolo_context(yolo_context)
    metadata = {
        "source": public.get("source"),
        "camera_id": public.get("camera_id"),
        "frame_count": public.get("frame_count"),
        "sampled_count": public.get("sampled_count"),
        "generated_at": public.get("generated_at"),
        "normalized_text_length": public.get("normalized_text_length"),
    }
    detection_summary = public.get("detection_summary") or {}
    text = str(yolo_context.get("normalized_text") or "")
    max_chars = int(getattr(settings, "cctv_vlm_yolo_context_prompt_max_chars", 420) or 420)
    return (
        f"YOLO정규화메타={json_dumps(metadata)}\n"
        f"YOLO탐지요약={json_dumps(detection_summary)}\n"
        "YOLO정규화텍스트=\n"
        f"{limit_text(text, max_chars)}"
    )


def compact_camera_for_prompt(camera: dict) -> dict:
    return {
        "sen_id": camera.get("sen_id"),
        "space_id": camera.get("space_id"),
        "space_name": camera.get("space_name"),
        "hazard_type": camera.get("hazard_type"),
        "is_hazard": as_bool(camera.get("is_hazard")),
    }


def hazard_response_guide(hazard_type: str) -> str:
    normalized = str(hazard_type or "").strip()
    if not normalized or normalized in {"none", "unknown"}:
        return "등록 위험물 정보가 없으므로 현장 안전관리자 지시에 따라 접근을 제한하고 작업자를 안전 구역으로 이동시킨다."

    lower = normalized.lower()
    for key, guide in HAZARD_RESPONSE_GUIDES.items():
        if key.lower() in lower or lower in key.lower():
            return guide
    return (
        f"{normalized}: 등록된 유의물질입니다. 직접 접촉·흡입·가열을 피하고, "
        "작업자를 위험물 반대 방향의 안전 구역으로 이동시키며 현장 안전관리자와 전문 대응팀에 즉시 알린다."
    )


def health_risk_factor_names(health_profile: dict) -> list[str]:
    items = []
    if as_bool(health_profile.get("elderly_flag")):
        items.append("고령")
    if as_bool(health_profile.get("heart_disease")):
        items.append("심장질환")
    if as_bool(health_profile.get("hypertension")):
        items.append("고혈압")
    if as_bool(health_profile.get("other_disease")):
        items.append("기타질환")
    return items


def calc_heat_index_c(temp_c: Any, rh: Any) -> float | None:
    try:
        from ai.rest.rest_calculator import RestCalculator

        if temp_c is None or rh is None:
            return None
        return RestCalculator.calc_heat_index_c(float(temp_c), float(rh))
    except Exception:
        return None


def extract_hazard_type(camera: dict, trigger: dict) -> str:
    prediction = trigger.get("prediction") if isinstance(trigger, dict) else {}
    if isinstance(prediction, dict) and prediction.get("hazard_type"):
        return str(prediction.get("hazard_type"))
    if isinstance(trigger, dict) and trigger.get("hazard_type"):
        return str(trigger.get("hazard_type"))
    return str(camera.get("hazard_type") or "none")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def limit_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[: max(0, max_chars - len(marker))] + marker


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _prediction_or_trigger(trigger: dict) -> dict:
    prediction = trigger.get("prediction") if isinstance(trigger, dict) else {}
    if isinstance(prediction, dict) and prediction:
        return prediction
    return trigger if isinstance(trigger, dict) else {}


def _risk_factor_action_hints(risk_factors: dict) -> str:
    hints = []
    if risk_factors.get("elderly", 0):
        hints.append("고령→고강도 작업 중단·시원한 장소 이동·수분 보충")
    if risk_factors.get("hypertension", 0):
        hints.append("고혈압→즉시 휴식·혈압 측정·무거운 물건 취급 제한")
    if risk_factors.get("heart_disease", 0):
        hints.append("심장질환→작업 즉시 중단·의료진 연락")
    if risk_factors.get("other_disease", 0):
        hints.append("기타질환→이상 증상 보고 유도·안정 취하도록 안내")
    if not hints:
        hints.append("이상 증상 발생 시 즉시 보고·안전한 장소 대기")
    return " / ".join(hints)


def _common_response_schema(*, target_type: str) -> str:
    if target_type == "site":
        schema = {
            "risk_level": "",
            "summary": "",
            "field_status": "",
            "worker_movements": "",
            "abnormal_behavior": "staggering|falling|slumping|crouching|leaning|normal|not_visible 중 하나",
            "health_risk_summary": "",
            "worker_risks": [],
            "recommended_actions": [],
            "target": {
                "type": "site",
                "site_id": "",
                "site_name": "",
            },
        }
    else:
        schema = {
            "risk_level": "none|low|medium|high|critical 중 하나",
            "summary": "",
            "reason": "",
            "health_considerations": "",
            "recommended_actions": [],
            "target": {
                "type": "worker",
                "site_id": "",
                "site_name": "",
                "worker_id": "작업자 ID",
                "worker_name": "작업자명",
            },
            "visible_people": "",
            "worker_location": "",
            "abnormal_behavior": "staggering|falling|slumping|crouching|leaning|normal|not_visible 중 하나",
            "behavior_observation": {
                "posture": "upright|leaning|crouching|fallen|unknown",
                "movement_pattern": "normal|irregular|staggering|stationary|unknown",
                "confidence": "high|medium|low",
                "detail": "화면에서 관찰한 자세·이동 패턴 자유 묘사 (한국어)",
            },
            "visible_risks": [],
            "recommended_action": "",
        }
    return json_dumps(schema)


def _rest_result_to_risk_level(result: Any) -> str:
    text = str(result or "")
    if "반드시" in text:
        return "critical"
    if "강한" in text:
        return "high"
    if "약한" in text:
        return "medium"
    return "unknown"
