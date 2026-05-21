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
        "산업 안전 안내만 작성하고, 의료 진단은 금지하세요.\n"
        "현재 CCTV 이미지와 YOLO 10초 정규화 이력을 함께 참고하세요.\n"
        "현재 화면에 보이지 않는 것은 확정하지 말고, 확인 필요라고 작성하세요.\n"
        "사람의 옷 색, 안전모 색, 조끼 색 등 외형 색상으로 작업자를 구분하거나 특정하지 마세요.\n"
        "영상 분석은 현장 요약과 사람의 위치·이동 방향 설명에 집중하세요.\n"
        "YOLO 이력은 사람의 이동 방향과 위치 변화 판단에만 사용하세요.\n"
        "비틀거림, 낙상, 건강 이상 같은 행동 이상은 불확실하면 단정하지 마세요.\n"
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
        "─ person_identification: 움직이는 사람 또는 건강 위험 판단 대상 후보의 구분 단서를 작성하세요.\n"
        "  예) '화면 중앙의 노란 안전모·주황 조끼 작업자, 오른쪽으로 이동'. 얼굴/이름/신원은 추정 금지.\n"
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

def compact_environment_for_prompt(trigger: dict, camera: dict) -> dict:
    prediction = _prediction_or_trigger(trigger)
    env = (
        prediction.get("environment")
        or trigger.get("environment")
        or camera.get("environment")
        or {}
    )

    return {
        "air_temperature_c": env.get("air_temperature_c") or env.get("temperature") or env.get("temp"),
        "humidity_pct": env.get("humidity_pct") or env.get("humidity"),
        "heat_index_c": env.get("heat_index_c") or env.get("heat_index"),
        "environment_risk_level": env.get("risk_level"),
    }

def build_worker_regression_prompt(
    camera: dict,
    trigger: dict,
    yolo_context: dict | None,
) -> str:
    worker_health_data = compact_worker_regression_trigger_for_prompt(trigger)
    environment_data = compact_environment_for_prompt(trigger, camera)

    return (
        build_common_autoregressive_vlm_prompt(camera, yolo_context)

        + "판단유형=worker_regression\n"
        + "응답 형식: 반드시 JSON만 출력하세요. 마크다운, 코드블록, 설명문은 출력하지 마세요.\n"
        + "출력 필드는 반드시 아래 3개만 사용하세요.\n"
        + "1. 영상 분석 결과 텍스트 전달\n"
        + "2. 상황 조치 방법\n"
        + "3. 건강정보 전달\n"
        + "위 3개 필드 외에 risk_level, abnormal_behavior, behavior_observation, person_identification, recommended_actions 같은 별도 필드는 출력하지 마세요.\n"

        + "\n[데이터 사용 원칙]\n"
        + "작업자_건강데이터는 '건강정보 전달' 작성에 사용하세요.\n"
        + "작업장_환경데이터는 '상황 조치 방법' 작성에만 사용하세요.\n"
        + "작업장 온도, 습도, 열지수는 건강 이상 판단 근거로 사용하지 마세요.\n"
        + "건강정보 전달은 심박수, 기준심박수, 체온, 건강요인, 회귀 결과, 휴식권고 결과만 기준으로 작성하세요.\n"

        + "\n[영상 분석 결과 텍스트 전달 작성 기준]\n"
        + "현재 CCTV 장면을 간단히 요약하세요.\n"
        + "사람이 보이면 화면 기준 위치와 이동 방향을 설명하세요.\n"
        + "YOLO 10초 이력의 person_movement.delta, bbox center 변화, frame별 위치 변화를 이용해 사람이 어디에서 어디로 이동했는지 작성하세요.\n"
        + "사람이 보이지 않으면 '현재 화면에서 작업자는 확인되지 않습니다'라고 작성하세요.\n"
        + "옷 색, 안전모 색, 조끼 색으로 사람을 구분하거나 특정하지 마세요.\n"

        + "\n[상황 조치 방법 작성 기준]\n"
        + "현장 상황을 보고 관리자가 해야 할 조치사항을 작성하세요.\n"
        + "작업장 온도, 습도, 열지수, 화재, 연기, 스파크, 쓰러진 사람, 위험 구역 접근, 위험물 취급 공간 여부를 참고하세요.\n"
        + "작업장 온도나 열지수가 높으면 환기, 냉방, 그늘 확보, 작업 강도 조절, 휴식 안내, 수분 섭취 안내를 포함하세요.\n"
        + "화재, 연기, 스파크가 보이면 즉시 작업 중지, 현장 대피, 전원 차단, 관리자 확인, 소화 장비 확인을 포함하세요.\n"
        + "사람이 쓰러진 것으로 명확히 보이면 즉시 현장 확인, 주변 작업 중지, 응급 연락, 의식 및 호흡 확인을 포함하세요.\n"
        + "위험물 취급 공간이면 해당 물질의 접촉, 누출, 화기 접근 가능성을 확인하라고 작성하세요.\n"

        + "\n[건강정보 전달 작성 기준]\n"
        + "해당 작업자의 개인 건강 데이터 기반 이상 사항과 조치사항을 작성하세요.\n"
        + "심박수, 기준심박수, 체온, 건강요인, 회귀 결과, 휴식권고 결과만 사용하세요.\n"
        + "작업장 온도, 습도, 열지수는 건강정보 판단 근거로 사용하지 마세요.\n"
        + "심박이 기준보다 높거나 체온 이상, 건강 위험 요인, 휴식권고가 있으면 어떤 이상이 있는지와 필요한 조치를 함께 작성하세요.\n"
        + "약한휴식권고이면 휴식 권고와 관리자 확인을 작성하세요.\n"
        + "강한휴식권고이면 즉시 작업 중지, 휴식, 관리자 확인을 작성하세요.\n"
        + "반드시 휴식이면 즉시 작업 중지, 안전한 장소 이동, 관리자 또는 응급 담당자 확인을 작성하세요.\n"
        + "CCTV에 사람이 보이지 않아도 개인 건강 데이터에 휴식권고가 있으면 휴식 및 관리자 확인 조치를 작성하세요.\n"
        + "건강 이상이 명확하지 않으면 '현재 전달된 개인 건강 데이터 기준으로 뚜렷한 이상 징후는 확인되지 않습니다'라고 작성하세요.\n"

        + f"\n작업자_건강데이터={json_dumps(worker_health_data)}\n"
        + f"작업장_환경데이터={json_dumps(environment_data)}\n"
        + f"응답스키마={_common_response_schema(target_type='worker')}\n"
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
            "risk_level": "none|low|medium|high|critical 중 하나",
            "summary": "관리자가 한눈에 이해할 수 있는 현장 상황 한 줄 요약",
            "field_status": "현재 화면에서 보이는 현장 상황을 구체적으로 묘사. 사람 수·위치·자세·행동 포함",
            "worker_movements": "YOLO 이력 기반 사람 이동 패턴 서술. 몇 명이 어느 방향으로 어떤 속도로 이동했는지 포함",
            "abnormal_behavior": "staggering|falling|slumping|crouching|leaning|normal|not_visible 중 하나",
            "health_risk_summary": "건강상 주의가 필요한 작업자 수와 위험 요인 한 문장 요약",
            "worker_risks": [],
            "recommended_actions": ["무엇을 → 어떻게가 명확한 조치 한 문장"],
            "target": {"type": "site", "site_id": "", "site_name": ""},
        }
    else:
        schema = {
            "risk_level": "none|low|medium|high|critical 중 하나",
            "summary": "작업자 현재 상태 한 줄 요약",
            "reason": "위험 판단 근거",
            "health_considerations": "건강 위험 요인과 주의사항",
            "recommended_actions": ["무엇을 → 어떻게가 명확한 조치 한 문장"],
            "target": {
                "type": "worker",
                "site_id": "",
                "site_name": "",
                "worker_id": "작업자 ID",
                "worker_name": "작업자명",
            },
            "visible_people": "화면에 보이는 사람 수와 위치",
            "worker_location": "same_space_unknown|identified|not_visible",
            "abnormal_behavior": "staggering|falling|slumping|crouching|leaning|normal|not_visible 중 하나",
            "behavior_observation": {
                "posture": "upright|leaning|crouching|fallen|unknown",
                "movement_pattern": "normal|irregular|staggering|stationary|unknown",
                "confidence": "high|medium|low",
                "detail": "현재 이미지와 YOLO 이동 이력을 바탕으로 자세·이동 패턴을 구체적으로 묘사. 방향·속도 변화·불규칙성 포함",
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
