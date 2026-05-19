"""FirePipeline 안전 분석 autoregressive VLM.

이미지 + YOLO 이력 + 작업장 DB 메타데이터를 입력받아 앱 알림용 JSON을 생성한다.
"""
import json
import time

from core.vlm_prompt_builder import hazard_response_guide
from cctv.fire_pipeline.vlm_client import OpenAiCompatibleVlm, extract_json


def limit_text(text, max_chars):
    if len(text) <= max_chars:
        return text

    marker = "\n...[truncated]"
    return text[:max_chars - len(marker)] + marker


def format_stream_text(text):
    lines = []

    for line in text.splitlines():
        line = line.strip()

        if line != "":
            lines.append(line)

    if len(lines) == 0:
        return "현재 autoregressive VLM 응답 생성 중..."

    return "\n".join(lines[:3])


def keep_three_lines(text):
    lines = []

    for line in text.splitlines():
        line = line.strip()

        if line != "":
            lines.append(line)

    if len(lines) > 3:
        lines = lines[:3]

    while len(lines) < 3:
        if len(lines) == 0:
            lines.append("이동 경로: 현재 화면 기준 이동을 판단하기 어렵습니다.")
        elif len(lines) == 1:
            lines.append("상황 분석: 화면 근거가 부족합니다.")
        else:
            lines.append("안전 정보: 화재 대피 방향으로 대피를 유지하세요.")

    return "\n".join(lines)


def json_dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


class SafetyAnalysisVlm:
    def __init__(self, config, client=None):
        if client is None:
            client = OpenAiCompatibleVlm(
                config.vllm_base_url,
                config.vllm_api_key,
                config.vllm_model,
                config.vllm_timeout
            )

        self.client = client
        self.config = config

    def make_prompt(self, normalized_text, validation, workplace_info=None):
        workplace = self.compact_workplace_info(workplace_info or getattr(self.config, "workplace_info", {}))
        validation_text = json.dumps(self.compact_validation(validation), ensure_ascii=False)
        hazard_material = str(workplace.get("hazard_type") or "none")
        hazard_guide = hazard_response_guide(hazard_material)
        response_schema = self.response_schema(workplace, hazard_guide)
        prefix = (
            "분석 명칭: autoregressive VLM.\n"
            "공통 CCTV 분석 계층:\n"
            "1. 최근 10초 CCTV 프레임을 YOLO(person, fire, smoke)로 분석하고 저장합니다.\n"
            "2. 저장된 프레임의 YOLO 결과를 정규화 텍스트로 요약합니다.\n"
            "3. 위험감지 이벤트가 발생하면 현재 이미지와 YOLO 정규화 텍스트를 함께 보고 판단합니다.\n"
            "분석 목적: 관리자 앱 알람에 넣을 화재/연기 상황, 작업장 위치, 화재원인, 확산경로, 조치 방법을 생성합니다.\n"
            "이미지에 현재 보이는 내용을 가장 우선으로 분석하세요. "
            "YOLO 정규화 이력은 사람 이동/위험 객체 흐름 보정용 보조 근거입니다.\n"
            "마지막 화면에 안 보이는 사람/객체는 현재 이동 중이라고 말하지 마세요.\n"
            "YOLO fire/smoke는 오탐일 수 있습니다. 현재 이미지에서 불꽃/연기가 명확할 때만 화재/연기라고 확정하세요.\n"
            "명확하지 않으면 '화재 의심/확인 필요'라고 쓰고, 불/연기가 고정되어 있다거나 이동한다고 단정하지 마세요.\n"
            "불/연기 이상상황, 사람 이동 위치, 최근 이력, 작업장 정보를 반영해 안전 정보를 반환하세요.\n"
            "검증 person 값에 옷 색/보호구 단서가 있으면 이동 경로 줄에 함께 써서 관리자가 사람을 구분하게 하세요.\n"
            "작업장 정보는 DB 등록값입니다. 화면에 위험물이 직접 보이지 않아도, 등록 위험물의 주의 조치는 반드시 작성하세요.\n"
            "화재원인은 화면/YOLO 근거로 가능한 원인을 쓰되, 확실하지 않으면 '원인 확인 필요'로 작성하세요.\n"
            "확산경로는 불꽃/연기 위치와 방향이 불명확하면 '확산 방향 확인 필요'로 작성하세요.\n"
            "모든 필드는 항상 채우세요. 모르면 unknown 또는 확인 필요를 쓰세요. 빈 문자열 금지.\n"
            "코드블록 없이 JSON 하나만 출력하세요. 모든 값은 한국어로 작성하세요.\n"
            "앱 알람용 짧은 문장을 우선하고, 장문 설명은 피하세요.\n"
            "검증 corrected_direction은 현재 사람이 보일 때만 반영하세요.\n"
            f"작업장정보={json_dumps(workplace)}\n"
            f"위험물조치가이드={hazard_guide}\n"
            f"응답형식={json_dumps(response_schema)}\n"
            "검증="
        )
        middle = validation_text + "\n이력="
        allowed = self.config.analysis_prompt_max_chars - len(prefix) - len(middle)

        if allowed < 0:
            allowed = 0

        return limit_text(
            prefix + middle + limit_text(normalized_text, allowed),
            self.config.analysis_prompt_max_chars
        )

    def compact_workplace_info(self, info):
        info = info if isinstance(info, dict) else {}
        hazard_type = str(info.get("hazard_type") or "none").strip() or "none"
        space_name = str(info.get("space_name") or "작업장 위치 확인 필요").strip()
        return {
            "camera_sen_id": info.get("sen_id") or info.get("camera_sen_id") or getattr(self.config, "camera_id", None),
            "sensor_id": info.get("sensor_id") or "",
            "camera_name": info.get("sen_name") or info.get("camera_name") or "CCTV",
            "space_id": info.get("space_id"),
            "space_name": space_name,
            "is_hazard": bool(info.get("is_hazard", False)),
            "hazard_type": hazard_type,
            "source_type": info.get("source_type") or "",
        }

    def response_schema(self, workplace, hazard_guide):
        return {
            "risk_level": "warning|danger|critical 중 하나",
            "summary": "앱 알람 한 줄 요약",
            "screen_analysis": "현재 화면에서 보이는 불꽃/연기/사람/주변 상태",
            "workplace_location": {
                "space_id": workplace.get("space_id"),
                "space_name": workplace.get("space_name") or "작업장 위치 확인 필요",
                "camera_sen_id": workplace.get("camera_sen_id"),
                "camera_name": workplace.get("camera_name") or "CCTV",
            },
            "fire_cause": {
                "cause": "가능한 화재 원인 또는 원인 확인 필요",
                "evidence": "화면/YOLO 근거",
                "confidence": "high|medium|low",
            },
            "spread_path": {
                "direction": "확산 방향 또는 확인 필요",
                "route": "확산 가능 경로",
                "evidence": "불꽃/연기 위치·이동 근거",
            },
            "visible_people": "사람 수와 위치, 없으면 없음",
            "person_movement": "이동 방향과 구분 단서, 없으면 없음",
            "hazard_material": workplace.get("hazard_type") or "none",
            "hazard_warning": hazard_guide,
            "hazard_specific_action": hazard_guide,
            "evacuation_route": "위험물과 화재/연기를 피하는 대피 방향",
            "recommended_actions": [
                "즉시 관리자 확인",
                "작업자 대피 안내",
                "위험물별 조치",
            ],
            "target": {
                "type": "site",
                "site_id": workplace.get("space_id"),
                "site_name": workplace.get("space_name") or "작업장 위치 확인 필요",
            },
        }

    def compact_validation(self, validation):
        return {
            "valid": validation.get("movement_valid", False),
            "dir": validation.get("corrected_direction", validation.get("person_direction", "unknown")),
            "fire_dir": validation.get("fire_direction", "unknown"),
            "smoke_dir": validation.get("smoke_direction", "unknown"),
            "risk": validation.get("risk_level", "unknown"),
            "person": validation.get("person_appearance", "unknown"),
            "sit": validation.get("situation", ""),
            "why": validation.get("reason_prediction", ""),
            "ev": validation.get("evidence", "")
        }

    def fallback_response(self, raw_text, validation=None, workplace_info=None):
        workplace = self.compact_workplace_info(workplace_info or getattr(self.config, "workplace_info", {}))
        hazard_material = workplace.get("hazard_type") or "none"
        hazard_guide = hazard_response_guide(hazard_material)
        validation = self.compact_validation(validation or {})
        return {
            "risk_level": "danger",
            "summary": f"{workplace.get('space_name')}에서 화재/연기 감지 알람이 발생했습니다.",
            "screen_analysis": str(raw_text or "화면 분석 결과 확인 필요"),
            "workplace_location": {
                "space_id": workplace.get("space_id"),
                "space_name": workplace.get("space_name"),
                "camera_sen_id": workplace.get("camera_sen_id"),
                "camera_name": workplace.get("camera_name"),
            },
            "fire_cause": {
                "cause": "원인 확인 필요",
                "evidence": validation.get("sit") or validation.get("ev") or "YOLO fire/smoke 감지",
                "confidence": "low",
            },
            "spread_path": {
                "direction": validation.get("fire_dir") or validation.get("smoke_dir") or "확인 필요",
                "route": "화재/연기 주변으로 확산 가능성 확인 필요",
                "evidence": "YOLO 10초 이력과 현재 이미지 기준",
            },
            "visible_people": validation.get("person") or "unknown",
            "person_movement": validation.get("dir") or "unknown",
            "hazard_material": hazard_material,
            "hazard_warning": hazard_guide,
            "hazard_specific_action": hazard_guide,
            "evacuation_route": "연기와 위험물 반대 방향의 안전 구역으로 대피",
            "recommended_actions": [
                "즉시 현장 확인 및 작업 중지",
                "작업자를 연기와 위험물 반대 방향으로 대피 안내",
                hazard_guide,
            ],
            "target": {
                "type": "site",
                "site_id": workplace.get("space_id"),
                "site_name": workplace.get("space_name"),
            },
            "raw_text": str(raw_text or ""),
        }

    def normalize_response(self, data, raw_text, validation=None, workplace_info=None):
        if not isinstance(data, dict):
            data = {}
        fallback = self.fallback_response(raw_text, validation, workplace_info)
        result = {**fallback, **data}

        for key in (
            "summary",
            "screen_analysis",
            "visible_people",
            "person_movement",
            "hazard_material",
            "hazard_warning",
            "hazard_specific_action",
            "evacuation_route",
        ):
            if not result.get(key):
                result[key] = fallback[key]

        for key in ("workplace_location", "fire_cause", "spread_path", "target"):
            if not isinstance(result.get(key), dict):
                result[key] = fallback[key]
            else:
                result[key] = {**fallback[key], **result[key]}

        actions = result.get("recommended_actions")
        if not isinstance(actions, list) or len(actions) == 0:
            result["recommended_actions"] = fallback["recommended_actions"]

        return result

    def emit_typing_text(self, text, on_text):
        if on_text is None:
            return

        shown = ""

        for char in text:
            shown = shown + char
            on_text(shown)

            if self.config.typing_fallback_delay > 0:
                time.sleep(self.config.typing_fallback_delay)

    def analyze(self, normalized_text, validation, image_path, on_text=None, workplace_info=None):
        prompt = self.make_prompt(normalized_text, validation, workplace_info)

        try:
            raw = self.client.request_text(
                prompt,
                image_path,
                self.config.analysis_vlm_max_tokens,
                self.config.analysis_vlm_temperature,
                False,
                on_text
            )
        except Exception:
            raw = self.client.request_text(
                prompt,
                image_path,
                self.config.analysis_vlm_max_tokens,
                self.config.analysis_vlm_temperature,
                False,
                None
            )
            self.emit_typing_text(raw, on_text)

        try:
            parsed = extract_json(raw)
        except Exception:
            parsed = {}

        return self.normalize_response(parsed, raw, validation, workplace_info)
