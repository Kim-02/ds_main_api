"""FirePipeline 안전 분석 autoregressive VLM.

이미지 + YOLO 이력 + 작업장 DB 메타데이터를 입력받아 앱 알림용 JSON을 생성한다.
"""
import json
import logging
import time

from core.vlm_prompt_builder import hazard_response_guide
from cctv.fire_pipeline.vlm_client import OpenAiCompatibleVlm, extract_json

logger = logging.getLogger(__name__)


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


def direction_to_korean(value):
    text = str(value or "").strip().lower()
    if text in {"right", "east"}:
        return "오른쪽"
    if text in {"left", "west"}:
        return "왼쪽"
    if text in {"up", "north"}:
        return "위쪽"
    if text in {"down", "south"}:
        return "아래쪽"
    if text in {"none", "unknown", "null", ""}:
        return "확인 필요"
    return str(value)


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
        response_schema = self.response_schema()
        prefix = (
            "분석 명칭: autoregressive VLM.\n"
            "현재 이미지와 짧은 탐지 이력으로 현장 상황과 확산 방향만 판단하세요.\n"
            "마지막 화면에 없는 사람/객체는 현재 보인다고 쓰지 마세요.\n"
            "불꽃/연기가 불명확하면 확인 필요라고 쓰세요.\n"
            "작업장 위치와 위험물 조치 문구는 DB에서 채우므로 반복하지 마세요.\n"
            "현장 설명, 확산 방향, 사람 유무에 집중하세요.\n"
            "코드블록 없이 JSON 하나만 출력하세요. 모든 값은 매우 짧은 한국어 문장으로 작성하세요.\n"
            "빈 문자열 금지. 모르면 unknown 또는 확인 필요.\n"
            f"작업장={workplace.get('space_name') or 'unknown'},등록위험물={hazard_material}\n"
            f"응답형식={json_dumps(response_schema)}\n"
            "검증="
        )
        middle = validation_text + "\n탐지이력="
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

    def response_schema(self):
        return {
            "risk_level": "warning|danger|critical",
            "summary": "한 줄 알림",
            "screen_analysis": "불/연기/사람 상태 한 문장",
            "fire_cause": {
                "cause": "원인 또는 확인 필요",
                "confidence": "high|medium|low"
            },
            "spread_path": {
                "direction": "방향 또는 확인 필요",
                "route": "확산 가능 경로"
            },
            "visible_people": "사람 수/위치",
            "person_movement": "이동/구분 단서",
        }

    def compact_validation(self, validation):
        return {
            "valid": validation.get("movement_valid", False),
            "dir": validation.get("corrected_direction", validation.get("person_direction", "unknown")),
            "fire_dir": validation.get("fire_direction", "unknown"),
            "smoke_dir": validation.get("smoke_direction", "unknown"),
            "risk": validation.get("risk_level", "unknown"),
            "person": validation.get("person_appearance", "unknown"),
        }

    def fallback_screen_analysis(self, validation, workplace):
        fire_direction = direction_to_korean(validation.get("fire_dir"))
        smoke_direction = direction_to_korean(validation.get("smoke_dir"))
        space_name = workplace.get("space_name") or "작업장"
        parts = [f"{space_name}에서 화재/연기 위험이 감지되었습니다."]

        if fire_direction != "확인 필요":
            parts.append(f"불꽃 방향은 {fire_direction}으로 추정됩니다.")
        if smoke_direction != "확인 필요":
            parts.append(f"연기 방향은 {smoke_direction}으로 추정됩니다.")

        parts.append("현장 상황과 확산 경로 확인이 필요합니다.")

        return " ".join(parts)

    def fallback_response(self, raw_text, validation=None, workplace_info=None):
        workplace = self.compact_workplace_info(workplace_info or getattr(self.config, "workplace_info", {}))
        hazard_material = workplace.get("hazard_type") or "none"
        hazard_guide = hazard_response_guide(hazard_material)
        validation = self.compact_validation(validation or {})
        fire_direction = direction_to_korean(validation.get("fire_dir"))
        smoke_direction = direction_to_korean(validation.get("smoke_dir"))
        spread_direction = fire_direction if fire_direction != "확인 필요" else smoke_direction
        return {
            "risk_level": "danger",
            "summary": f"{workplace.get('space_name')}에서 화재/연기 감지 알람이 발생했습니다.",
            "screen_analysis": self.fallback_screen_analysis(validation, workplace),
            "workplace_location": {
                "space_id": workplace.get("space_id"),
                "space_name": workplace.get("space_name"),
                "camera_sen_id": workplace.get("camera_sen_id"),
                "camera_name": workplace.get("camera_name"),
            },
            "fire_cause": {
                "cause": "원인 확인 필요",
                "evidence": "현재 영상 분석 결과",
                "confidence": "low",
            },
            "spread_path": {
                "direction": spread_direction,
                "route": "화재/연기 주변으로 확산 가능성 확인 필요",
                "evidence": "현재 영상 분석 결과",
            },
            "visible_people": validation.get("person") or "확인 필요",
            "person_movement": direction_to_korean(validation.get("dir")),
            "hazard_material": hazard_material,
            "hazard_warning": hazard_guide,
            "hazard_specific_action": hazard_guide,
            "evacuation_route": "연기와 위험물 반대 방향의 안전 구역으로 대피",
            "recommended_actions": [
                "관리자가 즉시 현장을 확인하고 작업을 중지합니다.",
                "작업자를 연기 확산 반대 방향으로 대피시킵니다.",
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
        logger.info(
            "[FirePipeline] final VLM prompt prepared chars=%s max_tokens=%s timeout=%s image=%s",
            len(prompt),
            self.config.analysis_vlm_max_tokens,
            self.config.vllm_timeout,
            image_path,
        )

        try:
            raw = self.client.request_text(
                prompt,
                image_path,
                self.config.analysis_vlm_max_tokens,
                self.config.analysis_vlm_temperature,
                False,
                on_text
            )
        except Exception as exc:
            logger.exception(
                "[FirePipeline] final VLM request failed; fallback alert will be emitted error=%s",
                exc,
            )
            return self.fallback_response(
                "최종 autoregressive VLM 응답 시간 초과 또는 실패. YOLO fire/smoke 감지와 검증 결과를 기준으로 알림을 생성했습니다.",
                validation,
                workplace_info,
            )

        try:
            parsed = extract_json(raw)
        except Exception as exc:
            logger.warning(
                "[FirePipeline] final VLM JSON parse failed; normalized fallback fields will be used raw_len=%s error=%s",
                len(str(raw or "")),
                exc,
            )
            parsed = {}

        return self.normalize_response(parsed, raw, validation, workplace_info)
