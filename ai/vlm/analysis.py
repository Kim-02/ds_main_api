import json
import time

from ai.vlm.client import OpenAiCompatibleVlm


def limit_text(text, max_chars):
    if len(text) <= max_chars:
        return text
    marker = "\n...[truncated]"
    return text[:max_chars - len(marker)] + marker


def keep_three_lines(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    if len(lines) > 3:
        lines = lines[:3]

    while len(lines) < 3:
        if len(lines) == 0:
            lines.append("이동 경로: 현재 화면 기준 이동을 판단하기 어렵습니다.")
        elif len(lines) == 1:
            lines.append("상황 분석: 화면 근거가 부족합니다.")
        else:
            lines.append("안전 정보: 위험 반대 방향으로 대피를 유지하세요.")

    return "\n".join(lines)


class SafetyAnalysisVlm:
    def __init__(self, config, client=None):
        if client is None:
            client = OpenAiCompatibleVlm(
                config.vllm_base_url,
                config.vllm_api_key,
                config.vllm_model,
                config.vllm_timeout,
            )
        self.client = client
        self.config = config

    def make_prompt(self, normalized_text, validation):
        validation_text = json.dumps(self._compact_validation(validation), ensure_ascii=False)
        prefix = (
            "분석 명칭: autoregressive VLM.\n"
            "공통 CCTV 분석 계층:\n"
            "1. 최근 10초 CCTV 프레임을 YOLO(person, fire, smoke)로 분석하고 저장합니다.\n"
            "2. 저장된 프레임의 YOLO 결과를 정규화 텍스트로 요약합니다.\n"
            "3. 위험감지 이벤트가 발생하면 현재 이미지와 YOLO 정규화 텍스트를 함께 보고 판단합니다.\n"
            "이미지에 현재 보이는 내용을 가장 우선으로 분석하세요. "
            "YOLO 정규화 이력은 사람 이동/위험 객체 흐름 보정용 보조 근거입니다.\n"
            "마지막 화면에 안 보이는 사람/객체는 현재 이동 중이라고 말하지 마세요.\n"
            "불/연기 이상상황, 사람 이동 위치, 최근 이력을 반영해 안전 정보를 반환하세요.\n"
            "정확히 3줄만 출력하세요. 각 줄 40자 이내, 질문 금지.\n"
            "1줄: 이동 경로: ...\n"
            "2줄: 상황 분석: ...\n"
            "3줄: 안전 정보: ...\n"
            "검증 corrected_direction은 현재 사람이 보일 때만 반영하세요.\n"
            "검증="
        )
        middle = validation_text + "\n이력="
        allowed = self.config.analysis_prompt_max_chars - len(prefix) - len(middle)
        return limit_text(
            prefix + middle + limit_text(normalized_text, max(0, allowed)),
            self.config.analysis_prompt_max_chars,
        )

    def _compact_validation(self, validation):
        return {
            "valid": validation.get("movement_valid", False),
            "dir": validation.get("corrected_direction", validation.get("person_direction", "unknown")),
            "fire_dir": validation.get("fire_direction", "unknown"),
            "smoke_dir": validation.get("smoke_direction", "unknown"),
            "risk": validation.get("risk_level", "unknown"),
            "sit": validation.get("situation", ""),
            "why": validation.get("reason_prediction", ""),
            "ev": validation.get("evidence", ""),
        }

    def _emit_typing(self, text, on_text):
        if on_text is None:
            return
        shown = ""
        for char in text:
            shown += char
            on_text(shown)
            if self.config.typing_fallback_delay > 0:
                time.sleep(self.config.typing_fallback_delay)

    def analyze(self, normalized_text, validation, image_path, on_text=None):
        prompt = self.make_prompt(normalized_text, validation)
        try:
            raw = self.client.request_text(
                prompt, image_path,
                self.config.analysis_vlm_max_tokens,
                self.config.analysis_vlm_temperature,
                self.config.analysis_stream,
                on_text,
            )
        except Exception:
            raw = self.client.request_text(
                prompt, image_path,
                self.config.analysis_vlm_max_tokens,
                self.config.analysis_vlm_temperature,
                False, None,
            )
            self._emit_typing(raw, on_text)

        return keep_three_lines(raw)
