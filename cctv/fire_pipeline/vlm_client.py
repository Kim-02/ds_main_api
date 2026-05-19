"""OpenAI-compatible 동기 VLM 클라이언트 — export_final.vlm_client 원본과 동일."""
import base64
import json
import mimetypes
from pathlib import Path

from openai import OpenAI

from ai.vlm.timeshare import run_vlm_timeshare


def image_to_data_url(image_path):
    path = Path(image_path)

    if not path.exists():
        raise FileNotFoundError("이미지 파일을 찾을 수 없습니다: " + str(image_path))

    mime_type, _ = mimetypes.guess_type(path)

    if mime_type is None:
        mime_type = "image/jpeg"

    with open(path, "rb") as file:
        image_base64 = base64.b64encode(file.read()).decode("utf-8")

    return "data:" + mime_type + ";base64," + image_base64


def make_content(prompt, image_path):
    content = [
        {
            "type": "text",
            "text": prompt
        }
    ]

    if image_path != "":
        content.append({
            "type": "image_url",
            "image_url": {
                "url": image_to_data_url(image_path)
            }
        })

    return content


def extract_json(text):
    text = text.strip()

    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    start_object = text.find("{")
    start_array = text.find("[")

    if start_object == -1:
        start = start_array
    elif start_array == -1:
        start = start_object
    else:
        start = min(start_object, start_array)

    if start == -1:
        raise ValueError("VLM 응답에서 JSON을 찾지 못했습니다.")

    end_object = text.rfind("}")
    end_array = text.rfind("]")
    end = max(end_object, end_array)

    if end == -1 or end < start:
        raise ValueError("VLM 응답 JSON 범위가 올바르지 않습니다.")

    return json.loads(text[start:end + 1])


class OpenAiCompatibleVlm:
    def __init__(self, base_url, api_key, model, timeout=30, client=None):
        self.base_url = base_url
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

        if client is None:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=self.timeout,
                # FirePipeline은 실시간 알림 파이프라인이므로 OpenAI SDK 내부 재시도로
                # 30초 타임아웃이 90초 이상 늘어나는 상황을 피한다.
                max_retries=0,
            )
        else:
            self.client = client

    def request_text(self, prompt, image_path="", max_tokens=256, temperature=0.2, stream=False, on_text=None):
        label = _make_timeshare_label(self.model, image_path, stream)

        def _request():
            content = make_content(prompt, image_path)

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": content
                    }
                ],
                max_tokens=max_tokens,
                temperature=temperature,
                stream=stream
            )

            if stream == False:
                text = response.choices[0].message.content

                if on_text is not None:
                    on_text(text)

                return text

            text = ""

            for chunk in response:
                if len(chunk.choices) == 0:
                    continue

                delta = chunk.choices[0].delta

                if delta.content is None:
                    continue

                text = text + delta.content

                if on_text is not None:
                    on_text(text)

            return text

        return run_vlm_timeshare(label, _request)

    def request_json(self, prompt, image_path="", max_tokens=512, temperature=0.0):
        text = self.request_text(
            prompt,
            image_path,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=False
        )
        return extract_json(text)


def _make_timeshare_label(model: str, image_path: str, stream: bool) -> str:
    source = "text"
    if image_path:
        source = Path(image_path).name
    return f"cctv.fire_pipeline model={model} source={source} stream={stream}"
