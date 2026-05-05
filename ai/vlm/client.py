"""vLLM 서버 HTTP 클라이언트 (OpenAI 호환 API)."""
import base64
import logging
from pathlib import Path

import httpx
from openai import AsyncOpenAI

from config import settings

logger = logging.getLogger(__name__)

_client: AsyncOpenAI | None = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=settings.vllm_base_url,
            api_key=settings.vllm_api_key,
            http_client=httpx.AsyncClient(timeout=60.0),
        )
    return _client


def _encode_image(frame_path: str) -> str:
    return base64.b64encode(Path(frame_path).read_bytes()).decode("utf-8")


async def query_with_image(prompt: str, frame_path: str) -> str:
    client = get_client()
    image_b64 = _encode_image(frame_path)

    response = await client.chat.completions.create(
        model=settings.vllm_model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        max_tokens=1024,
        temperature=0.2,
    )
    return response.choices[0].message.content or ""


async def query_text_only(prompt: str) -> str:
    client = get_client()
    response = await client.chat.completions.create(
        model=settings.vllm_model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
        temperature=0.2,
    )
    return response.choices[0].message.content or ""
