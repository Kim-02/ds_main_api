"""Runtime helpers for starting and checking the local vLLM server."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import shlex
import subprocess
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


logger = logging.getLogger(__name__)


def _expand_path(path: str) -> str:
    return str(Path(path).expanduser())


class VllmServerManager:
    """Start vLLM when needed and wait until the OpenAI-compatible API is ready."""

    def __init__(self, settings: Any):
        self.settings = settings
        self.process: subprocess.Popen | None = None
        self.started_by_manager = False
        self._log_handle = None

    @property
    def models_url(self) -> str:
        return self.settings.vllm_base_url.rstrip("/") + "/models"

    async def ensure_ready(self) -> bool:
        logger.info(
            "[VLLM] START ensure_ready autostart=%s base_url=%s model=%s",
            self.settings.vllm_autostart,
            self.settings.vllm_base_url,
            self.settings.vllm_model,
        )

        if await self.wait_ready(timeout_seconds=2, poll_seconds=0.5):
            logger.info("[VLLM] READY already running url=%s", self.models_url)
            return True

        if not self.settings.vllm_autostart:
            raise RuntimeError("VLLM_AUTOSTART=false 이고 VLM 서버가 준비되지 않았습니다.")

        self.start()

        ready = await self.wait_ready(
            timeout_seconds=self.settings.vllm_startup_timeout_seconds,
            poll_seconds=self.settings.vllm_startup_poll_seconds,
        )

        if not ready:
            status = self.status()
            raise RuntimeError(f"VLM 서버 준비 시간 초과: {status}")

        logger.info("[VLLM] END ensure_ready status=%s", self.status())
        return True

    def start(self) -> None:
        if self.process is not None and self.process.poll() is None:
            logger.info("[VLLM] process already started pid=%s", self.process.pid)
            return

        log_path = Path(self.settings.vllm_log_path).expanduser()

        if not log_path.is_absolute():
            log_path = Path.cwd() / log_path

        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = open(log_path, "ab", buffering=0)

        command = self.build_shell_command()
        logger.info("[VLLM] START process log=%s", log_path)
        logger.debug("[VLLM] command=%s", command)

        self.process = subprocess.Popen(
            ["bash", "-lc", command],
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.started_by_manager = True
        logger.info("[VLLM] process started pid=%s", self.process.pid)

    def build_shell_command(self) -> str:
        activate = _expand_path(self.settings.vllm_venv_activate)
        workdir = _expand_path(self.settings.vllm_workdir)
        model_path = self.settings.vllm_serve_model_path

        args = [
            "vllm",
            "serve",
            model_path,
            "--dtype",
            self.settings.vllm_dtype,
            "--host",
            self.settings.vllm_host,
            "--port",
            str(self.settings.vllm_port),
            "--api-key",
            self.settings.vllm_api_key,
            "--gpu-memory-utilization",
            str(self.settings.vllm_gpu_memory_utilization),
            "--max-model-len",
            str(self.settings.vllm_max_model_len),
            "--max-num-seqs",
            str(self.settings.vllm_max_num_seqs),
            "--max-num-batched-tokens",
            str(self.settings.vllm_max_num_batched_tokens),
            "--limit-mm-per-prompt",
            self.settings.vllm_limit_mm_per_prompt,
            "--trust-remote-code",
            "--enforce-eager",
        ]

        quoted_args = " ".join(shlex.quote(str(item)) for item in args)
        return (
            f"source {shlex.quote(activate)} "
            f"&& cd {shlex.quote(workdir)} "
            f"&& exec {quoted_args}"
        )

    async def wait_ready(self, timeout_seconds: float, poll_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        attempt = 1

        while time.monotonic() <= deadline:
            ready, detail = await asyncio.to_thread(self.check_ready_once)

            logger.info(
                "[VLLM] readiness attempt=%s ready=%s detail=%s",
                attempt,
                ready,
                detail,
            )

            if ready:
                return True

            if self.process is not None and self.process.poll() is not None:
                logger.error(
                    "[VLLM] process exited early pid=%s returncode=%s",
                    self.process.pid,
                    self.process.returncode,
                )
                return False

            attempt += 1
            await asyncio.sleep(poll_seconds)

        return False

    def check_ready_once(self) -> tuple[bool, str]:
        request = Request(self.models_url)

        if self.settings.vllm_api_key:
            request.add_header("Authorization", f"Bearer {self.settings.vllm_api_key}")

        try:
            with urlopen(request, timeout=3) as response:
                status_code = getattr(response, "status", response.getcode())
                body = response.read(2048).decode("utf-8", errors="replace")
        except HTTPError as exc:
            return False, f"http_error status={exc.code}"
        except URLError as exc:
            return False, f"url_error reason={exc.reason}"
        except TimeoutError:
            return False, "timeout"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

        if 200 <= status_code < 300:
            return True, body[:300]

        return False, f"status={status_code} body={body[:300]}"

    def status(self) -> dict:
        return {
            "base_url": self.settings.vllm_base_url,
            "models_url": self.models_url,
            "model": self.settings.vllm_model,
            "serve_model_path": self.settings.vllm_serve_model_path,
            "pid": self.process.pid if self.process else None,
            "returncode": self.process.poll() if self.process else None,
            "started_by_manager": self.started_by_manager,
            "log_path": self.settings.vllm_log_path,
        }

    def stop(self) -> None:
        if not self.started_by_manager or self.process is None:
            self._close_log()
            return

        if self.process.poll() is None:
            logger.info("[VLLM] stopping pid=%s", self.process.pid)
            self.process.terminate()

            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                logger.warning("[VLLM] terminate timeout, killing pid=%s", self.process.pid)
                self.process.kill()
                self.process.wait(timeout=10)

        self._close_log()
        logger.info("[VLLM] stopped status=%s", self.status())

    def _close_log(self) -> None:
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None


async def preload_yolo_engine(settings: Any) -> None:
    """Load and optionally warm up the YOLO TensorRT engine at startup."""
    if not settings.yolo_preload_on_startup:
        logger.info("[YOLO] preload disabled")
        return

    logger.info(
        "[YOLO] START preload model=%s classes=%s confidence=%s",
        settings.fire_pipeline_yolo_model_path,
        ["person", "fire", "smoke"],
        settings.yolo_confidence,
    )

    def _load() -> int:
        import numpy as np

        from ai.vlm.fire_pipeline.detector import YoloDetector

        detector = YoloDetector(
            settings.fire_pipeline_yolo_model_path,
            detect_classes=["person", "fire", "smoke"],
            confidence=settings.yolo_confidence,
        )

        if not settings.yolo_warmup_on_startup:
            return -1

        dummy_frame = np.zeros((640, 640, 3), dtype=np.uint8)
        detections, _ = detector.detect(dummy_frame)
        return len(detections)

    detection_count = await asyncio.to_thread(_load)

    if detection_count >= 0:
        logger.info(
            "[YOLO] warmup inference complete model=%s detections=%s",
            settings.fire_pipeline_yolo_model_path,
            detection_count,
        )

    logger.info("[YOLO] END preload model=%s", settings.fire_pipeline_yolo_model_path)
