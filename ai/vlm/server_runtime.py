"""Runtime helpers for starting and checking the local vLLM server."""
from __future__ import annotations

import asyncio
import json
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
        self.progress = {
            "phase": "created",
            "percent": 0,
            "message": "VLM server manager created",
            "elapsed_seconds": 0,
        }

    @property
    def models_url(self) -> str:
        return self.settings.vllm_base_url.rstrip("/") + "/models"

    async def ensure_ready(self) -> bool:
        start_time = time.monotonic()
        self._set_progress(
            "checking_existing_server",
            5,
            "기존 VLM 서버가 이미 켜져 있는지 확인 중",
            start_time=start_time,
        )
        logger.info(
            "[VLLM] START ensure_ready autostart=%s base_url=%s model=%s",
            self.settings.vllm_autostart,
            self.settings.vllm_base_url,
            self.settings.vllm_model,
        )

        if await self.wait_ready(timeout_seconds=2, poll_seconds=0.5):
            self._set_progress(
                "ready",
                100,
                "이미 실행 중인 VLM 서버 확인 완료",
                start_time=start_time,
            )
            logger.info("[VLLM] READY already running url=%s", self.models_url)
            return True

        if not self.settings.vllm_autostart:
            self._set_progress(
                "failed",
                0,
                "VLLM_AUTOSTART=false 이고 VLM 서버가 준비되지 않음",
                start_time=start_time,
            )
            raise RuntimeError("VLLM_AUTOSTART=false 이고 VLM 서버가 준비되지 않았습니다.")

        self._set_progress(
            "starting_process",
            10,
            "vLLM 프로세스 실행 준비 중",
            start_time=start_time,
        )
        self.start()

        ready = await self.wait_ready(
            timeout_seconds=self.settings.vllm_startup_timeout_seconds,
            poll_seconds=self.settings.vllm_startup_poll_seconds,
            start_percent=20,
            end_percent=95,
            start_time=start_time,
        )

        if not ready:
            status = self.status()
            self._set_progress(
                "failed",
                self.progress.get("percent", 0),
                "VLM 서버 준비 시간 초과 또는 프로세스 조기 종료",
                start_time=start_time,
                status=status,
            )
            raise RuntimeError(f"VLM 서버 준비 시간 초과: {status}")

        self._set_progress(
            "ready",
            100,
            "VLM 서버 준비 완료",
            start_time=start_time,
            status=self.status(),
        )
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
        self._set_progress(
            "launching_process",
            15,
            "vLLM 실행 명령 생성 완료, 프로세스 시작 중",
            command_preview=self._command_preview(command),
        )
        logger.info("[VLLM] START process log=%s", log_path)
        logger.debug("[VLLM] command=%s", command)

        self.process = subprocess.Popen(
            ["bash", "-lc", command],
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.started_by_manager = True
        self._set_progress(
            "process_started",
            20,
            "vLLM 프로세스 시작 완료, API 준비 대기 중",
            pid=self.process.pid,
            log_path=str(log_path),
        )
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

    async def wait_ready(
        self,
        timeout_seconds: float,
        poll_seconds: float,
        start_percent: int = 5,
        end_percent: int = 95,
        start_time: float | None = None,
    ) -> bool:
        deadline = time.monotonic() + timeout_seconds
        wait_start = time.monotonic()
        attempt = 1

        while time.monotonic() <= deadline:
            ready, detail = await asyncio.to_thread(self.check_ready_once)
            elapsed_wait = time.monotonic() - wait_start
            wait_ratio = min(elapsed_wait / max(timeout_seconds, 1), 1)
            percent = min(
                end_percent,
                start_percent + int((end_percent - start_percent) * wait_ratio),
            )

            if ready:
                percent = max(percent, end_percent)

            self._set_progress(
                "waiting_api_ready" if not ready else "api_ready",
                percent,
                "VLM API 준비 확인 중" if not ready else "VLM API 응답 확인 완료",
                attempt=attempt,
                ready=ready,
                detail=detail,
                start_time=start_time,
            )

            logger.info(
                "[VLLM_PROGRESS] percent=%s phase=%s attempt=%s ready=%s detail=%s",
                percent,
                self.progress.get("phase"),
                attempt,
                ready,
                detail,
            )

            if ready:
                return True

            if self.process is not None and self.process.poll() is not None:
                self._set_progress(
                    "process_exited",
                    percent,
                    "VLM 프로세스가 준비 전에 종료됨",
                    attempt=attempt,
                    returncode=self.process.returncode,
                    start_time=start_time,
                )
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
            "progress_path": self.settings.vllm_progress_path,
            "progress": self.progress,
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

    def _set_progress(
        self,
        phase: str,
        percent: int,
        message: str,
        start_time: float | None = None,
        **extra: Any,
    ) -> None:
        elapsed_seconds = 0

        if start_time is not None:
            elapsed_seconds = round(time.monotonic() - start_time, 1)

        progress = {
            "phase": phase,
            "percent": max(0, min(int(percent), 100)),
            "message": message,
            "elapsed_seconds": elapsed_seconds,
            "base_url": self.settings.vllm_base_url,
            "model": self.settings.vllm_model,
            "pid": self.process.pid if self.process else None,
        }
        progress.update(extra)
        self.progress = progress
        logger.info(
            "[VLLM_PROGRESS] %s%% %s - %s",
            progress["percent"],
            phase,
            message,
        )
        self._write_progress(progress)

    def _write_progress(self, progress: dict) -> None:
        progress_path = Path(self.settings.vllm_progress_path).expanduser()

        if not progress_path.is_absolute():
            progress_path = Path.cwd() / progress_path

        try:
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            progress_path.write_text(
                json.dumps(progress, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.debug("[VLLM] progress file write failed", exc_info=True)

    def _command_preview(self, command: str) -> str:
        if self.settings.vllm_api_key:
            command = command.replace(self.settings.vllm_api_key, "***")

        if len(command) > 500:
            return command[:500] + "...[truncated]"

        return command


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
