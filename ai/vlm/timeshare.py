"""Global time-sliced VLM execution queue.

vLLM 서버는 여러 카메라/파이프라인에서 동시에 요청이 들어오면 Jetson 메모리와
GPU 사용량이 튈 수 있다. 모든 VLM 호출은 이 큐를 통과하고, 설정된 슬롯 수만큼만
동시에 실행한다.
"""
from __future__ import annotations

import itertools
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class _VlmJob:
    job_id: int
    label: str
    fn: Callable[[], Any]
    created_at: float = field(default_factory=time.monotonic)
    event: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: BaseException | None = None


class VlmTimeshareScheduler:
    """고정된 worker slot 수만큼 VLM 작업을 시간 분할 처리한다."""

    def __init__(self, *, max_concurrent: int = 2, cooldown_seconds: float = 0.1):
        self.max_concurrent = max(1, int(max_concurrent))
        self.cooldown_seconds = cooldown_seconds
        self._queue: queue.Queue[_VlmJob] = queue.Queue()
        self._ids = itertools.count(1)
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._active_jobs: dict[str, dict[str, Any]] = {}
        self._started_at = ""
        self._completed_count = 0
        self._failed_count = 0
        self._last_error = ""

    def submit(self, label: str, fn: Callable[[], Any], *, timeout: float | None = None) -> Any:
        """Queue one VLM call and wait for its turn/result."""
        self._ensure_workers()

        job = _VlmJob(
            job_id=next(self._ids),
            label=label or "vlm_request",
            fn=fn,
        )
        self._queue.put(job)
        logger.info(
            "[VLM_TIMESHARE] queued job_id=%s label=%s queue_size=%s",
            job.job_id,
            job.label,
            self._queue.qsize(),
        )

        if not job.event.wait(timeout):
            raise TimeoutError(
                f"VLM time-share wait timed out: job_id={job.job_id}, label={job.label}"
            )

        if job.error is not None:
            raise job.error

        return job.result

    def status(self) -> dict:
        with self._lock:
            active_jobs = [dict(item) for item in self._active_jobs.values()]
            worker_alive_count = sum(1 for worker in self._workers if worker.is_alive())
            return {
                "worker_alive": worker_alive_count > 0,
                "worker_count": len(self._workers),
                "worker_alive_count": worker_alive_count,
                "max_concurrent": self.max_concurrent,
                "queue_size": self._queue.qsize(),
                "active_job": active_jobs[0] if active_jobs else None,
                "active_jobs": active_jobs,
                "completed_count": self._completed_count,
                "failed_count": self._failed_count,
                "last_error": self._last_error,
                "started_at": self._started_at,
                "cooldown_seconds": self.cooldown_seconds,
            }

    def _ensure_workers(self) -> None:
        with self._lock:
            alive_workers = [worker for worker in self._workers if worker.is_alive()]
            self._workers = alive_workers

            if len(self._workers) >= self.max_concurrent:
                return

            if not self._started_at:
                self._started_at = _now_iso()

            while len(self._workers) < self.max_concurrent:
                worker_index = len(self._workers) + 1
                worker = threading.Thread(
                    target=self._run,
                    args=(worker_index,),
                    name=f"vlm-timeshare-worker-{worker_index}",
                    daemon=True,
                )
                self._workers.append(worker)
                worker.start()
                logger.info("[VLM_TIMESHARE] worker started index=%s", worker_index)

    def _run(self, worker_index: int) -> None:
        worker_name = f"worker-{worker_index}"
        while True:
            job = self._queue.get()
            wait_seconds = time.monotonic() - job.created_at
            run_started = time.monotonic()

            with self._lock:
                self._active_jobs[worker_name] = {
                    "worker": worker_name,
                    "job_id": job.job_id,
                    "label": job.label,
                    "queued_wait_seconds": round(wait_seconds, 3),
                    "started_at": _now_iso(),
                }

            logger.info(
                "[VLM_TIMESHARE] START worker=%s job_id=%s label=%s waited=%.3fs queue_size=%s",
                worker_name,
                job.job_id,
                job.label,
                wait_seconds,
                self._queue.qsize(),
            )

            try:
                job.result = job.fn()
                with self._lock:
                    self._completed_count += 1
                    self._last_error = ""
                logger.info(
                    "[VLM_TIMESHARE] END job_id=%s label=%s elapsed=%.3fs",
                    job.job_id,
                    job.label,
                    time.monotonic() - run_started,
                )
            except BaseException as exc:
                job.error = exc
                with self._lock:
                    self._failed_count += 1
                    self._last_error = f"{type(exc).__name__}: {exc}"
                logger.exception(
                    "[VLM_TIMESHARE] FAIL job_id=%s label=%s elapsed=%.3fs",
                    job.job_id,
                    job.label,
                    time.monotonic() - run_started,
                )
            finally:
                with self._lock:
                    self._active_jobs.pop(worker_name, None)
                job.event.set()
                self._queue.task_done()
                if self.cooldown_seconds > 0:
                    time.sleep(self.cooldown_seconds)


def _default_max_concurrent() -> int:
    try:
        from config import settings

        return int(getattr(settings, "vlm_timeshare_max_concurrent", 2) or 2)
    except Exception:
        return 2


def _default_cooldown_seconds() -> float:
    try:
        from config import settings

        return float(getattr(settings, "vlm_timeshare_cooldown_seconds", 0.1) or 0.1)
    except Exception:
        return 0.1


_scheduler = VlmTimeshareScheduler(
    max_concurrent=_default_max_concurrent(),
    cooldown_seconds=_default_cooldown_seconds(),
)


def get_vlm_timeshare_scheduler() -> VlmTimeshareScheduler:
    return _scheduler


def run_vlm_timeshare(label: str, fn: Callable[[], Any], *, timeout: float | None = None) -> Any:
    return _scheduler.submit(label, fn, timeout=timeout)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
