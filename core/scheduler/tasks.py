import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def _db_sync_job() -> None:
    try:
        from core.sync.db_sync import run_full_sync
        result = await run_full_sync()
        logger.info("Scheduled DB sync: %s", result)
    except Exception:
        logger.exception("DB sync job failed")


def start() -> None:
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="Asia/Seoul")
    _scheduler.add_job(
        _db_sync_job,
        trigger="interval",
        seconds=settings.db_sync_interval_seconds,
        id="db_sync",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Scheduler started (db_sync every %ds)", settings.db_sync_interval_seconds)


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
