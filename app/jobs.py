"""Enqueueing background jobs (ARQ, backed by Redis)."""
import time
import uuid

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

from app.config import get_settings

_pool: ArqRedis | None = None


async def get_queue() -> ArqRedis:
    global _pool
    if _pool is None:
        _pool = await create_pool(RedisSettings.from_dsn(get_settings().redis_url))
    return _pool


async def close_queue() -> None:
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None


async def enqueue_scan(job_id: uuid.UUID | str, *, unique: bool = True) -> None:
    """Ask a worker to scan an upload.

    unique=True gives a stable ARQ job id, so enqueueing the same upload twice
    is ignored. The sweeper passes unique=False to force a fresh attempt when
    an earlier enqueue was lost. Duplicates are harmless either way: the
    worker claims a job atomically in the database before doing any work.
    """
    queue = await get_queue()
    arq_id = f"scan:{job_id}" if unique else f"scan:{job_id}:{int(time.time())}"
    await queue.enqueue_job("scan_upload", str(job_id), _job_id=arq_id)


async def enqueue_store(job_id: uuid.UUID | str, *, unique: bool = True) -> None:
    """Ask a worker to compress and store a clean upload (same rules as
    enqueue_scan: the database is the source of truth, duplicates are safe)."""
    queue = await get_queue()
    arq_id = f"store:{job_id}" if unique else f"store:{job_id}:{int(time.time())}"
    await queue.enqueue_job("store_upload", str(job_id), _job_id=arq_id)
