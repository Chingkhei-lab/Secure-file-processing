"""Background worker (ARQ). Run with:  arq app.worker.WorkerSettings

Job life cycle
--------------
Stage 1, scan_upload:
    quarantined --claim--> scanning --clean-----> clean      (then queues stage 2)
                              |     \\--infected--> infected  (object deleted)
                              |     \\--bad file--> failed    (integrity / missing)
                              \\--scan problem--> quarantined (retry with backoff)
                                                  ... failed after SCAN_MAX_TRIES
Stage 2, store_upload:
    clean --claim--> storing --ok--------> stored      (quarantine copy deleted)
                        |    \\--bad file--> failed      (integrity / invalid image)
                        \\--storage problem--> clean     (retry with backoff)
                                               ... failed after STORE_MAX_TRIES

Safety rules
------------
* Claiming is one atomic UPDATE ... WHERE status=<waiting state>, so two
  workers (or a duplicate queue message) can never process a job at once.
* Fail closed: if scanning cannot complete, the file is NEVER marked clean.
* Both stages re-hash what they download and compare it with the hash recorded
  at upload time, so tampering with the stored bytes (including between the
  scan and the store) is detected. Only bytes that were scanned get stored.
* Identical content is stored once. A Postgres advisory lock keyed by the
  SHA-256 serialises workers handling the same content, so two simultaneous
  uploads of one file can never write it twice.
* The queue is not the source of truth, the database is. If a queue message is
  lost, the sweeper finds the stranded job and enqueues it again.
"""
import asyncio
import logging
import tempfile
import uuid
from datetime import datetime, timedelta, timezone

from arq import Retry, cron
from arq.connections import RedisSettings
from sqlalchemy import func, select, text, update

from app import archive, compression, janitor, scanner, storage
from app.config import get_settings
from app.db import SessionLocal, engine
from app.jobs import close_queue, enqueue_scan, enqueue_store
from app.models import JobStatus, StoredObject, UploadJob
from app.redis_client import close_redis

settings = get_settings()
logging.basicConfig(level=settings.log_level)
log = logging.getLogger("fileservice.worker")

SCAN_MAX_TRIES = 4
STORE_MAX_TRIES = 4
RETRY_BASE_DELAY = 5  # seconds; doubles each attempt (5, 10, 20)
STUCK_AFTER = timedelta(minutes=10)  # a worker probably died mid-task
LOST_ENQUEUE_AFTER = timedelta(minutes=2)  # queue message probably lost
SWEEP_BATCH = 100

_TERMINAL = {JobStatus.INFECTED, JobStatus.FAILED, JobStatus.STORED}


class _JobLost(Exception):
    """The job changed state under us (another worker or the sweeper)."""


# ---------- small database helpers ----------

async def _claim(job_id: uuid.UUID):
    """Atomically move quarantined -> scanning. Returns the row, or None if
    someone else already has it (or it is no longer waiting)."""
    async with SessionLocal() as s:
        row = (
            await s.execute(
                update(UploadJob)
                .where(UploadJob.id == job_id, UploadJob.status == JobStatus.QUARANTINED.value)
                .values(
                    status=JobStatus.SCANNING.value,
                    attempts=UploadJob.attempts + 1,
                    updated_at=func.now(),
                )
                .returning(
                    UploadJob.quarantine_key,
                    UploadJob.sha256,
                    UploadJob.size_bytes,
                    UploadJob.attempts,
                )
            )
        ).one_or_none()
        await s.commit()
    return row


async def _claim_store(job_id: uuid.UUID):
    """Atomically move clean -> storing."""
    async with SessionLocal() as s:
        row = (
            await s.execute(
                update(UploadJob)
                .where(UploadJob.id == job_id, UploadJob.status == JobStatus.CLEAN.value)
                .values(
                    status=JobStatus.STORING.value,
                    store_attempts=UploadJob.store_attempts + 1,
                    updated_at=func.now(),
                )
                .returning(
                    UploadJob.quarantine_key,
                    UploadJob.sha256,
                    UploadJob.size_bytes,
                    UploadJob.content_type,
                    UploadJob.store_attempts,
                )
            )
        ).one_or_none()
        await s.commit()
    return row


async def _finish(
    job_id: uuid.UUID, status: JobStatus, *, expect: JobStatus = JobStatus.SCANNING, **fields
) -> bool:
    """Move `expect` -> `status`. False means the job was no longer ours."""
    values = {"status": status.value, "updated_at": func.now(), **fields}
    if status in _TERMINAL:
        values.setdefault("completed_at", func.now())
    async with SessionLocal() as s:
        result = await s.execute(
            update(UploadJob)
            .where(UploadJob.id == job_id, UploadJob.status == expect.value)
            .values(**values)
        )
        await s.commit()
    if result.rowcount != 1:
        log.warning(
            "job %s was not in '%s' when finishing as %s", job_id, expect.value, status.value
        )
    return result.rowcount == 1


async def _retry_or_fail(job_id: uuid.UUID, attempts: int, message: str) -> str:
    if attempts >= SCAN_MAX_TRIES:
        log.error("job %s failed permanently: %s", job_id, message)
        await _finish(job_id, JobStatus.FAILED, error=f"{message} (gave up after {attempts} attempts)")
        return "failed"
    await _finish(job_id, JobStatus.QUARANTINED)
    delay = RETRY_BASE_DELAY * 2 ** (attempts - 1)
    log.warning("job %s attempt %d failed (%s); retrying in %ds", job_id, attempts, message, delay)
    raise Retry(defer=delay)


async def _store_retry_or_fail(job_id: uuid.UUID, attempts: int, message: str) -> str:
    if attempts >= STORE_MAX_TRIES:
        log.error("job %s could not be stored: %s", job_id, message)
        await _finish(
            job_id,
            JobStatus.FAILED,
            expect=JobStatus.STORING,
            error=f"{message} (gave up after {attempts} attempts)",
        )
        return "failed"
    await _finish(job_id, JobStatus.CLEAN, expect=JobStatus.STORING)
    delay = RETRY_BASE_DELAY * 2 ** (attempts - 1)
    log.warning("job %s store attempt %d failed (%s); retrying in %ds", job_id, attempts, message, delay)
    raise Retry(defer=delay)


# ---------- stage 1: scan ----------

async def scan_upload(ctx, job_id: str) -> str:
    jid = uuid.UUID(job_id)
    claimed = await _claim(jid)
    if claimed is None:
        log.info("job %s is not waiting for a scan; skipping", jid)
        return "skipped"
    key, expected_sha, expected_size, attempts = claimed

    scratch = tempfile.TemporaryFile()  # on disk, so memory stays small
    try:
        # 1. Fetch the quarantined bytes and verify they are what was uploaded.
        try:
            sha, size = await asyncio.to_thread(storage.download_to_fileobj, key, scratch)
        except storage.ObjectNotFound:
            await _finish(jid, JobStatus.FAILED, error="quarantined object is missing")
            return "failed"
        except Exception as exc:  # noqa: BLE001 - treat as transient and retry
            return await _retry_or_fail(jid, attempts, f"storage error: {type(exc).__name__}")

        if sha != expected_sha or size != expected_size:
            await _finish(
                jid,
                JobStatus.FAILED,
                error="integrity check failed: stored bytes differ from what was uploaded",
            )
            return "failed"

        # 2. Scan.
        scratch.seek(0)
        try:
            result = await scanner.scan_file(scratch)
        except scanner.ScanError as exc:
            return await _retry_or_fail(jid, attempts, f"scan error: {exc}")

        # 3. Act on the verdict.
        now = datetime.now(timezone.utc)
        if result.infected:
            # Record the verdict first so it can never be lost, then delete.
            await _finish(jid, JobStatus.INFECTED, scan_signature=result.signature, scanned_at=now)
            log.warning("job %s INFECTED (%s)", jid, result.signature)
            await _purge_quarantine(jid, key)
            return "infected"

        await _finish(jid, JobStatus.CLEAN, scanned_at=now)
    finally:
        scratch.close()

    # Hand over to stage 2. If this enqueue is lost, the sweeper re-sends it.
    try:
        await enqueue_store(jid)
    except Exception:  # noqa: BLE001
        log.exception("could not enqueue store for job %s; sweeper will retry", jid)
    return "clean"


async def _purge_quarantine(job_id: uuid.UUID, key: str) -> None:
    """Delete the quarantined copy, then forget its key. If the delete fails the
    key stays set, and the sweeper retries (finished jobs with a leftover key)."""
    try:
        await asyncio.to_thread(storage.delete_object, key)
    except Exception:  # noqa: BLE001
        log.exception("could not delete quarantined object for job %s", job_id)
        return
    async with SessionLocal() as s:
        await s.execute(update(UploadJob).where(UploadJob.id == job_id).values(quarantine_key=None))
        await s.commit()


# ---------- stage 2: compress and store ----------

async def _object_exists(sha256: str) -> bool:
    async with SessionLocal() as s:
        found = await s.execute(select(StoredObject.id).where(StoredObject.sha256 == sha256))
        return found.first() is not None


async def _commit_stored(
    job_id: uuid.UUID,
    sha256: str,
    content_type: str,
    original_size: int,
    result: compression.Compressed | None,
) -> bool:
    """Write the final object (unless one already exists) and mark the job
    stored, in ONE database transaction. Returns True if it was a duplicate.

    The advisory lock is held until the transaction ends. It means two jobs
    with identical content run this block one after the other, so the second
    sees the first's row and reuses it instead of writing the blob again.
    """
    async with SessionLocal() as s:
        async with s.begin():
            await s.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": sha256}
            )
            obj = (
                await s.execute(select(StoredObject).where(StoredObject.sha256 == sha256))
            ).scalar_one_or_none()
            duplicate = obj is not None
            if obj is None:
                if result is None:
                    raise RuntimeError("stored object disappeared; retrying with a fresh copy")
                final_key = storage.final_key(sha256)
                result.fileobj.seek(0)
                await asyncio.to_thread(storage.put_fileobj, result.fileobj, final_key)
                obj = StoredObject(
                    sha256=sha256,
                    content_type=content_type,
                    original_size=original_size,
                    stored_size=result.size,
                    compression=result.method,
                    s3_key=final_key,
                )
                s.add(obj)
                await s.flush()
            updated = await s.execute(
                update(UploadJob)
                .where(UploadJob.id == job_id, UploadJob.status == JobStatus.STORING.value)
                .values(
                    status=JobStatus.STORED.value,
                    stored_object_id=obj.id,
                    deduplicated=duplicate,
                    error=None,
                    completed_at=func.now(),
                    updated_at=func.now(),
                )
            )
            if updated.rowcount != 1:
                raise _JobLost()  # rolls the transaction back
    return duplicate


async def store_upload(ctx, job_id: str) -> str:
    jid = uuid.UUID(job_id)
    claimed = await _claim_store(jid)
    if claimed is None:
        log.info("job %s is not waiting to be stored; skipping", jid)
        return "skipped"
    qkey, sha, expected_size, content_type, attempts = claimed

    src = tempfile.TemporaryFile()
    result: compression.Compressed | None = None
    try:
        # Fast path: this content is already stored, so there is nothing to
        # download or compress. (The job's hash was verified at scan time.)
        if not await _object_exists(sha):
            try:
                dl_sha, dl_size = await asyncio.to_thread(storage.download_to_fileobj, qkey, src)
            except storage.ObjectNotFound:
                await _finish(
                    jid, JobStatus.FAILED, expect=JobStatus.STORING,
                    error="quarantined object is missing",
                )
                return "failed"
            except Exception as exc:  # noqa: BLE001
                return await _store_retry_or_fail(jid, attempts, f"storage error: {type(exc).__name__}")

            # Only bytes that were scanned may be stored: re-verify the hash.
            if dl_sha != sha or dl_size != expected_size:
                await _finish(
                    jid, JobStatus.FAILED, expect=JobStatus.STORING,
                    error="integrity check failed: bytes changed after the malware scan",
                )
                return "failed"

            src.seek(0)
            try:
                result = await asyncio.to_thread(
                    compression.compress,
                    src,
                    content_type,
                    image_mode=settings.image_mode,
                    image_quality=settings.image_quality,
                    zstd_level=settings.zstd_level,
                    max_image_pixels=settings.max_image_pixels,
                )
            except compression.InvalidContent as exc:
                await _finish(
                    jid, JobStatus.FAILED, expect=JobStatus.STORING, error=f"rejected: {exc}"
                )
                return "failed"
            except Exception as exc:  # noqa: BLE001 - unexpected; may be transient (memory)
                log.exception("compression crashed for job %s", jid)
                return await _store_retry_or_fail(
                    jid, attempts, f"compression error: {type(exc).__name__}"
                )

        try:
            duplicate = await _commit_stored(jid, sha, content_type, expected_size, result)
        except _JobLost:
            log.warning("job %s changed state while being stored; leaving it", jid)
            return "skipped"
        except Exception as exc:  # noqa: BLE001
            return await _store_retry_or_fail(jid, attempts, f"storage error: {type(exc).__name__}")
    finally:
        if result is not None and result.fileobj is not src:
            result.fileobj.close()
        src.close()

    # Stored. Now the quarantined copy can go (a leftover key is cleaned up by
    # the sweeper if this delete fails).
    await _purge_quarantine(jid, qkey)
    return "deduplicated" if duplicate else "stored"


# ---------- the sweeper (runs every minute) ----------

async def sweep_jobs(ctx) -> dict:
    now = datetime.now(timezone.utc)
    stuck_cutoff = now - STUCK_AFTER
    lost_cutoff = now - LOST_ENQUEUE_AFTER
    report = {"failed": 0, "reset": 0, "requeued": 0, "cleaned": 0}

    async def abandon(status: JobStatus, tries_col, max_tries: int, reset_to: JobStatus, why: str):
        """Jobs stuck in an active state (worker died). Give up if they have
        used all attempts, otherwise put them back in line."""
        stuck = (UploadJob.status == status.value, UploadJob.updated_at < stuck_cutoff)
        gave_up = await s.execute(
            update(UploadJob)
            .where(*stuck, tries_col >= max_tries)
            .values(
                status=JobStatus.FAILED.value,
                error=why,
                completed_at=func.now(),
                updated_at=func.now(),
            )
        )
        reset = await s.execute(
            update(UploadJob)
            .where(*stuck, tries_col < max_tries)
            .values(status=reset_to.value, updated_at=func.now())
        )
        report["failed"] += gave_up.rowcount
        report["reset"] += reset.rowcount

    async def stranded_ids(status: JobStatus) -> list[uuid.UUID]:
        """Jobs waiting in `status` for suspiciously long: their queue message
        was probably lost. Touch updated_at so we do not re-send every minute."""
        ids = (
            await s.execute(
                select(UploadJob.id)
                .where(UploadJob.status == status.value, UploadJob.updated_at < lost_cutoff)
                .order_by(UploadJob.updated_at)
                .limit(SWEEP_BATCH)
            )
        ).scalars().all()
        if ids:
            await s.execute(
                update(UploadJob).where(UploadJob.id.in_(ids)).values(updated_at=func.now())
            )
        return list(ids)

    async with SessionLocal() as s:
        await abandon(
            JobStatus.SCANNING, UploadJob.attempts, SCAN_MAX_TRIES, JobStatus.QUARANTINED,
            "scan abandoned: worker kept dying or timing out",
        )
        await abandon(
            JobStatus.STORING, UploadJob.store_attempts, STORE_MAX_TRIES, JobStatus.CLEAN,
            "storing abandoned: worker kept dying or timing out",
        )
        scan_ids = await stranded_ids(JobStatus.QUARANTINED)
        store_ids = await stranded_ids(JobStatus.CLEAN)
        await s.commit()

        # Finished jobs whose quarantined copy could not be deleted earlier.
        leftovers = (
            await s.execute(
                select(UploadJob.id, UploadJob.quarantine_key)
                .where(
                    UploadJob.status.in_([JobStatus.INFECTED.value, JobStatus.STORED.value]),
                    UploadJob.quarantine_key.is_not(None),
                )
                .limit(SWEEP_BATCH)
            )
        ).all()

    for job_id in scan_ids:
        await enqueue_scan(job_id, unique=False)
        report["requeued"] += 1
    for job_id in store_ids:
        await enqueue_store(job_id, unique=False)
        report["requeued"] += 1
    for job_id, key in leftovers:
        await _purge_quarantine(job_id, key)
        report["cleaned"] += 1

    if any(report.values()):
        log.info("sweep: %s", report)
    return report


# ---------- daily maintenance ----------

async def daily_maintenance(ctx) -> dict:
    """Remove old failed-job files and archive old finished job rows."""
    purged = await janitor.purge_failed_quarantine(settings.failed_retention_days)
    archived = await archive.archive_old_jobs(settings.archive_after_days)
    report = {"purged_failed_files": purged, "archived_jobs": archived}
    if any(report.values()):
        log.info("daily maintenance: %s", report)
    return report


# ---------- ARQ configuration ----------

async def on_shutdown(ctx) -> None:
    await close_queue()
    await close_redis()
    await engine.dispose()


class WorkerSettings:
    functions = [scan_upload, store_upload]
    cron_jobs = [
        cron(sweep_jobs, minute=None, second=0),  # every minute
        cron(daily_maintenance, hour=3, minute=30, second=0),  # 03:30 container time
    ]
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    on_shutdown = on_shutdown
    # Image conversion can use a few hundred MB per job, so keep this modest.
    max_jobs = 4
    job_timeout = 300  # seconds
    max_tries = max(SCAN_MAX_TRIES, STORE_MAX_TRIES) + 1  # our own counters are the real limit
    health_check_interval = 20  # used by `arq --check` in the compose healthcheck
