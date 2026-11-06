"""Worker state-machine and sweeper tests (real Postgres, Redis and S3; the
ClamAV verdict is faked here, and tested for real in test_clamav_integration)."""
import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from arq import Retry
from sqlalchemy import update

from app import scanner, storage, worker
from app.db import SessionLocal
from app.models import JobStatus, UploadJob

CLEAN = scanner.ScanResult(infected=False)
INFECTED = scanner.ScanResult(infected=True, signature="Test.Signature")


async def _make_job(creds, body: bytes = b"hello world\n", *, put: bool = True, **overrides) -> uuid.UUID:
    """Insert a quarantined job directly, as the upload route would have."""
    job_id = uuid.uuid4()
    key = storage.quarantine_key(job_id)
    if put:
        import io

        await asyncio.to_thread(storage.put_fileobj, io.BytesIO(body), key)
    fields = dict(
        id=job_id,
        api_key_id=creds[2].id,
        original_filename="a.txt",
        content_type="text/plain",
        sha256=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        status=JobStatus.QUARANTINED.value,
        quarantine_key=key,
    )
    fields.update(overrides)
    async with SessionLocal() as s:
        s.add(UploadJob(**fields))
        await s.commit()
    return job_id


async def _row(job_id) -> UploadJob:
    async with SessionLocal() as s:
        return await s.get(UploadJob, job_id)


def _fake_scan(monkeypatch, result=None, error=None):
    calls = []

    async def fake(fileobj, **kwargs):
        calls.append(fileobj.read())
        if error:
            raise error
        return result

    monkeypatch.setattr(scanner, "scan_file", fake)
    return calls


@pytest.fixture(autouse=True)
def fast_retries(monkeypatch):
    monkeypatch.setattr(worker, "RETRY_BASE_DELAY", 1)


# ---------- the scan task ----------

async def test_clean_file_marked_clean(credentials, monkeypatch):
    calls = _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials, b"some text")
    assert await worker.scan_upload({}, str(job_id)) == "clean"
    row = await _row(job_id)
    assert row.status == "clean" and row.attempts == 1 and row.scanned_at is not None
    assert calls == [b"some text"]  # the scanner really received the file's bytes
    # Still in quarantine: Phase 6 moves clean files to permanent storage.
    assert await asyncio.to_thread(storage.object_exists, row.quarantine_key)


async def test_infected_file_deleted_and_recorded(credentials, monkeypatch):
    _fake_scan(monkeypatch, INFECTED)
    job_id = await _make_job(credentials)
    assert await worker.scan_upload({}, str(job_id)) == "infected"
    row = await _row(job_id)
    assert row.status == "infected"
    assert row.scan_signature == "Test.Signature"
    assert row.completed_at is not None
    assert row.quarantine_key is None
    assert row.sha256  # kept for audit
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


async def test_scan_error_retries_then_gives_up(credentials, monkeypatch):
    _fake_scan(monkeypatch, error=scanner.ScanError("clamd down"))
    job_id = await _make_job(credentials)
    for attempt in range(1, worker.SCAN_MAX_TRIES):
        with pytest.raises(Retry):
            await worker.scan_upload({}, str(job_id))
        row = await _row(job_id)
        assert row.status == "quarantined" and row.attempts == attempt  # back in line
    assert await worker.scan_upload({}, str(job_id)) == "failed"
    row = await _row(job_id)
    assert row.status == "failed" and "clamd down" in row.error
    assert row.attempts == worker.SCAN_MAX_TRIES
    # Fail closed: never marked clean, and the file is kept for investigation.
    assert await asyncio.to_thread(storage.object_exists, row.quarantine_key)


async def test_retry_succeeds_when_clamd_recovers(credentials, monkeypatch):
    _fake_scan(monkeypatch, error=scanner.ScanError("clamd starting"))
    job_id = await _make_job(credentials)
    with pytest.raises(Retry):
        await worker.scan_upload({}, str(job_id))
    _fake_scan(monkeypatch, CLEAN)
    assert await worker.scan_upload({}, str(job_id)) == "clean"
    assert (await _row(job_id)).attempts == 2


async def test_tampered_quarantine_object_detected(credentials, monkeypatch):
    calls = _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials, b"original")
    import io

    await asyncio.to_thread(storage.put_fileobj, io.BytesIO(b"swapped!"), storage.quarantine_key(job_id))
    assert await worker.scan_upload({}, str(job_id)) == "failed"
    row = await _row(job_id)
    assert row.status == "failed" and "integrity" in row.error
    assert calls == []  # never even scanned


async def test_missing_object_fails(credentials, monkeypatch):
    _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials, put=False)
    assert await worker.scan_upload({}, str(job_id)) == "failed"
    assert "missing" in (await _row(job_id)).error


async def test_duplicate_deliveries_scan_only_once(credentials, monkeypatch):
    calls = _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials)
    results = await asyncio.gather(*[worker.scan_upload({}, str(job_id)) for _ in range(5)])
    assert sorted(results) == ["clean", "skipped", "skipped", "skipped", "skipped"]
    assert len(calls) == 1
    assert (await _row(job_id)).attempts == 1


async def test_finished_jobs_are_not_rescanned(credentials, monkeypatch):
    calls = _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials, status="stored")
    assert await worker.scan_upload({}, str(job_id)) == "skipped"
    assert calls == []


async def test_unknown_job_is_skipped(monkeypatch):
    _fake_scan(monkeypatch, CLEAN)
    assert await worker.scan_upload({}, str(uuid.uuid4())) == "skipped"


async def test_failure_to_delete_infected_object_is_remembered(credentials, monkeypatch):
    _fake_scan(monkeypatch, INFECTED)
    job_id = await _make_job(credentials)
    real_delete = storage.delete_object

    def broken(key):
        raise RuntimeError("s3 hiccup")

    monkeypatch.setattr(storage, "delete_object", broken)
    assert await worker.scan_upload({}, str(job_id)) == "infected"
    row = await _row(job_id)
    assert row.status == "infected" and row.quarantine_key is not None  # still to clean up

    monkeypatch.setattr(storage, "delete_object", real_delete)
    report = await worker.sweep_jobs({})
    assert report["cleaned"] >= 1
    row = await _row(job_id)
    assert row.quarantine_key is None
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


# ---------- the sweeper ----------

async def _age(job_id, **fields):
    """Pretend the row was last touched `minutes` ago."""
    minutes = fields.pop("minutes")
    old = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    async with SessionLocal() as s:
        await s.execute(
            update(UploadJob).where(UploadJob.id == job_id).values(updated_at=old, **fields)
        )
        await s.commit()


@pytest.fixture
def requeued(monkeypatch):
    seen = []

    async def fake(job_id, **kwargs):
        seen.append((str(job_id), kwargs))

    monkeypatch.setattr(worker, "enqueue_scan", fake)
    return seen


async def test_sweeper_requeues_stranded_job_once(credentials, requeued):
    job_id = await _make_job(credentials)
    await _age(job_id, minutes=5)
    await worker.sweep_jobs({})
    assert (str(job_id), {"unique": False}) in requeued
    requeued.clear()
    await worker.sweep_jobs({})  # updated_at was refreshed, so not again
    assert str(job_id) not in [j for j, _ in requeued]


async def test_sweeper_ignores_fresh_jobs(credentials, requeued):
    job_id = await _make_job(credentials)
    await worker.sweep_jobs({})
    assert str(job_id) not in [j for j, _ in requeued]


async def test_sweeper_resets_scan_abandoned_by_dead_worker(credentials, requeued):
    job_id = await _make_job(credentials, status="scanning", attempts=1)
    await _age(job_id, minutes=30)
    await worker.sweep_jobs({})
    row = await _row(job_id)
    assert row.status == "quarantined"
    # It was reset; a later sweep re-enqueues it once it is old enough again.
    await _age(job_id, minutes=5)
    await worker.sweep_jobs({})
    assert str(job_id) in [j for j, _ in requeued]


async def test_sweeper_fails_job_that_used_all_attempts(credentials, requeued):
    job_id = await _make_job(credentials, status="scanning", attempts=worker.SCAN_MAX_TRIES)
    await _age(job_id, minutes=30)
    await worker.sweep_jobs({})
    row = await _row(job_id)
    assert row.status == "failed" and "abandoned" in row.error and row.completed_at is not None


async def test_sweeper_leaves_active_scans_alone(credentials, requeued):
    job_id = await _make_job(credentials, status="scanning", attempts=1)
    await _age(job_id, minutes=2)
    await worker.sweep_jobs({})
    assert (await _row(job_id)).status == "scanning"
