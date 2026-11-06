"""Stage 2 (compress + dedupe + store) tests: real Postgres, Redis and S3."""
import asyncio
import hashlib
import io
import random
import uuid

import pytest
import zstandard
from PIL import Image
from sqlalchemy import delete, func, select

from app import compression, scanner, storage, worker
from app.client import sign_headers
from app.db import SessionLocal
from app.models import JobStatus, StoredObject, UploadJob
from tests.test_compression import _encode, _noisy_image
from tests.test_worker import CLEAN, INFECTED, _age, _fake_scan, _make_job, _row


# ---------- helpers and fixtures ----------

@pytest.fixture
async def shas():
    """Records every content hash a test creates, and removes the resulting
    stored objects afterwards (they would otherwise turn reruns into dupes)."""
    seen: list[str] = []
    yield seen
    async with SessionLocal() as s:
        ids = select(StoredObject.id).where(StoredObject.sha256.in_(seen))
        await s.execute(delete(UploadJob).where(UploadJob.stored_object_id.in_(ids)))
        await s.execute(delete(StoredObject).where(StoredObject.sha256.in_(seen)))
        await s.commit()
    for sha in seen:
        await asyncio.to_thread(storage.delete_object, storage.final_key(sha))


def _unique_text(shas: list[str]) -> bytes:
    body = (f"unique line {uuid.uuid4()}\n" * 1).encode() * 1500
    shas.append(hashlib.sha256(body).hexdigest())
    return body


def _track(shas: list[str], body: bytes) -> bytes:
    shas.append(hashlib.sha256(body).hexdigest())
    return body


async def _object(sha: str) -> StoredObject | None:
    async with SessionLocal() as s:
        return (
            await s.execute(select(StoredObject).where(StoredObject.sha256 == sha))
        ).scalar_one_or_none()


async def _object_count(sha: str) -> int:
    async with SessionLocal() as s:
        return (
            await s.execute(
                select(func.count()).select_from(StoredObject).where(StoredObject.sha256 == sha)
            )
        ).scalar_one()


async def _final_bytes(sha: str) -> bytes:
    return await asyncio.to_thread(storage.get_object_bytes, storage.final_key(sha))


@pytest.fixture
def puts(monkeypatch):
    """Records the final-object keys written to S3 (quarantine writes ignored)."""
    real = storage.put_fileobj
    keys: list[str] = []

    def counting(fileobj, key):
        if key.startswith("objects/"):
            keys.append(key)
        return real(fileobj, key)

    monkeypatch.setattr(storage, "put_fileobj", counting)
    return keys


# ---------- storing ----------

async def test_text_is_compressed_and_stored(credentials, shas):
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    job_id = await _make_job(credentials, body, status="clean")

    assert await worker.store_upload({}, str(job_id)) == "stored"

    row = await _row(job_id)
    assert row.status == "stored" and row.store_attempts == 1
    assert row.completed_at is not None and row.deduplicated is False
    assert row.quarantine_key is None
    obj = await _object(sha)
    assert obj.compression == "zstd" and obj.original_size == len(body)
    assert obj.stored_size < len(body) / 10
    assert obj.s3_key == storage.final_key(sha) and row.stored_object_id == obj.id
    # What is in S3 really decompresses to the original bytes.
    assert zstandard.ZstdDecompressor().decompress(await _final_bytes(sha)) == body
    # The quarantine copy is gone.
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


async def test_png_is_stored_as_smaller_webp(credentials, shas):
    png = _track(shas, _encode(_noisy_image(seed=random.randrange(10**9)), "PNG"))
    sha = hashlib.sha256(png).hexdigest()
    job_id = await _make_job(credentials, png, status="clean", content_type="image/png")

    assert await worker.store_upload({}, str(job_id)) == "stored"

    obj = await _object(sha)
    assert obj.compression == "webp" and obj.stored_size < obj.original_size
    assert Image.open(io.BytesIO(await _final_bytes(sha))).format == "WEBP"


async def test_incompressible_file_is_stored_unchanged(credentials, shas):
    body = _track(shas, b"short " + uuid.uuid4().bytes)  # too small to benefit
    sha = hashlib.sha256(body).hexdigest()
    job_id = await _make_job(credentials, body, status="clean")
    assert await worker.store_upload({}, str(job_id)) == "stored"
    obj = await _object(sha)
    assert obj.compression == "none" and obj.stored_size == len(body)
    assert await _final_bytes(sha) == body


# ---------- deduplication ----------

async def test_identical_content_is_stored_once(credentials, shas, puts):
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    first = await _make_job(credentials, body, status="clean")
    second = await _make_job(credentials, body, status="clean")

    assert await worker.store_upload({}, str(first)) == "stored"
    assert await worker.store_upload({}, str(second)) == "deduplicated"

    a, b = await _row(first), await _row(second)
    assert a.status == b.status == "stored"
    assert a.stored_object_id == b.stored_object_id
    assert (a.deduplicated, b.deduplicated) == (False, True)
    assert await _object_count(sha) == 1
    assert len(puts) == 1  # the blob was written to S3 exactly once
    for job_id in (first, second):  # both quarantine copies are cleaned up
        assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


async def test_dedup_works_across_api_keys_but_is_not_revealed(
    client, credentials, other_credentials, shas, puts
):
    body = _unique_text(shas)
    mine = await _make_job(credentials, body, status="clean")
    theirs = await _make_job(other_credentials, body, status="clean")
    await worker.store_upload({}, str(mine))
    assert await worker.store_upload({}, str(theirs)) == "deduplicated"
    assert len(puts) == 1

    key, secret, _ = other_credentials
    target = f"/v1/uploads/{theirs}"
    resp = await client.get(target, headers=sign_headers(key, secret, "GET", target))
    data = resp.json()
    assert resp.status_code == 200 and data["status"] == "stored"
    assert data["compression"] == "zstd" and data["stored_size"] > 0
    # The uploader must not be able to learn that someone else holds this file.
    assert "deduplicated" not in data and "stored_object_id" not in data


async def test_simultaneous_identical_uploads_write_once(credentials, shas, puts):
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    jobs = [await _make_job(credentials, body, status="clean") for _ in range(4)]

    results = await asyncio.gather(*[worker.store_upload({}, str(j)) for j in jobs])

    assert sorted(results) == ["deduplicated"] * 3 + ["stored"]
    assert len(puts) == 1 and await _object_count(sha) == 1
    assert len({(await _row(j)).stored_object_id for j in jobs}) == 1


async def test_duplicate_queue_deliveries_store_once(credentials, shas, puts):
    body = _unique_text(shas)
    job_id = await _make_job(credentials, body, status="clean")
    results = await asyncio.gather(*[worker.store_upload({}, str(job_id)) for _ in range(5)])
    assert sorted(results) == ["skipped"] * 4 + ["stored"]
    assert len(puts) == 1
    assert (await _row(job_id)).store_attempts == 1


async def test_only_clean_jobs_are_stored(credentials, shas, puts):
    body = _unique_text(shas)
    job_id = await _make_job(credentials, body)  # still quarantined, never scanned
    assert await worker.store_upload({}, str(job_id)) == "skipped"
    assert (await _row(job_id)).status == "quarantined" and puts == []
    assert await worker.store_upload({}, str(uuid.uuid4())) == "skipped"


# ---------- integrity and bad input ----------

async def test_bytes_swapped_after_scan_are_not_stored(credentials, shas, puts):
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    job_id = await _make_job(credentials, body, status="clean")
    # An attacker with storage access replaces the scanned file.
    await asyncio.to_thread(
        storage.put_fileobj, io.BytesIO(b"malicious replacement"), storage.quarantine_key(job_id)
    )
    assert await worker.store_upload({}, str(job_id)) == "failed"
    row = await _row(job_id)
    assert row.status == "failed" and "after the malware scan" in row.error
    assert await _object(sha) is None and puts == []


async def test_invalid_image_is_rejected(credentials, shas, puts):
    body = _track(shas, b"\x89PNG\r\n\x1a\n" + uuid.uuid4().bytes + b"not an image" * 30)
    job_id = await _make_job(credentials, body, status="clean", content_type="image/png")
    assert await worker.store_upload({}, str(job_id)) == "failed"
    row = await _row(job_id)
    assert row.status == "failed" and "not a valid image" in row.error
    assert puts == [] and await _object(hashlib.sha256(body).hexdigest()) is None


async def test_missing_quarantine_object_fails(credentials, shas):
    body = _unique_text(shas)
    job_id = await _make_job(credentials, body, status="clean", put=False)
    assert await worker.store_upload({}, str(job_id)) == "failed"
    assert "missing" in (await _row(job_id)).error


# ---------- failures and retries ----------

async def test_storage_failure_retries_then_succeeds(credentials, shas, monkeypatch):
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    job_id = await _make_job(credentials, body, status="clean")
    real = storage.put_fileobj
    outage = {"on": True}

    def flaky(fileobj, key):
        if key.startswith("objects/") and outage["on"]:
            raise RuntimeError("s3 down")
        return real(fileobj, key)

    monkeypatch.setattr(storage, "put_fileobj", flaky)
    with pytest.raises(worker.Retry):
        await worker.store_upload({}, str(job_id))
    row = await _row(job_id)
    assert row.status == "clean" and row.store_attempts == 1  # back in line
    assert await _object(sha) is None  # nothing half-written in the database

    outage["on"] = False
    assert await worker.store_upload({}, str(job_id)) == "stored"
    assert (await _row(job_id)).store_attempts == 2


async def test_gives_up_after_max_tries(credentials, shas, monkeypatch):
    body = _unique_text(shas)
    job_id = await _make_job(credentials, body, status="clean")
    real = storage.put_fileobj

    def always_down(fileobj, key):
        if key.startswith("objects/"):
            raise RuntimeError("s3 down")
        return real(fileobj, key)

    monkeypatch.setattr(storage, "put_fileobj", always_down)
    monkeypatch.setattr(worker, "RETRY_BASE_DELAY", 1)
    for _ in range(worker.STORE_MAX_TRIES - 1):
        with pytest.raises(worker.Retry):
            await worker.store_upload({}, str(job_id))
    assert await worker.store_upload({}, str(job_id)) == "failed"
    row = await _row(job_id)
    assert row.status == "failed" and "gave up" in row.error
    # The scanned file is kept for investigation.
    assert await asyncio.to_thread(storage.object_exists, row.quarantine_key)


async def test_job_that_changed_state_rolls_everything_back(credentials, shas):
    """If the job is no longer 'storing' at commit time, no stored object may
    be left behind in the database."""
    body = _unique_text(shas)
    sha = hashlib.sha256(body).hexdigest()
    job_id = await _make_job(credentials, body, status="clean")  # not 'storing'
    result = compression.compress(io.BytesIO(body), "text/plain")
    with pytest.raises(worker._JobLost):
        await worker._commit_stored(job_id, sha, "text/plain", len(body), result)
    assert await _object(sha) is None


async def test_failed_quarantine_delete_is_retried_by_sweeper(credentials, shas, monkeypatch):
    body = _unique_text(shas)
    job_id = await _make_job(credentials, body, status="clean")
    real_delete = storage.delete_object

    def broken(key):
        raise RuntimeError("s3 hiccup")

    monkeypatch.setattr(storage, "delete_object", broken)
    assert await worker.store_upload({}, str(job_id)) == "stored"
    row = await _row(job_id)
    assert row.status == "stored" and row.quarantine_key is not None  # still to clean up

    monkeypatch.setattr(storage, "delete_object", real_delete)
    report = await worker.sweep_jobs({})
    assert report["cleaned"] >= 1
    assert (await _row(job_id)).quarantine_key is None
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


# ---------- hand-over from stage 1 ----------

async def test_clean_scan_queues_the_store_step(credentials, monkeypatch, store_enqueued):
    _fake_scan(monkeypatch, CLEAN)
    job_id = await _make_job(credentials, b"fine")
    assert await worker.scan_upload({}, str(job_id)) == "clean"
    assert [j for j, _ in store_enqueued] == [str(job_id)]


async def test_infected_scan_does_not_queue_store(credentials, monkeypatch, store_enqueued):
    _fake_scan(monkeypatch, INFECTED)
    job_id = await _make_job(credentials, b"bad")
    assert await worker.scan_upload({}, str(job_id)) == "infected"
    assert store_enqueued == []


async def test_lost_store_enqueue_does_not_break_scan(credentials, monkeypatch):
    _fake_scan(monkeypatch, CLEAN)

    async def broken(job_id, **kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(worker, "enqueue_store", broken)
    job_id = await _make_job(credentials, b"fine")
    assert await worker.scan_upload({}, str(job_id)) == "clean"
    assert (await _row(job_id)).status == "clean"  # the sweeper will queue it


# ---------- sweeper: stage 2 ----------

async def test_sweeper_requeues_stranded_clean_job_once(credentials, store_enqueued):
    job_id = await _make_job(credentials, b"x", status="clean")
    await _age(job_id, minutes=5)
    await worker.sweep_jobs({})
    assert (str(job_id), {"unique": False}) in store_enqueued
    store_enqueued.clear()
    await worker.sweep_jobs({})
    assert str(job_id) not in [j for j, _ in store_enqueued]


async def test_sweeper_ignores_fresh_clean_jobs(credentials, store_enqueued):
    job_id = await _make_job(credentials, b"x", status="clean")
    await worker.sweep_jobs({})
    assert str(job_id) not in [j for j, _ in store_enqueued]


async def test_sweeper_resets_store_abandoned_by_dead_worker(credentials, store_enqueued):
    job_id = await _make_job(credentials, b"x", status="storing", store_attempts=1)
    await _age(job_id, minutes=30)
    await worker.sweep_jobs({})
    assert (await _row(job_id)).status == "clean"


async def test_sweeper_fails_store_that_used_all_attempts(credentials):
    job_id = await _make_job(
        credentials, b"x", status="storing", store_attempts=worker.STORE_MAX_TRIES
    )
    await _age(job_id, minutes=30)
    await worker.sweep_jobs({})
    row = await _row(job_id)
    assert row.status == "failed" and "abandoned" in row.error and row.completed_at is not None


async def test_sweeper_leaves_active_stores_alone(credentials):
    job_id = await _make_job(credentials, b"x", status="storing", store_attempts=1)
    await _age(job_id, minutes=2)
    await worker.sweep_jobs({})
    assert (await _row(job_id)).status == "storing"
