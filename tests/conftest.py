"""Shared fixtures for the integration tests (real Postgres, Redis and S3)."""
import asyncio

import httpx
import pytest
from sqlalchemy import delete, select, text

from app import storage
from app.config import get_settings
from app.db import SessionLocal
from app.main import app
from app.models import ApiKey, UploadJob
from app.redis_client import get_redis
from app.routers import uploads as uploads_module
from app.security import derive_signing_secret, generate_api_key, hash_api_key

settings = get_settings()


@pytest.fixture(autouse=True)
def enqueued(monkeypatch):
    """Stop uploads from really queueing scans during tests (the real worker
    container would otherwise race with assertions). Returns the list of job
    ids that WOULD have been enqueued."""
    calls: list[str] = []

    async def fake_enqueue(job_id, **kwargs):
        calls.append(str(job_id))

    monkeypatch.setattr(uploads_module, "enqueue_scan", fake_enqueue)
    return calls


@pytest.fixture(autouse=True)
def store_enqueued(monkeypatch):
    """Same idea for stage 2: the worker must not really queue store jobs
    (the live worker container would race with the tests). Returns the list of
    (job_id, kwargs) that WOULD have been enqueued."""
    from app import worker

    calls: list[tuple[str, dict]] = []

    async def fake_enqueue_store(job_id, **kwargs):
        calls.append((str(job_id), kwargs))

    monkeypatch.setattr(worker, "enqueue_store", fake_enqueue_store)
    return calls


@pytest.fixture(scope="session")
async def bucket():
    await asyncio.to_thread(storage.ensure_bucket)


@pytest.fixture
async def client(bucket):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _make_credentials(name: str):
    key = generate_api_key()
    async with SessionLocal() as s:
        row = ApiKey(name=name, key_hash=hash_api_key(key), key_prefix=key[:12])
        s.add(row)
        await s.commit()
        await s.refresh(row)
    secret = derive_signing_secret(settings.signing_master_secret, row.id)
    return key, secret, row


async def _cleanup_credentials(row: ApiKey):
    """Remove the key, its jobs, their quarantined S3 objects and rate counters."""
    async with SessionLocal() as s:
        job_ids = (
            await s.execute(select(UploadJob.id).where(UploadJob.api_key_id == row.id))
        ).scalars().all()
    for job_id in job_ids:
        await asyncio.to_thread(storage.delete_object, storage.quarantine_key(job_id))
    redis = get_redis()
    async for k in redis.scan_iter(f"rl:{row.id}:*"):
        await redis.delete(k)
    async with SessionLocal() as s:
        await s.execute(delete(UploadJob).where(UploadJob.api_key_id == row.id))
        await s.execute(
            text("DELETE FROM upload_jobs_archive WHERE api_key_id = :k"), {"k": row.id}
        )
        await s.execute(delete(ApiKey).where(ApiKey.id == row.id))
        await s.commit()


@pytest.fixture
async def credentials():
    creds = await _make_credentials("pytest")
    yield creds
    await _cleanup_credentials(creds[2])


@pytest.fixture
async def other_credentials():
    creds = await _make_credentials("pytest-other")
    yield creds
    await _cleanup_credentials(creds[2])
