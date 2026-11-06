"""Archive, janitor, reconcile and daily-maintenance tests (real Postgres and S3)."""
import asyncio
import io
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, text

from app import archive, janitor, storage, worker
from app.client import sign_headers
from app.db import SessionLocal
from app.models import StoredObject, UploadJob
from tests.test_worker import _make_job, _row

DAYS = 24 * 3600


def _ago(days: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


async def _archived_ids(creds) -> set[uuid.UUID]:
    async with SessionLocal() as s:
        rows = await s.execute(
            text("SELECT id FROM upload_jobs_archive WHERE api_key_id = :k"), {"k": creds[2].id}
        )
        return {r.id for r in rows}


# ---------- archive ----------

async def test_archive_moves_only_old_finished_jobs(credentials):
    old_stored = await _make_job(credentials, b"a", put=False, status="stored", quarantine_key=None, completed_at=_ago(100))
    old_failed = await _make_job(credentials, b"b", put=False, status="failed", quarantine_key=None, completed_at=_ago(100))
    old_infected = await _make_job(credentials, b"c", put=False, status="infected", quarantine_key=None, completed_at=_ago(100))
    recent = await _make_job(credentials, b"d", put=False, status="stored", quarantine_key=None, completed_at=_ago(5))
    in_flight = await _make_job(credentials, b"e", put=False, status="clean")
    holding_file = await _make_job(credentials, b"f", put=False, status="failed", completed_at=_ago(100))  # quarantine_key still set

    moved = await archive.archive_old_jobs(30, api_key_id=credentials[2].id)

    assert moved == 3
    assert await _archived_ids(credentials) == {old_stored, old_failed, old_infected}
    for kept in (recent, in_flight, holding_file):
        assert await _row(kept) is not None  # still in the hot table
    for gone in (old_stored, old_failed, old_infected):
        assert await _row(gone) is None


async def test_archive_preserves_every_column(credentials):
    job_id = await _make_job(
        credentials, b"keep me", put=False, status="stored", quarantine_key=None,
        completed_at=_ago(60), scan_signature=None, original_filename="precious.txt",
    )
    original = await _row(job_id)
    snapshot = {c.name: getattr(original, c.name) for c in UploadJob.__table__.columns}
    await archive.archive_old_jobs(30, api_key_id=credentials[2].id)
    async with SessionLocal() as s:
        row = (
            await s.execute(text("SELECT * FROM upload_jobs_archive WHERE id = :i"), {"i": job_id})
        ).mappings().one()
    assert dict(row) == snapshot


async def test_dry_run_changes_nothing(credentials):
    job_id = await _make_job(credentials, b"a", put=False, status="stored", quarantine_key=None, completed_at=_ago(100))
    assert await archive.archive_old_jobs(30, dry_run=True, api_key_id=credentials[2].id) == 1
    assert await _row(job_id) is not None and await _archived_ids(credentials) == set()


async def test_archive_works_in_small_batches_and_is_repeatable(credentials):
    ids = [
        await _make_job(credentials, b"x", put=False, status="stored", quarantine_key=None, completed_at=_ago(100 + i))
        for i in range(5)
    ]
    assert await archive.archive_old_jobs(30, batch_size=2, api_key_id=credentials[2].id) == 5
    assert await _archived_ids(credentials) == set(ids)
    assert await archive.archive_old_jobs(30, batch_size=2, api_key_id=credentials[2].id) == 0


async def test_archived_job_leaves_the_api(client, credentials):
    job_id = await _make_job(credentials, b"a", put=False, status="stored", quarantine_key=None, completed_at=_ago(100))
    key, secret, _ = credentials
    target = f"/v1/uploads/{job_id}"
    assert (await client.get(target, headers=sign_headers(key, secret, "GET", target))).status_code == 200
    await archive.archive_old_jobs(30, api_key_id=credentials[2].id)
    # A different timestamp gives a different signature; an identical repeat
    # within the same second would (correctly) be rejected as a replay.
    later = sign_headers(key, secret, "GET", target, timestamp=int(time.time()) + 1)
    assert (await client.get(target, headers=later)).status_code == 404


async def test_archive_table_has_the_same_columns_as_the_job_table():
    """If someone adds a column to upload_jobs, the archive must get it too."""
    async with SessionLocal() as s:
        cols = lambda table: {  # noqa: E731
            r.column_name
            for r in (
                s.sync_session if False else None
            )
            or []
        }
        rows = await s.execute(
            text(
                """SELECT table_name, column_name FROM information_schema.columns
                   WHERE table_name IN ('upload_jobs', 'upload_jobs_archive')"""
            )
        )
        by_table: dict[str, set[str]] = {}
        for r in rows:
            by_table.setdefault(r.table_name, set()).add(r.column_name)
    assert by_table["upload_jobs"] == by_table["upload_jobs_archive"]
    assert by_table["upload_jobs"] == {c.name for c in UploadJob.__table__.columns}


# ---------- janitor: failed jobs ----------

async def test_failed_files_removed_after_retention_only(credentials):
    old = await _make_job(credentials, b"old failure", status="failed", completed_at=_ago(30))
    fresh = await _make_job(credentials, b"new failure", status="failed", completed_at=_ago(1))
    not_failed = await _make_job(credentials, b"fine", status="clean")

    removed = await janitor.purge_failed_quarantine(7, api_key_id=credentials[2].id)

    assert removed == 1
    assert (await _row(old)).quarantine_key is None
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(old))
    assert (await _row(fresh)).quarantine_key is not None
    assert await asyncio.to_thread(storage.object_exists, storage.quarantine_key(fresh))
    assert await asyncio.to_thread(storage.object_exists, storage.quarantine_key(not_failed))


async def test_failed_delete_keeps_the_key_for_next_time(credentials, monkeypatch):
    job_id = await _make_job(credentials, b"stuck", status="failed", completed_at=_ago(30))
    real_delete = storage.delete_object

    def broken(key):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(storage, "delete_object", broken)
    assert await janitor.purge_failed_quarantine(7, api_key_id=credentials[2].id) == 0
    assert (await _row(job_id)).quarantine_key is not None  # will be retried tomorrow
    # Put the real delete back: the fixtures' own cleanup runs before the patch is
    # undone, and it needs to delete this job's file.
    monkeypatch.setattr(storage, "delete_object", real_delete)


# ---------- janitor: reconcile S3 against the database ----------

@pytest.fixture
async def reconcile_area():
    """A private key prefix so these tests never look at (or delete) real objects."""
    prefix = f"objects/zz-test-{uuid.uuid4().hex[:10]}/"
    created_keys: list[str] = []
    yield prefix, created_keys
    for key in created_keys:
        await asyncio.to_thread(storage.delete_object, key)
    async with SessionLocal() as s:
        await s.execute(delete(StoredObject).where(StoredObject.s3_key.startswith(prefix)))
        await s.commit()


async def _put(key: str, keys: list[str]) -> None:
    keys.append(key)
    await asyncio.to_thread(storage.put_fileobj, io.BytesIO(b"data"), key)


async def _row_for(key: str) -> None:
    async with SessionLocal() as s:
        s.add(
            StoredObject(
                sha256=uuid.uuid4().hex * 2, content_type="text/plain", original_size=4,
                stored_size=4, compression="none", s3_key=key,
            )
        )
        await s.commit()


async def test_reconcile_reports_orphans_and_missing_without_deleting(reconcile_area):
    prefix, keys = reconcile_area
    good, orphan, ghost = prefix + "good", prefix + "orphan", prefix + "ghost"
    await _put(good, keys)
    await _row_for(good)  # healthy: object and row both exist
    await _put(orphan, keys)  # object with no row
    await _row_for(ghost)  # row with no object

    report = await janitor.reconcile(prefix=prefix, min_age_hours=0)

    assert report["orphans"] == [orphan]
    assert report["missing"] == [ghost]
    assert report["deleted"] == 0
    assert await asyncio.to_thread(storage.object_exists, orphan)  # reporting is read-only


async def test_reconcile_ignores_objects_that_are_too_new(reconcile_area):
    prefix, keys = reconcile_area
    await _put(prefix + "just-written", keys)
    report = await janitor.reconcile(prefix=prefix, min_age_hours=24)
    assert report["orphans"] == []  # could be a write whose database commit is in flight


async def test_reconcile_delete_removes_only_orphans(reconcile_area):
    prefix, keys = reconcile_area
    good, orphan = prefix + "good", prefix + "orphan"
    await _put(good, keys)
    await _row_for(good)
    await _put(orphan, keys)

    report = await janitor.reconcile(prefix=prefix, min_age_hours=0, delete=True)

    assert report["deleted"] == 1
    assert not await asyncio.to_thread(storage.object_exists, orphan)
    assert await asyncio.to_thread(storage.object_exists, good)


# ---------- daily maintenance task ----------

async def test_daily_maintenance_runs_both_jobs_with_the_configured_ages(monkeypatch):
    calls = {}

    async def fake_purge(days, **kw):
        calls["purge"] = days
        return 2

    async def fake_archive(days, **kw):
        calls["archive"] = days
        return 5

    monkeypatch.setattr(janitor, "purge_failed_quarantine", fake_purge)
    monkeypatch.setattr(archive, "archive_old_jobs", fake_archive)
    report = await worker.daily_maintenance({})
    assert report == {"purged_failed_files": 2, "archived_jobs": 5}
    assert calls == {
        "purge": worker.settings.failed_retention_days,
        "archive": worker.settings.archive_after_days,
    }


def test_daily_maintenance_is_scheduled():
    names = {job.name for job in worker.WorkerSettings.cron_jobs}
    assert {"cron:sweep_jobs", "cron:daily_maintenance"} <= names
