"""Live-stats tests: the savings numbers must match what is really stored."""
import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app import archive
from app.db import SessionLocal
from app.models import StoredObject, UploadJob
from app.stats import compute_stats, format_stats


def _object(original, stored, method) -> StoredObject:
    return StoredObject(
        sha256=uuid.uuid4().hex * 2,
        content_type="text/plain",
        original_size=original,
        stored_size=stored,
        compression=method,
        s3_key=f"objects/zz-stats/{uuid.uuid4().hex}",
    )


def _job(creds, obj, size, **kw) -> UploadJob:
    now = datetime.now(timezone.utc)
    fields = dict(
        api_key_id=creds[2].id,
        original_filename="f.txt",
        content_type="text/plain",
        sha256=obj.sha256,
        size_bytes=size,
        status="stored",
        stored_object_id=obj.id,
        completed_at=now,
    )
    fields.update(kw)
    return UploadJob(**fields)


@pytest.fixture
async def objects(credentials):
    """Creates objects, and removes them (and the jobs pointing at them) after."""
    made: list[StoredObject] = []
    yield made
    async with SessionLocal() as s:
        ids = [o.id for o in made]
        await s.execute(delete(UploadJob).where(UploadJob.stored_object_id.in_(ids)))
        await s.execute(delete(StoredObject).where(StoredObject.id.in_(ids)))
        await s.commit()


async def _seed(credentials, objects):
    """Object A (1000 -> 200 bytes) uploaded twice, object B (500 -> 500) once."""
    a, b = _object(1000, 200, "zstd"), _object(500, 500, "none")
    objects += [a, b]
    async with SessionLocal() as s:
        s.add_all([a, b])
        await s.flush()
        s.add_all([_job(credentials, a, 1000), _job(credentials, a, 1000), _job(credentials, b, 500)])
        # Jobs that must NOT count: not stored yet / failed.
        s.add(_job(credentials, a, 999, status="clean", stored_object_id=None, completed_at=None))
        s.add(_job(credentials, a, 999, status="failed", stored_object_id=None))
        await s.commit()
    return a, b


async def test_numbers_match_what_is_stored(credentials, objects):
    await _seed(credentials, objects)
    d = await compute_stats(credentials[2].id)

    assert d["uploads"] == 3 and d["unique_objects"] == 2 and d["duplicate_uploads"] == 1
    assert d["uploaded_bytes"] == 2500  # 1000 + 1000 + 500
    assert d["unique_original_bytes"] == 1500  # A once + B
    assert d["stored_bytes"] == 700  # 200 + 500
    r = d["reduction"]
    assert r["total"] == pytest.approx(1 - 700 / 2500)
    assert r["from_dedupe"] == pytest.approx(1 - 1500 / 2500)
    assert r["from_compression_on_unique"] == pytest.approx(1 - 700 / 1500)
    methods = {m["method"]: m for m in d["by_compression"]}
    assert methods["zstd"]["stored_bytes"] == 200 and methods["none"]["reduction"] == 0.0


async def test_cost_estimate_uses_the_price(credentials, objects):
    await _seed(credentials, objects)
    d = await compute_stats(credentials[2].id, price_per_gb_month=1.0)
    gib = 1024**3
    assert d["estimated_cost"]["without_optimisation_per_month"] == pytest.approx(2500 / gib)
    assert d["estimated_cost"]["saved_per_month"] == pytest.approx(1800 / gib)


async def test_archived_jobs_still_count(credentials, objects):
    await _seed(credentials, objects)
    before = await compute_stats(credentials[2].id)
    # Make every finished job look old, then archive them.
    old = datetime.now(timezone.utc) - timedelta(days=400)
    async with SessionLocal() as s:
        rows = (
            await s.execute(select(UploadJob).where(UploadJob.api_key_id == credentials[2].id))
        ).scalars()
        for row in rows:
            row.completed_at = old if row.status in ("stored", "failed") else None
            row.quarantine_key = None
        await s.commit()
    moved = await archive.archive_old_jobs(30, api_key_id=credentials[2].id)
    assert moved >= 3
    after = await compute_stats(credentials[2].id)
    assert after["uploaded_bytes"] == before["uploaded_bytes"] == 2500
    assert after["stored_bytes"] == before["stored_bytes"] == 700


async def test_empty_scope_gives_zeros_not_errors(credentials):
    d = await compute_stats(credentials[2].id)
    assert d["uploads"] == 0 and d["uploaded_bytes"] == 0 and d["reduction"]["total"] == 0.0
    assert "LIVE STORAGE SAVINGS" in format_stats(d)


async def test_report_text_mentions_the_totals(credentials, objects):
    await _seed(credentials, objects)
    text = format_stats(await compute_stats(credentials[2].id))
    assert "TOTAL reduction" in text and "zstd" in text and "72.0%" in text
