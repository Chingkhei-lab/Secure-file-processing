"""Schema tests. Run inside the api container after migrating:

    docker compose exec api alembic upgrade head
    docker compose exec api pytest -v

Every test runs inside a transaction that is rolled back, so the database
is left untouched.
"""
import uuid

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import get_settings
from app.models import ApiKey, StoredObject, UploadJob


@pytest.fixture
async def session():
    # A fresh engine per test avoids event-loop reuse problems with asyncpg.
    engine = create_async_engine(get_settings().database_url)
    async with engine.connect() as conn:
        trans = await conn.begin()
        # create_savepoint: a failed flush rolls back to a savepoint instead of
        # killing the outer transaction (this removes the SAWarning).
        async with AsyncSession(
            bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint"
        ) as s:
            yield s
        await trans.rollback()
    await engine.dispose()


def _key() -> ApiKey:
    return ApiKey(name="test", key_hash=uuid.uuid4().hex * 2, key_prefix="tst_1234")


def _object(sha: str | None = None) -> StoredObject:
    return StoredObject(
        sha256=sha or uuid.uuid4().hex * 2,
        content_type="text/plain",
        original_size=1000,
        stored_size=400,
        compression="zstd",
        s3_key=f"objects/{uuid.uuid4().hex}",
    )


async def test_tables_exist():
    engine = create_async_engine(get_settings().database_url)
    async with engine.connect() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
    await engine.dispose()
    for table in ("api_keys", "stored_objects", "upload_jobs"):
        assert table in names


async def test_job_links_to_object_and_key(session: AsyncSession):
    key, obj = _key(), _object()
    session.add_all([key, obj])
    await session.flush()
    job = UploadJob(
        api_key_id=key.id,
        original_filename="a.txt",
        size_bytes=1000,
        status="stored",
        stored_object_id=obj.id,
    )
    session.add(job)
    await session.flush()
    assert job.id is not None


async def test_invalid_status_rejected(session: AsyncSession):
    key = _key()
    session.add(key)
    await session.flush()
    session.add(
        UploadJob(api_key_id=key.id, original_filename="a.txt", size_bytes=1, status="bogus")
    )
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_storing_status_and_new_columns(session: AsyncSession):
    key = _key()
    session.add(key)
    await session.flush()
    job = UploadJob(
        api_key_id=key.id, original_filename="a.txt", size_bytes=1, status="storing"
    )
    session.add(job)
    await session.flush()
    await session.refresh(job)
    assert job.store_attempts == 0 and job.deduplicated is False


async def test_duplicate_sha256_rejected(session: AsyncSession):
    sha = uuid.uuid4().hex * 2
    session.add(_object(sha))
    await session.flush()
    session.add(_object(sha))
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_negative_sizes_rejected(session: AsyncSession):
    obj = _object()
    obj.stored_size = -5
    session.add(obj)
    with pytest.raises(IntegrityError):
        await session.flush()
