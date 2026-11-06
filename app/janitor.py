"""Housekeeping that stops storage from leaking.

    docker compose exec api python -m app.janitor purge                # failed files past retention
    docker compose exec api python -m app.janitor reconcile            # REPORT only
    docker compose exec api python -m app.janitor reconcile --delete   # also remove orphans

1. purge: a failed job keeps its quarantined file for investigation. After
   FAILED_RETENTION_DAYS the file is deleted and the key forgotten. Runs
   nightly from the worker (see daily_maintenance in app/worker.py).

2. reconcile: compares what is in S3 with what the database knows.
     orphan   an object with no database row (the S3 write succeeded, the
              database commit then failed)
     missing  a database row whose object is not in S3 (serious: a stored file
              has gone). This is only ever REPORTED, never "fixed".
   It reports by default. Deleting orphans needs --delete and is manual; the
   nightly job never does it.

Because reconcile --delete REMOVES DATA it is deliberately cautious:
  * objects younger than --min-age-hours are never touched, so a write whose
    database commit is still in flight is safe;
  * each deletion runs inside a transaction holding the SAME advisory lock the
    store step holds while it writes that content, so the janitor can never
    delete an object in the gap between a worker uploading it and committing
    its row, and the row and the object's age are re-checked under that lock;
  * only keys under the chosen prefix (default objects/) are considered.
"""
import argparse
import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text, update

from app import storage
from app.config import get_settings
from app.db import SessionLocal, engine
from app.models import JobStatus, StoredObject, UploadJob

log = logging.getLogger("fileservice.janitor")

_SHA_TAIL = re.compile(r"(?:^|/)([0-9a-f]{64})$")


# ---------- 1. failed jobs past retention ----------

async def purge_failed_quarantine(
    older_than_days: int, *, api_key_id=None, limit: int = 1000
) -> int:
    """Delete the quarantined file of every job that failed more than
    `older_than_days` ago. Returns how many files were removed. A delete that
    fails keeps its key, so the next run tries again."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    conditions = [
        UploadJob.status == JobStatus.FAILED.value,
        UploadJob.quarantine_key.is_not(None),
        UploadJob.completed_at < cutoff,
    ]
    if api_key_id is not None:
        conditions.append(UploadJob.api_key_id == api_key_id)
    async with SessionLocal() as s:
        rows = (
            await s.execute(
                select(UploadJob.id, UploadJob.quarantine_key)
                .where(*conditions)
                .order_by(UploadJob.completed_at)
                .limit(limit)
            )
        ).all()

    removed = 0
    for job_id, key in rows:
        try:
            await asyncio.to_thread(storage.delete_object, key)
        except Exception:  # noqa: BLE001
            log.exception("could not delete quarantined file of failed job %s", job_id)
            continue
        async with SessionLocal() as s:
            await s.execute(update(UploadJob).where(UploadJob.id == job_id).values(quarantine_key=None))
            await s.commit()
        removed += 1
    return removed


# ---------- 2. reconcile S3 against the database ----------

def _list_keys(prefix: str, cutoff: datetime) -> tuple[set[str], set[str]]:
    """(every key under prefix, the subset last modified at or before cutoff)"""
    every: set[str] = set()
    old: set[str] = set()
    paginator = storage.get_s3().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=get_settings().s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            every.add(obj["Key"])
            if obj["LastModified"] <= cutoff:
                old.add(obj["Key"])
    return every, old


def _lock_name(key: str) -> str:
    """The store step locks on the content's SHA-256, so for real object keys
    the janitor must use that exact string. Other keys lock on their own name."""
    match = _SHA_TAIL.search(key)
    return match.group(1) if match else key


async def _delete_orphan(key: str, cutoff: datetime) -> bool:
    bucket = get_settings().s3_bucket
    async with SessionLocal() as s:
        async with s.begin():
            await s.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": _lock_name(key)}
            )
            has_row = await s.execute(select(StoredObject.id).where(StoredObject.s3_key == key))
            if has_row.first() is not None:
                return False  # a row appeared meanwhile: not an orphan after all
            try:
                head = await asyncio.to_thread(storage.get_s3().head_object, Bucket=bucket, Key=key)
            except Exception:  # noqa: BLE001 - already gone or unreadable: do nothing
                return False
            if head["LastModified"] > cutoff:
                return False  # rewritten recently: someone is using it
            await asyncio.to_thread(storage.get_s3().delete_object, Bucket=bucket, Key=key)
    return True


async def reconcile(
    *, prefix: str = "objects/", min_age_hours: float = 24, delete: bool = False
) -> dict:
    """Compare S3 with the database under `prefix`.

    Memory note: keys are held in sets, which is fine for millions of objects
    but would need streaming beyond that.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    s3_keys, old_keys = await asyncio.to_thread(_list_keys, prefix, cutoff)
    async with SessionLocal() as s:
        db_keys = set(
            (
                await s.execute(
                    select(StoredObject.s3_key).where(
                        StoredObject.s3_key.startswith(prefix, autoescape=True)
                    )
                )
            ).scalars()
        )
    orphans = sorted(old_keys - db_keys)
    missing = sorted(db_keys - s3_keys)

    deleted = 0
    if delete:
        for key in orphans:
            if await _delete_orphan(key, cutoff):
                deleted += 1
    if orphans or missing:
        log.warning(
            "reconcile: %d orphan object(s), %d missing object(s), %d deleted",
            len(orphans), len(missing), deleted,
        )
    return {"prefix": prefix, "orphans": orphans, "missing": missing, "deleted": deleted}


# ---------- command line ----------

async def _main() -> int:
    p = argparse.ArgumentParser(prog="app.janitor")
    sub = p.add_subparsers(dest="cmd", required=True)
    pp = sub.add_parser("purge", help="delete quarantined files of old failed jobs")
    pp.add_argument("--days", type=int, default=get_settings().failed_retention_days)
    pr = sub.add_parser("reconcile", help="compare S3 with the database")
    pr.add_argument("--prefix", default="objects/")
    pr.add_argument("--min-age-hours", type=float, default=24)
    pr.add_argument("--delete", action="store_true", help="remove orphans (default: report only)")
    args = p.parse_args()
    try:
        if args.cmd == "purge":
            n = await purge_failed_quarantine(args.days)
            print(f"removed {n} quarantined file(s) of jobs that failed more than {args.days} days ago")
            return 0
        report = await reconcile(
            prefix=args.prefix, min_age_hours=args.min_age_hours, delete=args.delete
        )
    finally:
        await engine.dispose()
    print(json.dumps(report, indent=2))
    return 1 if report["missing"] else 0  # a missing stored file needs a human


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
