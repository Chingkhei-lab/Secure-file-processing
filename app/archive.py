"""Move old, finished job rows out of the hot table.

    docker compose exec api python -m app.archive --dry-run
    docker compose exec api python -m app.archive --days 90

Why: upload_jobs is read and updated constantly by the API and the workers.
Finished jobs are never touched again, but they would keep growing the table
and its indexes forever. Moving them to upload_jobs_archive keeps the hot
table small and its indexes cheap, without losing the audit record (SHA-256,
timestamps, scan verdict).

The files themselves are NOT touched. Only job rows move; the stored blobs
stay in S3 and in stored_objects. Once archived, a job no longer appears in
the API (GET /v1/uploads/{id} returns 404): that is the retention policy.

Rows still holding a quarantined file are skipped (they need cleaning first).
The move is atomic per batch (one DELETE ... RETURNING feeding one INSERT), so
a crash cannot lose or duplicate rows.
"""
import argparse
import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from app.config import get_settings
from app.db import SessionLocal, engine
from app.models import UploadJob

FINISHED = "('stored', 'infected', 'failed')"
COLUMNS = ", ".join(c.name for c in UploadJob.__table__.columns)


async def archive_old_jobs(
    older_than_days: int,
    *,
    batch_size: int = 5000,
    dry_run: bool = False,
    api_key_id: uuid.UUID | None = None,
) -> int:
    """Archive finished jobs completed more than `older_than_days` ago.
    Returns how many rows were (or, with dry_run, would be) moved."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    key_filter = "AND api_key_id = :k" if api_key_id else ""
    params = {"cutoff": cutoff, "n": batch_size, **({"k": api_key_id} if api_key_id else {})}

    if dry_run:
        async with SessionLocal() as s:
            return (
                await s.execute(
                    text(
                        f"""SELECT count(*) FROM upload_jobs
                            WHERE status IN {FINISHED} AND completed_at < :cutoff
                              AND quarantine_key IS NULL {key_filter}"""
                    ),
                    params,
                )
            ).scalar_one()

    total = 0
    while True:
        async with SessionLocal() as s:
            result = await s.execute(
                text(
                    f"""
                    WITH moved AS (
                        DELETE FROM upload_jobs WHERE id IN (
                            SELECT id FROM upload_jobs
                            WHERE status IN {FINISHED} AND completed_at < :cutoff
                              AND quarantine_key IS NULL {key_filter}
                            ORDER BY completed_at
                            LIMIT :n
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING {COLUMNS}
                    )
                    INSERT INTO upload_jobs_archive ({COLUMNS})
                    SELECT {COLUMNS} FROM moved
                    """
                ),
                params,
            )
            await s.commit()
        total += result.rowcount
        if result.rowcount < batch_size:
            return total


async def _main() -> None:
    p = argparse.ArgumentParser(prog="app.archive")
    p.add_argument("--days", type=int, default=get_settings().archive_after_days)
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    try:
        n = await archive_old_jobs(args.days, batch_size=args.batch_size, dry_run=args.dry_run)
        verb = "would archive" if args.dry_run else "archived"
        print(f"{verb} {n} job(s) completed more than {args.days} days ago")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(_main())
