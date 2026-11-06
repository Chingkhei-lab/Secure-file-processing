"""Live storage savings, computed from what is really stored.

    docker compose exec api python -m app.stats
    docker compose exec api python -m app.stats --key-prefix fsk_ab12cd34
    docker compose exec api python -m app.stats --json

Three byte counts describe the stored uploads:

  uploaded    every upload counted at its original size (duplicates included)
  unique      each distinct file counted once, at its original size
  stored      each distinct file counted once, at its compressed size

so   dedupe saves      1 - unique / uploaded
     compression saves 1 - stored / unique     (on the unique files)
     both together     1 - stored / uploaded

and (1 - dedupe) * (1 - compression) = 1 - total. Quote them separately:
compression depends on the file types, dedupe on how often files repeat.

Archived job rows (see app/archive.py) are included, so archiving old rows
never changes these numbers. Computed in SQL, so it stays fast on big tables.
"""
import argparse
import asyncio
import json
import sys
import uuid

from sqlalchemy import select, text

from app.config import get_settings
from app.db import SessionLocal, engine
from app.models import ApiKey

GIB = 1024**3  # S3 bills per GB, where 1 GB = 2**30 bytes


def _reduction(after: int, before: int) -> float:
    return 0.0 if before == 0 else 1 - after / before


async def compute_stats(
    api_key_id: uuid.UUID | None = None, *, price_per_gb_month: float | None = None
) -> dict:
    """Savings over every stored upload, or only one API key's."""
    price = get_settings().s3_price_per_gb_month if price_per_gb_month is None else price_per_gb_month
    key_filter = "AND api_key_id = :k" if api_key_id else ""
    params = {"k": api_key_id} if api_key_id else {}

    jobs_cte = f"""
        WITH jobs AS (
            SELECT size_bytes, stored_object_id FROM upload_jobs
             WHERE status = 'stored' AND stored_object_id IS NOT NULL {key_filter}
            UNION ALL
            SELECT size_bytes, stored_object_id FROM upload_jobs_archive
             WHERE status = 'stored' AND stored_object_id IS NOT NULL {key_filter}
        ),
        objs AS (SELECT DISTINCT stored_object_id AS id FROM jobs)
    """
    async with SessionLocal() as s:
        uploads, uploaded_bytes = (
            await s.execute(
                text(jobs_cte + "SELECT count(*), COALESCE(sum(size_bytes), 0) FROM jobs"), params
            )
        ).one()
        rows = (
            await s.execute(
                text(
                    jobs_cte
                    + """
                    SELECT o.compression, count(*), sum(o.original_size), sum(o.stored_size)
                    FROM stored_objects o JOIN objs ON objs.id = o.id
                    GROUP BY o.compression
                    ORDER BY sum(o.original_size) DESC
                    """
                ),
                params,
            )
        ).all()

    by_compression = [
        {
            "method": method,
            "objects": int(n),
            "original_bytes": int(original),
            "stored_bytes": int(stored),
            "reduction": _reduction(int(stored), int(original)),
        }
        for method, n, original, stored in rows
    ]
    uploads, uploaded_bytes = int(uploads), int(uploaded_bytes)
    unique_objects = sum(m["objects"] for m in by_compression)
    unique_original = sum(m["original_bytes"] for m in by_compression)
    stored_bytes = sum(m["stored_bytes"] for m in by_compression)

    without = uploaded_bytes / GIB * price
    with_ = stored_bytes / GIB * price
    return {
        "uploads": uploads,
        "unique_objects": unique_objects,
        "duplicate_uploads": uploads - unique_objects,
        "uploaded_bytes": uploaded_bytes,
        "unique_original_bytes": unique_original,
        "stored_bytes": stored_bytes,
        "reduction": {
            "total": _reduction(stored_bytes, uploaded_bytes),
            "from_dedupe": _reduction(unique_original, uploaded_bytes),
            "from_compression_on_unique": _reduction(stored_bytes, unique_original),
        },
        "by_compression": by_compression,
        "estimated_cost": {
            "price_per_gb_month": price,
            "without_optimisation_per_month": without,
            "with_optimisation_per_month": with_,
            "saved_per_month": without - with_,
        },
    }


def _mb(n: int) -> str:
    return f"{n / 1_048_576:,.2f} MB"


def _pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def format_stats(d: dict) -> str:
    r, c = d["reduction"], d["estimated_cost"]
    out = [
        "LIVE STORAGE SAVINGS",
        "=" * 64,
        f"uploads stored: {d['uploads']:,}   unique files: {d['unique_objects']:,}   "
        f"duplicate uploads: {d['duplicate_uploads']:,}",
        "",
        f"  uploaded (logical)          {_mb(d['uploaded_bytes']):>14}",
        f"  after dedupe only           {_mb(d['unique_original_bytes']):>14}   saved {_pct(r['from_dedupe'])}",
        f"  stored (dedupe + compress)  {_mb(d['stored_bytes']):>14}",
        "",
        f"  reduction from dedupe:                      {_pct(r['from_dedupe'])}",
        f"  reduction from compression (unique files):  {_pct(r['from_compression_on_unique'])}",
        f"  TOTAL reduction:                            {_pct(r['total'])}",
    ]
    if d["by_compression"]:
        out += ["", f"  {'method':9} {'files':>6} {'original':>13} {'stored':>13} {'saved':>7}"]
        for m in d["by_compression"]:
            out.append(
                f"  {m['method']:9} {m['objects']:>6,} {_mb(m['original_bytes']):>13} "
                f"{_mb(m['stored_bytes']):>13} {_pct(m['reduction']):>7}"
            )
    out += [
        "",
        f"ESTIMATED STORAGE COST at ${c['price_per_gb_month']}/GB-month, one storage class",
        f"  without optimisation: ${c['without_optimisation_per_month']:,.6f}/month   "
        f"with: ${c['with_optimisation_per_month']:,.6f}/month",
    ]
    return "\n".join(out)


async def _main() -> int:
    p = argparse.ArgumentParser(prog="app.stats")
    p.add_argument("--key-prefix", help="only this API key (the fsk_xxxxxxxx prefix)")
    p.add_argument("--price-per-gb-month", type=float, default=None)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    try:
        key_id = None
        if args.key_prefix:
            async with SessionLocal() as s:
                ids = (
                    await s.execute(select(ApiKey.id).where(ApiKey.key_prefix == args.key_prefix))
                ).scalars().all()
            if len(ids) != 1:
                print(f"expected exactly one key with prefix {args.key_prefix}, found {len(ids)}", file=sys.stderr)
                return 2
            key_id = ids[0]
        d = await compute_stats(key_id, price_per_gb_month=args.price_per_gb_month)
    finally:
        await engine.dispose()
    print(json.dumps(d, indent=2) if args.json else format_stats(d))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
