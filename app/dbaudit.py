"""Database index audit.

    docker compose exec api python -m app.dbaudit              # 200,000 rows
    docker compose exec api python -m app.dbaudit --rows 1000000

Question it answers: "do the queries this service runs every minute stay fast
when upload_jobs holds a lot of history?"

How: inside ONE transaction it creates a temporary API key and fills the real
tables with synthetic rows (mostly finished jobs, a few in flight, some failed),
runs ANALYZE, then times each important query with EXPLAIN (ANALYZE, BUFFERS).
Everything is ROLLED BACK at the end, so nothing is left behind and live data is
never touched. (Uncommitted rows are invisible to the running API and workers.)

Each query is planned twice:
  natural  what PostgreSQL really chooses at this size
  forced   with sequential scans disabled, which shows whether a usable index
           EXISTS at all. At small sizes PostgreSQL may legitimately prefer a
           sequential scan, so a missing index only shows up here.
A query is flagged NO INDEX if even the forced plan has to scan a whole table.
"""
import argparse
import asyncio
import json
import sys
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.db import engine

# {key}, {job} and {sha} are filled with real values from the seeded data.
QUERIES: list[tuple[str, str]] = [
    (
        "API: my recent uploads",
        "SELECT * FROM upload_jobs WHERE api_key_id = '{key}'::uuid "
        "ORDER BY created_at DESC LIMIT 20",
    ),
    ("API: one job by id", "SELECT * FROM upload_jobs WHERE id = '{job}'::uuid"),
    ("worker: dedupe lookup by SHA-256", "SELECT id FROM stored_objects WHERE sha256 = '{sha}'"),
    (
        "sweeper: stranded quarantined jobs",
        "SELECT id FROM upload_jobs WHERE status = 'quarantined' "
        "AND updated_at < now() - interval '2 minutes' ORDER BY updated_at LIMIT 100",
    ),
    (
        "sweeper: stuck scanning jobs",
        "SELECT id FROM upload_jobs WHERE status = 'scanning' "
        "AND updated_at < now() - interval '10 minutes'",
    ),
    (
        "sweeper: leftover quarantine copies",
        "SELECT id, quarantine_key FROM upload_jobs "
        "WHERE status IN ('infected', 'stored') AND quarantine_key IS NOT NULL LIMIT 100",
    ),
    (
        "janitor: failed jobs past retention",
        "SELECT id, quarantine_key FROM upload_jobs WHERE status = 'failed' "
        "AND quarantine_key IS NOT NULL AND completed_at < now() - interval '7 days' LIMIT 1000",
    ),
    (
        "archive: oldest finished jobs",
        "SELECT id FROM upload_jobs WHERE status IN ('stored', 'infected', 'failed') "
        "AND completed_at < now() - interval '90 days' AND quarantine_key IS NULL "
        "ORDER BY completed_at LIMIT 5000",
    ),
]

SEED_KEY = """
INSERT INTO api_keys (id, name, key_hash, key_prefix)
VALUES (:key, 'dbaudit', md5(random()::text) || md5(random()::text), 'audit')
"""
SEED_OBJECTS = """
INSERT INTO stored_objects (id, sha256, content_type, original_size, stored_size, compression, s3_key)
SELECT gen_random_uuid(), md5(g::text) || md5((g + 1)::text), 'text/plain', 1000, 400, 'zstd',
       'audit/' || g
FROM generate_series(1, :objs) AS g
"""
# ~98.8% stored, 1% failed (still holding a quarantined file), a few in flight.
SEED_JOBS = """
INSERT INTO upload_jobs (id, api_key_id, original_filename, content_type, sha256, size_bytes,
                         status, stored_object_id, quarantine_key,
                         created_at, updated_at, completed_at)
SELECT gen_random_uuid(), :key, 'f' || src.g || '.txt', 'text/plain', md5(src.g::text), 1000,
       src.status,
       CASE WHEN src.status = 'stored' THEN o.id END,
       CASE WHEN src.status <> 'stored' THEN 'quarantine/audit-' || src.g END,
       src.ts - interval '1 minute',
       CASE WHEN src.status IN ('stored', 'failed') THEN src.ts ELSE now() - interval '30 minutes' END,
       CASE WHEN src.status IN ('stored', 'failed') THEN src.ts END
FROM (
    SELECT g,
           CASE WHEN g % 1000 < 10 THEN 'failed'
                WHEN g % 1000 = 10 THEN 'quarantined'
                WHEN g % 1000 = 11 THEN 'scanning'
                WHEN g % 1000 = 12 THEN 'clean'
                ELSE 'stored' END AS status,
           now() - (random() * 200) * interval '1 day' AS ts
    FROM generate_series(1, :n) AS g
) src
LEFT JOIN (
    SELECT id, row_number() OVER () AS rn FROM stored_objects WHERE s3_key LIKE 'audit/%'
) o ON o.rn = (src.g % :objs) + 1
"""


def _nodes(plan: dict) -> list[dict]:
    found = [plan]
    for child in plan.get("Plans", []):
        found += _nodes(child)
    return found


async def _explain(conn: AsyncConnection, sql: str) -> dict:
    raw = (await conn.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql))).scalar_one()
    top = (json.loads(raw) if isinstance(raw, str) else raw)[0]
    nodes = _nodes(top["Plan"])
    scans = [n for n in nodes if "Scan" in n["Node Type"]]
    return {
        "ms": round(top["Execution Time"], 3),
        "buffers": top["Plan"].get("Shared Hit Blocks", 0) + top["Plan"].get("Shared Read Blocks", 0),
        "seq_scans": [n["Relation Name"] for n in scans if n["Node Type"] == "Seq Scan"],
        "indexes": sorted({n["Index Name"] for n in scans if n.get("Index Name")}),
    }


async def audit(rows: int = 200_000) -> dict:
    """Seed, measure, roll back. Returns per-query results and table sizes."""
    rows = max(rows, 1000)
    key = uuid.uuid4()
    objs = max(rows * 8 // 10, 100)
    async with engine.connect() as conn:
        trans = await conn.begin()
        try:
            await conn.execute(text(SEED_KEY), {"key": key})
            await conn.execute(text(SEED_OBJECTS), {"objs": objs})
            await conn.execute(text(SEED_JOBS), {"key": key, "n": rows, "objs": objs})
            await conn.execute(text("ANALYZE upload_jobs"))
            await conn.execute(text("ANALYZE stored_objects"))

            job = (
                await conn.execute(
                    text("SELECT id FROM upload_jobs WHERE api_key_id = :k LIMIT 1 OFFSET 777"),
                    {"k": key},
                )
            ).scalar_one()
            sha = (
                await conn.execute(
                    text("SELECT sha256 FROM stored_objects WHERE s3_key LIKE 'audit/%' LIMIT 1 OFFSET 42")
                )
            ).scalar_one()

            results = []
            for name, template in QUERIES:
                sql = template.format(key=key, job=job, sha=sha)
                natural = await _explain(conn, sql)
                await conn.execute(text("SET LOCAL enable_seqscan = off"))
                forced = await _explain(conn, sql)
                await conn.execute(text("SET LOCAL enable_seqscan = on"))
                results.append(
                    {
                        "query": name,
                        "natural": natural,
                        "forced": forced,
                        "index_available": not forced["seq_scans"],
                    }
                )

            sizes = (
                await conn.execute(
                    text(
                        """SELECT 'table' AS kind, 'upload_jobs' AS name,
                                  pg_relation_size('upload_jobs') AS bytes
                           UNION ALL
                           SELECT 'index', indexrelname, pg_relation_size(indexrelid)
                           FROM pg_stat_user_indexes WHERE relname = 'upload_jobs'
                           ORDER BY 1, 3 DESC"""
                    )
                )
            ).all()
        finally:
            await trans.rollback()
    return {
        "rows": rows,
        "results": results,
        "sizes": [{"kind": r.kind, "name": r.name, "bytes": int(r.bytes)} for r in sizes],
    }


def format_audit(report: dict) -> str:
    out = [f"INDEX AUDIT on {report['rows']:,} temporary rows (rolled back afterwards)", "=" * 78]
    out.append(f"{'query':40} {'natural ms':>10}  {'index?':7} plan")
    for r in report["results"]:
        nat = r["natural"]
        plan = ("SEQ SCAN " + ",".join(nat["seq_scans"])) if nat["seq_scans"] else ("index " + ",".join(nat["indexes"]))
        flag = "yes" if r["index_available"] else "NO INDEX"
        out.append(f"{r['query']:40} {nat['ms']:>10.3f}  {flag:7} {plan}")
    out += ["", "upload_jobs size:"]
    for s in report["sizes"]:
        out.append(f"  {s['kind']:5} {s['name']:36} {s['bytes'] / 1024:>10,.0f} kB")
    missing = [r["query"] for r in report["results"] if not r["index_available"]]
    out.append("")
    out.append("All queries have a usable index." if not missing else "MISSING INDEX for: " + "; ".join(missing))
    return "\n".join(out)


async def _main() -> int:
    p = argparse.ArgumentParser(prog="app.dbaudit")
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    try:
        report = await audit(args.rows)
    finally:
        await engine.dispose()
    print(json.dumps(report, indent=2) if args.json else format_audit(report))
    return 0 if all(r["index_available"] for r in report["results"]) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
