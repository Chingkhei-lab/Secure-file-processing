"""Index-audit tests: it must find indexes for every query and leave no trace."""
from sqlalchemy import text

from app import dbaudit
from app.db import SessionLocal


async def _counts():
    async with SessionLocal() as s:
        return (
            await s.execute(
                text(
                    """SELECT (SELECT count(*) FROM upload_jobs) AS jobs,
                              (SELECT count(*) FROM stored_objects) AS objects,
                              (SELECT count(*) FROM api_keys WHERE key_prefix = 'audit') AS keys"""
                )
            )
        ).one()


async def test_every_query_has_a_usable_index():
    report = await dbaudit.audit(rows=3000)
    assert [r["query"] for r in report["results"]] == [q for q, _ in dbaudit.QUERIES]
    missing = [r["query"] for r in report["results"] if not r["index_available"]]
    assert missing == [], f"queries without an index: {missing}"


async def test_audit_leaves_the_database_untouched():
    before = await _counts()
    await dbaudit.audit(rows=2000)
    after = await _counts()
    assert tuple(after) == tuple(before)
    assert after.keys == 0  # the temporary API key was rolled back too


async def test_report_lists_sizes_and_formats():
    report = await dbaudit.audit(rows=2000)
    names = {s["name"] for s in report["sizes"]}
    assert {"upload_jobs", "ix_upload_jobs_failed_retained", "ix_upload_jobs_terminal_completed"} <= names
    text_out = dbaudit.format_audit(report)
    assert "INDEX AUDIT" in text_out and "All queries have a usable index." in text_out
