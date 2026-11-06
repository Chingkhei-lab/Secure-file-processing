"""Dependency health checks.

Each check returns "ok" or an error string, so /health shows exactly which
service is broken instead of a generic 500.
"""
import asyncio

import boto3
import redis.asyncio as aioredis
from botocore.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings

CHECK_TIMEOUT = 3.0  # seconds per dependency


async def check_postgres(engine: AsyncEngine) -> str:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return "ok"


async def check_redis(settings: Settings) -> str:
    client = aioredis.from_url(settings.redis_url)
    try:
        await client.ping()
    finally:
        await client.aclose()
    return "ok"


def make_s3_client(settings: Settings):
    # Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}),
    )


async def check_s3(settings: Settings) -> str:
    client = make_s3_client(settings)
    # boto3 is synchronous, so run it in a thread to avoid blocking the event loop.
    await asyncio.to_thread(client.head_bucket, Bucket=settings.s3_bucket)
    return "ok"


async def check_clamav(settings: Settings) -> str:
    """Talk to clamd directly: send PING, expect PONG."""
    reader, writer = await asyncio.open_connection(
        settings.clamav_host, settings.clamav_port
    )
    try:
        writer.write(b"zPING\0")
        await writer.drain()
        reply = await reader.read(16)
    finally:
        writer.close()
        await writer.wait_closed()
    if reply.strip(b"\0\n") != b"PONG":
        return f"unexpected reply: {reply!r}"
    return "ok"


async def safe(name: str, coro) -> tuple[str, str]:
    """Run one check with a timeout; never raise."""
    try:
        return name, await asyncio.wait_for(coro, timeout=CHECK_TIMEOUT)
    except asyncio.TimeoutError:
        return name, "error: timeout"
    except Exception as exc:  # noqa: BLE001 - health endpoint must not crash
        return name, f"error: {type(exc).__name__}"


async def run_all_checks(engine: AsyncEngine, settings: Settings) -> dict[str, str]:
    results = await asyncio.gather(
        safe("postgres", check_postgres(engine)),
        safe("redis", check_redis(settings)),
        safe("s3", check_s3(settings)),
        safe("clamav", check_clamav(settings)),
    )
    return dict(results)
