"""Request authentication: API key + HMAC signature + replay and rate limits.

Required headers:
    X-API-Key         the API key
    X-Timestamp       unix seconds when the request was signed
    X-Signature       hex HMAC-SHA256 (see app/security.py)
    X-Content-SHA256  hex SHA-256 of the body (optional for bodyless requests)

Every authentication failure returns the same generic 401, so an attacker
cannot tell a wrong key from a wrong signature. The real reason is logged.
"""
import hmac
import logging
import time
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.models import ApiKey
from app.redis_client import get_redis
from app.security import (
    EMPTY_SHA256,
    compute_signature,
    derive_signing_secret,
    hash_api_key,
    is_valid_sha256_hex,
)

log = logging.getLogger("fileservice.auth")
settings = get_settings()

LAST_USED_UPDATE_INTERVAL = 60  # seconds; avoids a DB write on every request


def _unauthorized(reason: str) -> HTTPException:
    log.warning("auth failed: %s", reason)
    return HTTPException(
        status_code=401,
        detail="Invalid credentials",
        headers={"WWW-Authenticate": "Signature"},
    )


async def enforce_rate_limit(redis: aioredis.Redis, key_id: str, limit: int) -> None:
    """Fixed-window limiter: at most `limit` requests per API key per minute."""
    now = int(time.time())
    window = now // 60
    redis_key = f"rl:{key_id}:{window}"
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(redis_key)
        pipe.expire(redis_key, 60)
        count, _ = await pipe.execute()
    if count > limit:
        retry_after = 60 - (now % 60)
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )


async def require_auth(
    request: Request,
    session: AsyncSession = Depends(get_session),
    redis: aioredis.Redis = Depends(get_redis),
) -> ApiKey:
    headers = request.headers
    api_key = headers.get("x-api-key")
    timestamp = headers.get("x-timestamp")
    signature = headers.get("x-signature")
    content_sha256 = headers.get("x-content-sha256", EMPTY_SHA256).lower()

    if not (api_key and timestamp and signature):
        raise _unauthorized("missing auth headers")
    if not is_valid_sha256_hex(content_sha256):
        raise _unauthorized("malformed X-Content-SHA256")

    # 1. Cheap check first: is the timestamp fresh?
    try:
        ts = int(timestamp)
    except ValueError:
        raise _unauthorized("non-numeric timestamp") from None
    if abs(time.time() - ts) > settings.signature_window_seconds:
        raise _unauthorized("timestamp outside allowed window")

    # 2. Look the key up by its hash.
    row = (
        await session.execute(select(ApiKey).where(ApiKey.key_hash == hash_api_key(api_key)))
    ).scalar_one_or_none()
    if row is None:
        raise _unauthorized("unknown api key")
    if not row.is_active:
        raise _unauthorized(f"revoked key {row.key_prefix}")

    # 3. Verify the signature (constant-time comparison).
    target = request.url.path + (f"?{request.url.query}" if request.url.query else "")
    secret = derive_signing_secret(settings.signing_master_secret, row.id)
    expected = compute_signature(secret, timestamp, request.method, target, content_sha256)
    if not hmac.compare_digest(expected, signature.lower()):
        raise _unauthorized(f"bad signature for key {row.key_prefix}")

    # 4. Replay protection. Done only AFTER the signature checks out, so
    #    forged requests cannot fill Redis. A signature stays acceptable for
    #    up to 2x the window (timestamp may be up to +window in the future),
    #    so the marker lives that long.
    first_use = await redis.set(
        f"replay:{expected}", "1", nx=True, ex=settings.signature_window_seconds * 2
    )
    if not first_use:
        raise _unauthorized(f"replayed signature for key {row.key_prefix}")

    # 5. Rate limit per key.
    await enforce_rate_limit(redis, str(row.id), settings.rate_limit_per_minute)

    # 6. Record usage, throttled.
    now = datetime.now(timezone.utc)
    if (
        row.last_used_at is None
        or (now - row.last_used_at).total_seconds() > LAST_USED_UPDATE_INTERVAL
    ):
        row.last_used_at = now
        await session.commit()

    # The upload route (Phase 4) compares this against the bytes it actually
    # receives, so a signed hash cannot be paired with a different body.
    request.state.content_sha256 = content_sha256
    return row
