"""Authentication tests (need the real Postgres and Redis from docker compose).

    docker compose exec api alembic upgrade head
    docker compose exec api pytest -v
"""
import time
import uuid

from app.client import sign_headers
from app.config import get_settings
from app.db import SessionLocal
from app.models import ApiKey
from app.security import (
    canonical_string,
    compute_signature,
    derive_signing_secret,
    generate_api_key,
    hash_api_key,
)

settings = get_settings()


# ---------- pure unit tests (no services needed) ----------

def test_generated_keys_are_unique_and_prefixed():
    a, b = generate_api_key(), generate_api_key()
    assert a != b and a.startswith("fsk_") and len(a) > 40


def test_hash_is_deterministic_and_not_the_key():
    key = generate_api_key()
    assert hash_api_key(key) == hash_api_key(key) != key


def test_signing_secret_differs_per_key_id_and_master():
    id1, id2 = uuid.uuid4(), uuid.uuid4()
    assert derive_signing_secret("m", id1) != derive_signing_secret("m", id2)
    assert derive_signing_secret("m1", id1) != derive_signing_secret("m2", id1)


def test_signature_changes_if_any_field_changes():
    base = ("secret", "1000", "GET", "/x", "a" * 64)
    sig = compute_signature(*base)
    assert sig == compute_signature(*base)
    assert sig != compute_signature("other", "1000", "GET", "/x", "a" * 64)
    assert sig != compute_signature("secret", "1001", "GET", "/x", "a" * 64)
    assert sig != compute_signature("secret", "1000", "POST", "/x", "a" * 64)
    assert sig != compute_signature("secret", "1000", "GET", "/y", "a" * 64)
    assert sig != compute_signature("secret", "1000", "GET", "/x", "b" * 64)


def test_canonical_string_format():
    assert canonical_string("1", "get", "/p?q=1", "h") == "1\nGET\n/p?q=1\nh"


# ---------- integration tests ----------

# The `client` and `credentials` fixtures live in tests/conftest.py.

async def _get(client, creds, target="/v1/whoami", **overrides):
    key, secret, _ = creds
    headers = sign_headers(key, secret, "GET", target)
    headers.update(overrides)
    return await client.get(target, headers=headers)


async def test_valid_signed_request_succeeds(client, credentials):
    resp = await _get(client, credentials)
    assert resp.status_code == 200
    assert resp.json()["name"] == "pytest"


async def test_missing_headers_rejected(client):
    assert (await client.get("/v1/whoami")).status_code == 401


async def test_wrong_signature_rejected(client, credentials):
    resp = await _get(client, credentials, **{"X-Signature": "0" * 64})
    assert resp.status_code == 401


async def test_unknown_key_rejected(client, credentials):
    _, secret, _ = credentials
    headers = sign_headers(generate_api_key(), secret, "GET", "/v1/whoami")
    assert (await client.get("/v1/whoami", headers=headers)).status_code == 401


async def test_signature_bound_to_path_and_query(client, credentials):
    key, secret, _ = credentials
    headers = sign_headers(key, secret, "GET", "/v1/whoami?a=1")
    # Same signature, different query string -> must fail.
    resp = await client.get("/v1/whoami?a=2", headers=headers)
    assert resp.status_code == 401


async def test_signature_bound_to_body_hash(client, credentials):
    # Signed for one body hash, but we claim another.
    resp = await _get(client, credentials, **{"X-Content-SHA256": "b" * 64})
    assert resp.status_code == 401


async def test_stale_timestamp_rejected(client, credentials):
    key, secret, _ = credentials
    old = int(time.time()) - settings.signature_window_seconds - 60
    headers = sign_headers(key, secret, "GET", "/v1/whoami", timestamp=old)
    assert (await client.get("/v1/whoami", headers=headers)).status_code == 401


async def test_replayed_request_rejected(client, credentials):
    key, secret, _ = credentials
    headers = sign_headers(key, secret, "GET", "/v1/whoami")
    first = await client.get("/v1/whoami", headers=headers)
    second = await client.get("/v1/whoami", headers=headers)
    assert first.status_code == 200
    assert second.status_code == 401


async def test_revoked_key_rejected(client, credentials):
    _, _, row = credentials
    async with SessionLocal() as s:
        db_row = await s.get(ApiKey, row.id)
        db_row.is_active = False
        await s.commit()
    assert (await _get(client, credentials)).status_code == 401


async def test_rate_limit_returns_429(client, credentials, monkeypatch):
    monkeypatch.setattr(settings, "rate_limit_per_minute", 3)
    codes = []
    for i in range(5):
        # A different query string each time gives a different signature,
        # so these are not rejected as replays.
        resp = await _get(client, credentials, target=f"/v1/whoami?n={i}")
        codes.append(resp.status_code)
    assert codes[:3] == [200, 200, 200]
    assert codes[3:] == [429, 429]
    assert "retry-after" in resp.headers
