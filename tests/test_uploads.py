"""Upload route tests (real Postgres, Redis and S3).

    docker compose exec api alembic upgrade head
    docker compose exec api pytest -v
"""
import asyncio

import pytest
from sqlalchemy import func, select

from app import storage
from app.client import sign_headers, upload_target
from app.config import get_settings
from app.db import SessionLocal
from app.models import UploadJob

settings = get_settings()

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
PDF = b"%PDF-1.7\n" + b"x" * 64
WEBP = b"RIFF\x00\x00\x00\x00WEBPVP8 " + b"\x00" * 32
EXE_AS_PNG = b"MZ\x90\x00" + b"\x00" * 64


async def _post(client, creds, body, filename, **extra_headers):
    key, secret, _ = creds
    target = upload_target(filename)
    headers = sign_headers(key, secret, "POST", target, body)
    headers.update(extra_headers)
    return await client.post(target, headers=headers, content=body)


async def _job_count(creds) -> int:
    async with SessionLocal() as s:
        return (
            await s.execute(
                select(func.count()).select_from(UploadJob).where(UploadJob.api_key_id == creds[2].id)
            )
        ).scalar_one()


async def _get(client, creds, target):
    key, secret, _ = creds
    return await client.get(target, headers=sign_headers(key, secret, "GET", target))


# ---------- happy paths ----------

@pytest.mark.parametrize(
    "filename, body, content_type",
    [
        ("photo.png", PNG, "image/png"),
        ("photo.JPG", JPEG, "image/jpeg"),
        ("photo.jpeg", JPEG, "image/jpeg"),
        ("photo.webp", WEBP, "image/webp"),
        ("doc.pdf", PDF, "application/pdf"),
        ("notes.txt", b"hello world\n", "text/plain"),
        ("data.csv", b"a,b\n1,2\n", "text/csv"),
        ("data.json", b'{"a": [1, 2]}', "application/json"),
        ("unicode.txt", "héllo €".encode(), "text/plain"),
    ],
)
async def test_valid_uploads_accepted(client, credentials, filename, body, content_type):
    resp = await _post(client, credentials, body, filename)
    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "quarantined"
    assert data["content_type"] == content_type
    assert data["size_bytes"] == len(body)

    # The bytes really landed in the quarantine area, unchanged.
    stored = await asyncio.to_thread(
        storage.get_object_bytes, storage.quarantine_key(data["job_id"])
    )
    assert stored == body


async def test_accepted_upload_is_queued_for_scanning(client, credentials, enqueued):
    resp = await _post(client, credentials, PNG, "photo.png")
    assert enqueued == [resp.json()["job_id"]]


async def test_rejected_upload_is_not_queued(client, credentials, enqueued):
    await _post(client, credentials, EXE_AS_PNG, "photo.png")
    assert enqueued == []


async def test_queue_failure_does_not_lose_the_upload(client, credentials, monkeypatch):
    from app.routers import uploads as uploads_module

    async def broken(job_id, **kwargs):
        raise ConnectionError("redis down")

    monkeypatch.setattr(uploads_module, "enqueue_scan", broken)
    resp = await _post(client, credentials, PNG, "photo.png")
    assert resp.status_code == 202  # the sweeper will pick it up
    assert await _job_count(credentials) == 1


async def test_job_status_and_list(client, credentials):
    resp = await _post(client, credentials, PNG, "photo.png")
    job_id = resp.json()["job_id"]

    status = await _get(client, credentials, f"/v1/uploads/{job_id}")
    assert status.status_code == 200
    assert status.json()["status"] == "quarantined"
    assert status.json()["filename"] == "photo.png"

    listing = await _get(client, credentials, "/v1/uploads?limit=5")
    assert listing.status_code == 200
    assert job_id in [u["job_id"] for u in listing.json()["uploads"]]


# ---------- security: what must be rejected ----------

async def test_upload_requires_auth(client):
    resp = await client.post(upload_target("a.png"), content=PNG)
    assert resp.status_code == 401


async def test_filename_is_covered_by_signature(client, credentials):
    key, secret, _ = credentials
    headers = sign_headers(key, secret, "POST", upload_target("a.png"), PNG)
    # Same signature, but the filename was changed in transit.
    resp = await client.post(upload_target("b.png"), headers=headers, content=PNG)
    assert resp.status_code == 401
    assert await _job_count(credentials) == 0


async def test_missing_filename_rejected(client, credentials):
    key, secret, _ = credentials
    headers = sign_headers(key, secret, "POST", "/v1/uploads", PNG)
    resp = await client.post("/v1/uploads", headers=headers, content=PNG)
    assert resp.status_code == 422


async def test_disallowed_extension_rejected(client, credentials):
    for name in ["run.exe", "page.html", "script.js", "archive.zip", "noextension"]:
        resp = await _post(client, credentials, b"data", name)
        assert resp.status_code == 415, name
    assert await _job_count(credentials) == 0


async def test_disguised_executable_rejected(client, credentials):
    resp = await _post(client, credentials, EXE_AS_PNG, "totally_a_photo.png")
    assert resp.status_code == 415
    assert await _job_count(credentials) == 0


async def test_content_must_match_extension(client, credentials):
    assert (await _post(client, credentials, PNG, "image.pdf")).status_code == 415
    assert (await _post(client, credentials, PDF, "doc.png")).status_code == 415
    assert (await _post(client, credentials, JPEG, "pic.webp")).status_code == 415


async def test_short_binary_file_still_checked(client, credentials):
    # Shorter than the 16 byte header window: must still be validated.
    assert (await _post(client, credentials, b"MZ", "tiny.png")).status_code == 415


@pytest.mark.parametrize(
    "filename, body",
    [
        ("a.txt", b"abc\x00def"),            # NUL byte
        ("a.txt", b"\xff\xfe\xfa\xfb"),      # invalid UTF-8
        ("a.csv", b"\x89PNG\r\n\x1a\n\x00"),  # binary pretending to be CSV
        ("a.json", b"hello, not json"),
        ("a.json", b"   "),
    ],
)
async def test_bad_text_files_rejected(client, credentials, filename, body):
    assert (await _post(client, credentials, body, filename)).status_code == 415
    assert await _job_count(credentials) == 0


async def test_empty_upload_rejected(client, credentials):
    assert (await _post(client, credentials, b"", "empty.txt")).status_code == 400


async def test_body_must_match_signed_hash(client, credentials):
    key, secret, _ = credentials
    # Sign for one body, send a different (but valid) one.
    target = upload_target("photo.png")
    headers = sign_headers(key, secret, "POST", target, PNG)
    resp = await client.post(target, headers=headers, content=PNG + b"tampered")
    assert resp.status_code == 400
    assert await _job_count(credentials) == 0


async def test_oversized_rejected_via_content_length(client, credentials, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_bytes", 1000)
    resp = await _post(client, credentials, PNG + b"\x00" * 2000, "big.png")
    assert resp.status_code == 413
    assert await _job_count(credentials) == 0


async def test_oversized_rejected_when_streamed_without_content_length(
    client, credentials, monkeypatch
):
    monkeypatch.setattr(settings, "max_upload_bytes", 1000)
    body = PNG + b"\x00" * 2000
    key, secret, _ = credentials
    target = upload_target("big.png")
    headers = sign_headers(key, secret, "POST", target, body)

    async def chunks():  # chunked transfer: the server never sees a Content-Length
        for i in range(0, len(body), 500):
            yield body[i : i + 500]

    resp = await client.post(target, headers=headers, content=chunks())
    assert resp.status_code == 413
    assert await _job_count(credentials) == 0


@pytest.mark.parametrize(
    "evil_name",
    ["../../evil.txt", "..\\..\\evil.txt", "/etc/evil.txt", "a/b/../evil.txt"],
)
async def test_path_traversal_filenames_neutralised(client, credentials, evil_name):
    key, secret, _ = credentials
    body = b"harmless"
    target = upload_target(evil_name)
    headers = sign_headers(key, secret, "POST", target, body)
    resp = await client.post(target, headers=headers, content=body)
    assert resp.status_code == 202
    data = resp.json()
    assert data["filename"] == "evil.txt"
    # The S3 key is built from the job ID, never from the filename.
    assert await asyncio.to_thread(storage.object_exists, f"quarantine/{data['job_id']}")
    assert not await asyncio.to_thread(storage.object_exists, "evil.txt")


async def test_non_ascii_filename_roundtrip(client, credentials):
    resp = await _post(client, credentials, b"data", "résumé 2026.txt")
    assert resp.status_code == 202
    assert resp.json()["filename"] == "résumé 2026.txt"


async def test_s3_failure_leaves_no_job(client, credentials, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(storage, "put_fileobj", boom)
    resp = await _post(client, credentials, PNG, "photo.png")
    assert resp.status_code == 503
    assert await _job_count(credentials) == 0


# ---------- isolation between API keys ----------

async def test_cannot_read_another_keys_job(client, credentials, other_credentials):
    job_id = (await _post(client, credentials, PNG, "photo.png")).json()["job_id"]
    resp = await _get(client, other_credentials, f"/v1/uploads/{job_id}")
    assert resp.status_code == 404
    listing = await _get(client, other_credentials, "/v1/uploads")
    assert listing.json()["uploads"] == []


async def test_unknown_job_id_404(client, credentials):
    resp = await _get(client, credentials, "/v1/uploads/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
