"""Ingestion routes. Deliberately kept in their own router so the upload path
has its own rules (size limit, strict validation, quarantine) separate from
the rest of the API.

Upload protocol: POST /v1/uploads?filename=<name> with the RAW file bytes as
the body.
    filename          query parameter, so it is covered by the HMAC signature
                      (the signature covers the full path + query string)
    X-Content-SHA256  hex SHA-256 of the body (also covered by the signature)
    + the usual auth headers (see app/auth.py)

Flow: authenticate -> cheap header checks -> stream the body to a temp file
while hashing and validating it -> verify the hash against the signed value
-> upload to the quarantine prefix in S3 -> record a job -> return 202.
Nothing is trusted or processed further until a worker scans it (Phase 5).
"""
import asyncio
import hashlib
import hmac
import logging
import tempfile
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app import storage
from app.auth import require_auth
from app.config import get_settings
from app.db import get_session
from app.jobs import enqueue_scan
from app.models import ApiKey, JobStatus, UploadJob
from app.validation import (
    ALLOWED,
    HEAD_BYTES,
    TextValidator,
    extension_of,
    matches_magic,
    sanitize_filename,
)

log = logging.getLogger("fileservice.uploads")
settings = get_settings()
router = APIRouter(prefix="/v1/uploads", tags=["uploads"])

SPOOL_MEMORY_BYTES = 1024 * 1024  # buffer up to 1 MB in RAM, spill to disk beyond


def _job_out(job: UploadJob) -> dict:
    return {
        "job_id": str(job.id),
        "status": job.status,
        "filename": job.original_filename,
        "content_type": job.content_type,
        "size_bytes": job.size_bytes,
        "sha256": job.sha256,
        "error": job.error,
        "scan_signature": job.scan_signature,
        "scanned_at": job.scanned_at.isoformat() if job.scanned_at else None,
        # Only describes the stored blob. Whether it was a duplicate of someone
        # else's file is deliberately NOT exposed (it would let an uploader
        # probe whether other customers hold a given file).
        "compression": job.stored_object.compression if job.stored_object else None,
        "stored_size": job.stored_object.stored_size if job.stored_object else None,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


@router.post("", status_code=202)
async def create_upload(
    request: Request,
    filename_raw: str = Query(..., alias="filename", max_length=1000),
    key: ApiKey = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
):
    max_bytes = settings.max_upload_bytes

    # --- 1. Cheap rejections before reading a single byte of the body ---
    declared_length = request.headers.get("content-length")
    if declared_length is not None:
        if not declared_length.isdigit():
            raise HTTPException(400, "Invalid Content-Length")
        if int(declared_length) > max_bytes:
            raise HTTPException(413, f"File too large (limit {max_bytes} bytes)")

    filename = sanitize_filename(filename_raw)
    ext = extension_of(filename)
    ftype = ALLOWED.get(ext)
    if ftype is None:
        raise HTTPException(415, "File type not allowed")
    declared_type = (request.headers.get("content-type") or "")[:100] or None

    # --- 2. Stream the body: count, hash and validate as it arrives ---
    # Content-Length can lie (or be absent for chunked uploads), so the limit
    # is enforced on the bytes actually received.
    hasher = hashlib.sha256()
    size = 0
    head = bytearray()
    magic_checked = False
    text_check = TextValidator(expect_json=(ext == ".json")) if ftype.kind == "text" else None
    spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MEMORY_BYTES)

    try:
        async for chunk in request.stream():
            size += len(chunk)
            if size > max_bytes:
                raise HTTPException(413, f"File too large (limit {max_bytes} bytes)")
            hasher.update(chunk)
            if len(head) < HEAD_BYTES:
                head += chunk[: HEAD_BYTES - len(head)]
            if ftype.kind == "binary" and not magic_checked and len(head) >= HEAD_BYTES:
                magic_checked = True
                if not matches_magic(ftype, bytes(head)):
                    raise HTTPException(415, "File content does not match its extension")
            if text_check is not None:
                try:
                    text_check.feed(chunk)
                except ValueError as exc:
                    raise HTTPException(415, f"Invalid text file: {exc}") from None
            spool.write(chunk)

        # --- 3. Checks that need the complete body ---
        if size == 0:
            raise HTTPException(400, "Empty upload")
        if ftype.kind == "binary" and not magic_checked:
            if not matches_magic(ftype, bytes(head)):
                raise HTTPException(415, "File content does not match its extension")
        if text_check is not None:
            try:
                text_check.finish()
            except ValueError as exc:
                raise HTTPException(415, f"Invalid text file: {exc}") from None

        digest = hasher.hexdigest()
        # The signature covered X-Content-SHA256; make sure the bytes we really
        # received are the bytes that were signed.
        if not hmac.compare_digest(digest, request.state.content_sha256):
            raise HTTPException(400, "Body does not match signed X-Content-SHA256")

        # --- 4. Quarantine in S3, then record the job (one DB transaction) ---
        job_id = uuid.uuid4()
        qkey = storage.quarantine_key(job_id)
        job = UploadJob(
            id=job_id,
            api_key_id=key.id,
            original_filename=filename,
            declared_content_type=declared_type,
            content_type=ftype.content_type,
            sha256=digest,
            size_bytes=size,
            status=JobStatus.QUARANTINED.value,
            quarantine_key=qkey,
        )
        session.add(job)
        await session.flush()

        spool.seek(0)
        try:
            await asyncio.to_thread(storage.put_fileobj, spool, qkey)
        except Exception:
            log.exception("quarantine upload failed for job %s", job_id)
            await session.rollback()
            raise HTTPException(503, "Storage temporarily unavailable") from None

        try:
            await session.commit()
        except Exception:
            log.exception("db commit failed for job %s; removing quarantined object", job_id)
            await asyncio.to_thread(storage.delete_object, qkey)
            raise HTTPException(503, "Database temporarily unavailable") from None
    finally:
        spool.close()

    # The job is safely committed. If queueing fails the upload still
    # succeeds: the worker's sweeper will find the stranded job and enqueue it.
    try:
        await enqueue_scan(job_id)
    except Exception:  # noqa: BLE001
        log.exception("could not enqueue scan for job %s; sweeper will retry", job_id)
    log.info("accepted upload job=%s key=%s size=%d", job_id, key.key_prefix, size)
    return {
        "job_id": str(job_id),
        "status": JobStatus.QUARANTINED.value,
        "filename": filename,
        "content_type": ftype.content_type,
        "size_bytes": size,
        "sha256": digest,
    }


@router.get("/{job_id}")
async def get_upload(
    job_id: uuid.UUID,
    key: ApiKey = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
):
    job = await session.get(UploadJob, job_id, options=[joinedload(UploadJob.stored_object)])
    # 404 (not 403) for other people's jobs, so IDs cannot be probed.
    if job is None or job.api_key_id != key.id:
        raise HTTPException(404, "Upload not found")
    return _job_out(job)


@router.get("")
async def list_uploads(
    limit: int = Query(20, ge=1, le=100),
    key: ApiKey = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
):
    # Served by the ix_upload_jobs_owner_created index.
    rows = (
        await session.execute(
            select(UploadJob)
            .options(joinedload(UploadJob.stored_object))
            .where(UploadJob.api_key_id == key.id)
            .order_by(UploadJob.created_at.desc())
            .limit(limit)
        )
    ).scalars()
    return {"uploads": [_job_out(j) for j in rows]}
