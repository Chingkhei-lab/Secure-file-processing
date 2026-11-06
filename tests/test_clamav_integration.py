"""End-to-end tests against a REAL ClamAV, using the harmless EICAR test string.

EICAR is an industry-standard test file that every antivirus detects on
purpose. It is NOT malware and is built here at runtime, never committed.

These tests are skipped automatically when ClamAV is not reachable. Inside
docker compose it is, so they run as part of `pytest`.
"""
import asyncio
import io
import socket

import pytest

from app import scanner, storage, worker
from app.config import get_settings
from app.db import SessionLocal
from app.models import UploadJob
from tests.test_worker import _make_job, _row

settings = get_settings()

# Assembled from two halves so this source file itself is not flagged by
# antivirus tools that scan repositories.
EICAR = (
    "X5O!P%@AP[4\\PZX54(P^)7CC)7}$" + "EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
).encode()


def _clamav_ready() -> bool:
    try:
        with socket.create_connection((settings.clamav_host, settings.clamav_port), timeout=2) as s:
            s.sendall(b"zPING\0")
            return s.recv(16).startswith(b"PONG")
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _clamav_ready(), reason="ClamAV is not reachable")


async def test_real_clamav_detects_eicar():
    result = await scanner.scan_file(io.BytesIO(EICAR))
    assert result.infected is True
    assert "eicar" in result.signature.lower()


async def test_real_clamav_passes_clean_text():
    result = await scanner.scan_file(io.BytesIO(b"just an ordinary text file\n"))
    assert result.infected is False


async def test_full_pipeline_infected_file_is_deleted(credentials):
    job_id = await _make_job(credentials, EICAR)
    assert await worker.scan_upload({}, str(job_id)) == "infected"
    row = await _row(job_id)
    assert row.status == "infected" and "eicar" in row.scan_signature.lower()
    assert not await asyncio.to_thread(storage.object_exists, storage.quarantine_key(job_id))


async def test_full_pipeline_clean_file_passes(credentials):
    job_id = await _make_job(credentials, b"a perfectly harmless document\n")
    assert await worker.scan_upload({}, str(job_id)) == "clean"
    assert (await _row(job_id)).status == "clean"
