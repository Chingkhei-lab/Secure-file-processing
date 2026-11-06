"""Scanner protocol tests, using a fake clamd (no ClamAV needed)."""
import io

import pytest

from app.scanner import ScanError, parse_reply, scan_file
from tests.fake_clamd import FakeClamd


def test_parse_clean_reply():
    assert parse_reply(b"stream: OK\0").infected is False


def test_parse_infected_reply():
    result = parse_reply(b"stream: Win.Test.EICAR_HDB-1 FOUND\0")
    assert result.infected is True
    assert result.signature == "Win.Test.EICAR_HDB-1"


@pytest.mark.parametrize(
    "reply",
    [
        b"INSTREAM size limit exceeded. ERROR\0",
        b"stream: Can't allocate memory ERROR\0",
        b"garbage\0",
        b"\0",
        b"",
    ],
)
def test_anything_unclear_fails_closed(reply):
    with pytest.raises(ScanError):
        parse_reply(reply)


async def test_streams_file_with_correct_framing():
    data = bytes(range(256)) * 14  # 3584 bytes
    async with FakeClamd() as clamd:
        result = await scan_file(io.BytesIO(data), host="127.0.0.1", port=clamd.port, chunk_size=1000)
    assert result.infected is False
    assert clamd.command == b"zINSTREAM\0"
    assert clamd.received == data
    assert clamd.chunk_sizes == [1000, 1000, 1000, 584]


async def test_infected_verdict_returned():
    async with FakeClamd(reply=b"stream: Eicar-Test-Signature FOUND\0") as clamd:
        result = await scan_file(io.BytesIO(b"whatever"), host="127.0.0.1", port=clamd.port)
    assert result.infected and result.signature == "Eicar-Test-Signature"


async def test_empty_stream_is_still_terminated():
    async with FakeClamd() as clamd:
        await scan_file(io.BytesIO(b""), host="127.0.0.1", port=clamd.port)
    assert clamd.received == b"" and clamd.chunk_sizes == []


async def test_clamd_error_reply_raises():
    async with FakeClamd(reply=b"INSTREAM size limit exceeded. ERROR\0") as clamd:
        with pytest.raises(ScanError):
            await scan_file(io.BytesIO(b"x"), host="127.0.0.1", port=clamd.port)


async def test_unreachable_clamd_raises():
    with pytest.raises(ScanError):
        await scan_file(io.BytesIO(b"x"), host="127.0.0.1", port=1, timeout=2)


async def test_hung_clamd_times_out():
    async with FakeClamd(hang=True) as clamd:
        with pytest.raises(ScanError, match="timed out"):
            await scan_file(io.BytesIO(b"x"), host="127.0.0.1", port=clamd.port, timeout=0.5)
