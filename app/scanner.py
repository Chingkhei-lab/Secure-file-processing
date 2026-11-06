"""ClamAV client (clamd INSTREAM protocol).

Protocol, in short: send "zINSTREAM\\0", then the file as chunks, each
prefixed by its length as a 4-byte big-endian integer, then a zero-length
chunk to finish. clamd replies "stream: OK\\0" or "stream: <Name> FOUND\\0".

The scanner FAILS CLOSED: anything other than a clear "OK" or "FOUND"
(timeout, connection error, size-limit error, garbage reply) raises
ScanError. A file is never treated as clean just because scanning broke.
"""
import asyncio
import struct
from dataclasses import dataclass
from typing import BinaryIO

from app.config import get_settings

CHUNK_SIZE = 256 * 1024
DEFAULT_TIMEOUT = 120.0


class ScanError(Exception):
    """The file could not be scanned. It must NOT be treated as clean."""


@dataclass(frozen=True)
class ScanResult:
    infected: bool
    signature: str | None = None


def parse_reply(reply: bytes) -> ScanResult:
    text = reply.rstrip(b"\0\n").decode("utf-8", "replace").strip()
    if text.startswith("stream:"):
        text = text[len("stream:") :].strip()
    if text == "OK":
        return ScanResult(infected=False)
    if text.endswith(" FOUND"):
        return ScanResult(infected=True, signature=text[: -len(" FOUND")].strip()[:200])
    raise ScanError(f"unexpected clamd reply: {text[:200]!r}")


async def scan_file(
    fileobj: BinaryIO,
    *,
    host: str | None = None,
    port: int | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    chunk_size: int = CHUNK_SIZE,
) -> ScanResult:
    settings = get_settings()
    host = host or settings.clamav_host
    port = port or settings.clamav_port

    async def _run() -> ScanResult:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            writer.write(b"zINSTREAM\0")
            while True:
                # File reads happen in a thread so a big file never blocks the loop.
                chunk = await asyncio.to_thread(fileobj.read, chunk_size)
                if not chunk:
                    break
                writer.write(struct.pack(">I", len(chunk)) + chunk)
                await writer.drain()
            writer.write(struct.pack(">I", 0))
            await writer.drain()
            reply = await reader.readuntil(b"\0")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 - closing is best effort
                pass
        return parse_reply(reply)

    try:
        return await asyncio.wait_for(_run(), timeout=timeout)
    except ScanError:
        raise
    except asyncio.TimeoutError:
        raise ScanError(f"scan timed out after {timeout:.0f}s") from None
    except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError) as exc:
        raise ScanError(f"clamd connection problem: {type(exc).__name__}") from None
