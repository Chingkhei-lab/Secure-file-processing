"""A tiny stand-in for clamd that speaks just enough of the INSTREAM protocol
to test our client without a real ClamAV."""
import asyncio
import struct


class FakeClamd:
    def __init__(self, reply: bytes = b"stream: OK\0", hang: bool = False):
        self.reply = reply
        self.hang = hang
        self.command = b""
        self.received = b""
        self.chunk_sizes: list[int] = []
        self.port = 0
        self._server = None
        self._release = asyncio.Event()  # set on exit so a "hung" server lets go

    async def __aenter__(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self._release.set()
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader, writer):
        try:
            self.command = await reader.readuntil(b"\0")
            data = bytearray()
            while True:
                (length,) = struct.unpack(">I", await reader.readexactly(4))
                if length == 0:
                    break
                data += await reader.readexactly(length)
                self.chunk_sizes.append(length)
            self.received = bytes(data)
            if self.hang:
                await self._release.wait()
            writer.write(self.reply)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
