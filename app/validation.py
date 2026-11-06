"""Upload validation. Pure functions, no I/O, so they are easy to test.

The rule: never trust anything the client says about a file. The filename is
sanitized, the extension must be on an allowlist, and the actual bytes must
match what that extension claims.
"""
import codecs
import os
import re
import unicodedata
from dataclasses import dataclass

HEAD_BYTES = 16  # how many leading bytes we keep for magic-number checks
MAX_FILENAME_LEN = 100


@dataclass(frozen=True)
class FileType:
    content_type: str
    kind: str  # "binary" (checked by magic bytes) or "text" (checked as UTF-8)
    magic: tuple[tuple[int, bytes], ...] = ()  # (offset, expected bytes) pairs


ALLOWED: dict[str, FileType] = {
    ".png": FileType("image/png", "binary", ((0, b"\x89PNG\r\n\x1a\n"),)),
    ".jpg": FileType("image/jpeg", "binary", ((0, b"\xff\xd8\xff"),)),
    ".jpeg": FileType("image/jpeg", "binary", ((0, b"\xff\xd8\xff"),)),
    ".webp": FileType("image/webp", "binary", ((0, b"RIFF"), (8, b"WEBP"))),
    ".pdf": FileType("application/pdf", "binary", ((0, b"%PDF-"),)),
    ".txt": FileType("text/plain", "text"),
    ".csv": FileType("text/csv", "text"),
    ".json": FileType("application/json", "text"),
}


def sanitize_filename(raw: str | None) -> str:
    """Return a safe display name. It is only ever stored as a label: S3 keys
    are built from the job ID, never from this string."""
    name = unicodedata.normalize("NFKC", raw or "")
    # Keep only the last path component (handles both / and \ separators).
    name = name.replace("\\", "/").split("/")[-1]
    # Drop control characters and invisible/bidi-override characters.
    name = "".join(ch for ch in name if ch.isprintable())
    name = re.sub(r"[^\w.\- ]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip(" .")
    if len(name) > MAX_FILENAME_LEN:
        stem, ext = os.path.splitext(name)
        name = stem[: MAX_FILENAME_LEN - len(ext)] + ext
    return name or "upload"


def extension_of(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()


def matches_magic(ftype: FileType, head: bytes) -> bool:
    return all(head[off : off + len(sig)] == sig for off, sig in ftype.magic)


# Control characters that never appear in real text files (tab, LF, CR are fine).
_BAD_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


class TextValidator:
    """Incremental check that a stream is clean UTF-8 text.

    Fed chunk by chunk, so memory use does not grow with file size. Handles
    multi-byte characters split across chunk boundaries.
    """

    def __init__(self, expect_json: bool = False) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._expect_json = expect_json
        self._json_start_seen = False

    def feed(self, chunk: bytes) -> None:
        try:
            text = self._decoder.decode(chunk)
        except UnicodeDecodeError:
            raise ValueError("not valid UTF-8 text") from None
        if _BAD_CONTROL.search(text):
            raise ValueError("contains binary control characters")
        if self._expect_json and not self._json_start_seen:
            stripped = text.lstrip("﻿ \t\r\n")
            if stripped:
                if stripped[0] not in "{[":
                    raise ValueError("JSON must start with { or [")
                self._json_start_seen = True

    def finish(self) -> None:
        try:
            self._decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            raise ValueError("not valid UTF-8 text") from None
        if self._expect_json and not self._json_start_seen:
            raise ValueError("empty JSON document")
