"""Type-aware compression. Synchronous and CPU-bound: call it through
asyncio.to_thread.

Policy
------
* Images (png, jpeg)   -> WebP (lossy or lossless, see settings). Metadata
                          such as EXIF/GPS is dropped, which is also a privacy
                          benefit. Orientation is applied first so pictures do
                          not end up rotated. Animated images and WebP inputs
                          are left alone.
* Text (txt/csv/json)  -> zstd.
* PDF                  -> structural optimisation with qpdf (pikepdf).
* Anything else        -> stored as is.
A result is used ONLY if it is strictly smaller than the original; otherwise
the original bytes are kept (method "none"), so compression can never make
storage bigger.

Security: these functions parse untrusted files. Images are rejected before
decoding if their pixel count is above a limit (decompression bombs), and an
image that cannot be decoded is rejected outright. A PDF that qpdf cannot
handle is simply stored unchanged, since optimisation is optional there.
"""
import logging
import tempfile
import warnings
from dataclasses import dataclass
from typing import BinaryIO

import pikepdf
import zstandard
from PIL import Image, ImageOps, UnidentifiedImageError

log = logging.getLogger("fileservice.compression")

IMAGE_FORMATS = {"image/png": "PNG", "image/jpeg": "JPEG"}
ZSTD_TYPES = {"text/plain", "text/csv", "application/json"}


class InvalidContent(Exception):
    """The file claims to be one thing but cannot be parsed as that thing."""


@dataclass
class Compressed:
    method: str  # none | zstd | webp | pdf_opt
    fileobj: BinaryIO  # positioned at 0, holds the bytes to store
    size: int


def _size_of(f: BinaryIO) -> int:
    f.seek(0, 2)
    n = f.tell()
    f.seek(0)
    return n


def compress(
    src: BinaryIO,
    content_type: str,
    *,
    image_mode: str = "lossy",
    image_quality: int = 80,
    zstd_level: int = 15,
    max_image_pixels: int = 50_000_000,
) -> Compressed:
    """Return the smallest acceptable representation of `src`.

    The returned fileobj is either `src` itself (method "none") or a new
    temporary file; callers close both.
    """
    original = _size_of(src)
    candidate: Compressed | None = None

    if content_type in IMAGE_FORMATS:
        candidate = _to_webp(
            src, IMAGE_FORMATS[content_type], image_mode, image_quality, max_image_pixels
        )
    elif content_type in ZSTD_TYPES:
        candidate = _to_zstd(src, original, zstd_level)
    elif content_type == "application/pdf":
        candidate = _optimise_pdf(src)

    if candidate is not None and candidate.size < original:
        return candidate
    if candidate is not None:
        candidate.fileobj.close()
    src.seek(0)
    return Compressed("none", src, original)


def _to_zstd(src: BinaryIO, original_size: int, level: int) -> Compressed:
    src.seek(0)
    out = tempfile.TemporaryFile()
    zstandard.ZstdCompressor(level=level).copy_stream(src, out, size=original_size)
    size = _size_of(out)
    return Compressed("zstd", out, size)


def _to_webp(
    src: BinaryIO, expected_format: str, mode: str, quality: int, max_pixels: int
) -> Compressed | None:
    src.seek(0)
    Image.MAX_IMAGE_PIXELS = max_pixels
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            im = Image.open(src)
            if im.format != expected_format:
                raise InvalidContent("image content does not match its type")
            width, height = im.size
            if width * height > max_pixels:
                raise InvalidContent("image dimensions are too large")
            if getattr(im, "is_animated", False) and getattr(im, "n_frames", 1) > 1:
                return None  # converting would drop the animation: keep original
            im.load()
    except InvalidContent:
        raise
    except (Image.DecompressionBombError, Image.DecompressionBombWarning) as exc:
        # Pillow's own guard fired (far above our limit): same verdict as our check.
        raise InvalidContent("image dimensions are too large") from exc
    except (
        UnidentifiedImageError,
        OSError,
        SyntaxError,
        ValueError,
        EOFError,
    ) as exc:
        raise InvalidContent("not a valid image") from exc

    try:
        im = ImageOps.exif_transpose(im)
        has_alpha = im.mode in ("RGBA", "LA", "PA") or "transparency" in im.info
        im = im.convert("RGBA" if has_alpha else "RGB")
        out = tempfile.TemporaryFile()
        if mode == "lossless":
            im.save(out, "WEBP", lossless=True, quality=75, method=4)
        else:
            im.save(out, "WEBP", quality=quality, method=4)
    except (ValueError, OSError):
        # e.g. an exotic pixel mode, or larger than WebP's 16383 px limit.
        # The image is valid, so keep the original rather than rejecting it.
        log.info("could not convert image to WebP; keeping original")
        return None
    size = _size_of(out)
    return Compressed("webp", out, size)


def _optimise_pdf(src: BinaryIO) -> Compressed | None:
    src.seek(0)
    out = tempfile.TemporaryFile()
    try:
        with pikepdf.open(src) as pdf:
            pdf.save(
                out,
                compress_streams=True,
                recompress_flate=True,
                object_stream_mode=pikepdf.ObjectStreamMode.generate,
            )
    except Exception as exc:  # noqa: BLE001 - optimisation is optional for PDFs
        out.close()
        log.info("PDF not optimised (%s); keeping original", type(exc).__name__)
        return None
    size = _size_of(out)
    return Compressed("pdf_opt", out, size)
