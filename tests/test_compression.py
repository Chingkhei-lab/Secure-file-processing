"""Compression unit tests (no services needed)."""
import io
import random

import pikepdf
import pytest
import zstandard
from PIL import Image

from app import compression
from app.compression import InvalidContent, compress


def _noisy_image(width=300, height=300, seed=1) -> Image.Image:
    """A photo-like picture: smooth gradient plus noise (hard for PNG, easy for WebP)."""
    rng = random.Random(seed)
    im = Image.new("RGB", (width, height))
    px = im.load()
    for y in range(height):
        for x in range(width):
            px[x, y] = (
                (x * 255 // width + rng.randint(0, 20)) % 256,
                (y * 255 // height + rng.randint(0, 20)) % 256,
                ((x + y) * 255 // (width + height) + rng.randint(0, 20)) % 256,
            )
    return im


def _encode(im: Image.Image, fmt: str, **kw) -> bytes:
    buf = io.BytesIO()
    im.save(buf, fmt, **kw)
    return buf.getvalue()


def _run(data: bytes, content_type: str, **kw):
    return compress(io.BytesIO(data), content_type, **kw)


def _read(result) -> bytes:
    result.fileobj.seek(0)
    return result.fileobj.read()


# ---------- images ----------

def test_png_becomes_smaller_webp():
    png = _encode(_noisy_image(), "PNG")
    result = _run(png, "image/png")
    assert result.method == "webp"
    assert result.size < len(png)
    out = Image.open(io.BytesIO(_read(result)))
    assert out.format == "WEBP" and out.size == (300, 300)


def test_jpeg_becomes_smaller_webp():
    jpeg = _encode(_noisy_image(), "JPEG", quality=95)
    result = _run(jpeg, "image/jpeg")
    assert result.method == "webp" and result.size < len(jpeg)


def test_lossless_mode_keeps_exact_pixels():
    src = Image.new("RGB", (200, 200), (10, 120, 200))  # flat colour: tiny either way
    png = _encode(src, "PNG", compress_level=0)  # deliberately inefficient PNG
    result = _run(png, "image/png", image_mode="lossless")
    assert result.method == "webp"
    out = Image.open(io.BytesIO(_read(result))).convert("RGB")
    assert out.tobytes() == src.tobytes()


def test_transparency_is_preserved():
    im = Image.new("RGBA", (200, 200), (255, 0, 0, 0))
    im.paste((0, 0, 255, 255), (50, 50, 150, 150))
    png = _encode(im, "PNG", compress_level=0)
    result = _run(png, "image/png")
    assert result.method == "webp"
    out = Image.open(io.BytesIO(_read(result)))
    assert out.mode == "RGBA"
    assert out.getpixel((10, 10))[3] == 0 and out.getpixel((100, 100))[3] == 255


def test_exif_orientation_applied_and_metadata_dropped():
    im = _noisy_image(width=60, height=30)
    exif = Image.Exif()
    exif[0x0112] = 6  # "rotate 90 degrees to display correctly"
    exif[0x010F] = "SecretCameraMaker"
    jpeg = _encode(im, "JPEG", quality=95, exif=exif)
    result = _run(jpeg, "image/jpeg")
    assert result.method == "webp"
    out = Image.open(io.BytesIO(_read(result)))
    assert out.size == (30, 60)  # rotated, so it still looks right
    assert b"SecretCameraMaker" not in _read(result)  # metadata gone


def test_animated_image_is_left_untouched():
    frames = [Image.new("RGB", (64, 64), c) for c in [(255, 0, 0), (0, 255, 0), (0, 0, 255)]]
    buf = io.BytesIO()
    frames[0].save(buf, "PNG", save_all=True, append_images=frames[1:], duration=100, loop=0)
    apng = buf.getvalue()
    result = _run(apng, "image/png")
    assert result.method == "none" and _read(result) == apng


def test_webp_input_is_not_recompressed():
    webp = _encode(_noisy_image(), "WEBP", quality=80)
    result = compress(io.BytesIO(webp), "image/webp")
    assert result.method == "none" and _read(result) == webp


def test_never_returns_something_bigger():
    # A tiny flat image: the WebP wrapper is bigger than a 1-colour PNG.
    png = _encode(Image.new("RGB", (4, 4), (1, 2, 3)), "PNG")
    result = _run(png, "image/png")
    assert result.size <= len(png)
    if result.method == "none":
        assert _read(result) == png


def test_garbage_with_png_header_is_rejected():
    fake = b"\x89PNG\r\n\x1a\n" + b"not really an image" * 20
    with pytest.raises(InvalidContent):
        _run(fake, "image/png")


def test_truncated_image_is_rejected():
    png = _encode(_noisy_image(), "PNG")
    with pytest.raises(InvalidContent):
        _run(png[: len(png) // 2], "image/png")


def test_format_mismatch_is_rejected():
    jpeg = _encode(_noisy_image(60, 60), "JPEG")
    with pytest.raises(InvalidContent, match="does not match"):
        _run(jpeg, "image/png")


def test_oversized_image_rejected_before_decoding():
    png = _encode(Image.new("RGB", (100, 100)), "PNG")
    with pytest.raises(InvalidContent, match="too large"):
        _run(png, "image/png", max_image_pixels=1000)


# ---------- text ----------

@pytest.mark.parametrize(
    "ctype, body",
    [
        ("text/plain", b"the quick brown fox\n" * 2000),
        ("text/csv", b"id,name,score\n" + b"1,alice,99\n2,bob,87\n" * 1500),
        ("application/json", b'{"items": [' + b'{"a": 1, "b": "xyz"},' * 1500 + b"{}]}"),
    ],
)
def test_text_types_use_zstd_and_roundtrip(ctype, body):
    result = _run(body, ctype)
    assert result.method == "zstd"
    assert result.size < len(body) / 5
    assert zstandard.ZstdDecompressor().decompress(_read(result)) == body


def test_tiny_text_is_not_expanded():
    result = _run(b"hello", "text/plain")
    assert result.method == "none" and _read(result) == b"hello"


# ---------- pdf ----------

def _uncompressed_pdf() -> bytes:
    pdf = pikepdf.new()
    page = pdf.add_blank_page()
    page.Contents = pdf.make_stream(b"BT /F1 12 Tf 72 700 Td (hello world) Tj ET\n" * 3000)
    buf = io.BytesIO()
    pdf.save(buf, compress_streams=False, object_stream_mode=pikepdf.ObjectStreamMode.disable)
    return buf.getvalue()


def test_pdf_is_optimised_and_still_valid():
    original = _uncompressed_pdf()
    result = _run(original, "application/pdf")
    assert result.method == "pdf_opt"
    assert result.size < len(original) / 2
    data = _read(result)
    with pikepdf.open(io.BytesIO(data)) as reopened:
        assert len(reopened.pages) == 1
    assert data.startswith(b"%PDF-")


def test_unparseable_pdf_is_kept_as_is():
    fake = b"%PDF-1.7\n" + b"this is not a real pdf\n" * 50
    result = _run(fake, "application/pdf")
    assert result.method == "none" and _read(result) == fake


# ---------- everything else ----------

def test_unknown_type_stored_as_is():
    result = _run(b"whatever", "application/x-something")
    assert result.method == "none" and _read(result) == b"whatever"


def test_method_names_match_database_constraint():
    from app.models import Compression

    allowed = {c.value for c in Compression}
    assert {"none", "zstd", "webp", "pdf_opt"} <= allowed


def test_module_exports_expected_api():
    assert hasattr(compression, "Compressed") and hasattr(compression, "InvalidContent")
