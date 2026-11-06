"""Pure unit tests for filename sanitizing and content validation (no services)."""
import pytest

from app.validation import (
    ALLOWED,
    TextValidator,
    extension_of,
    matches_magic,
    sanitize_filename,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("photo.png", "photo.png"),
        ("../../etc/passwd", "passwd"),
        ("C:\\Users\\x\\evil.png", "evil.png"),
        ("a b?.png", "a b_.png"),
        ("..hidden.png", "hidden.png"),
        ("evil\x00.png", "evil.png"),
        ("name\u202egpj.png", "namegpj.png"),  # right-to-left override removed
        ("résumé.pdf", "résumé.pdf"),
        ("", "upload"),
        (None, "upload"),
        ("...", "upload"),
    ],
)
def test_sanitize_filename(raw, expected):
    assert sanitize_filename(raw) == expected


def test_sanitize_filename_limits_length_and_keeps_extension():
    name = sanitize_filename("x" * 500 + ".png")
    assert len(name) <= 100 and name.endswith(".png")


def test_sanitized_names_never_contain_separators():
    for raw in ["a/b", "a\\b", "../x", "..\\x", "/abs/path.txt"]:
        out = sanitize_filename(raw)
        assert "/" not in out and "\\" not in out


def test_extension_allowlist_is_what_we_intend():
    assert set(ALLOWED) == {".png", ".jpg", ".jpeg", ".webp", ".pdf", ".txt", ".csv", ".json"}
    assert extension_of("A.PNG") == ".png"
    assert extension_of("noext") == ""


def test_magic_bytes():
    assert matches_magic(ALLOWED[".png"], b"\x89PNG\r\n\x1a\n" + b"0" * 8)
    assert matches_magic(ALLOWED[".jpg"], b"\xff\xd8\xff\xe0" + b"0" * 12)
    assert matches_magic(ALLOWED[".pdf"], b"%PDF-1.7\n" + b"0" * 7)
    assert matches_magic(ALLOWED[".webp"], b"RIFF\x00\x00\x00\x00WEBPVP8 ")
    # Wrong content for the claimed type:
    assert not matches_magic(ALLOWED[".png"], b"MZ\x90\x00" + b"0" * 12)  # Windows exe
    assert not matches_magic(ALLOWED[".pdf"], b"\x89PNG\r\n\x1a\n" + b"0" * 8)
    assert not matches_magic(ALLOWED[".webp"], b"RIFF\x00\x00\x00\x00WAVEfmt ")  # RIFF but WAV


def test_text_validator_accepts_plain_text():
    v = TextValidator()
    v.feed(b"hello\nworld\ttab\r\n")
    v.finish()


def test_text_validator_handles_multibyte_char_split_across_chunks():
    data = "é€".encode("utf-8")
    v = TextValidator()
    for i in range(len(data)):  # feed one byte at a time
        v.feed(data[i : i + 1])
    v.finish()


@pytest.mark.parametrize("bad", [b"abc\x00def", b"\xff\xfe\xfa", b"x\x07y"])
def test_text_validator_rejects_binary(bad):
    v = TextValidator()
    with pytest.raises(ValueError):
        v.feed(bad)
        v.finish()


def test_text_validator_rejects_truncated_utf8_at_end():
    v = TextValidator()
    v.feed("é".encode("utf-8")[:1])
    with pytest.raises(ValueError):
        v.finish()


def test_json_must_start_with_object_or_array():
    ok = TextValidator(expect_json=True)
    ok.feed(b'\xef\xbb\xbf  \n{"a": 1}')
    ok.finish()
    with pytest.raises(ValueError):
        TextValidator(expect_json=True).feed(b"hello")
    empty = TextValidator(expect_json=True)
    empty.feed(b"   \n ")
    with pytest.raises(ValueError):
        empty.finish()
