"""Benchmark tests (no services needed)."""
import io
import json
import os

import pytest
from PIL import Image

from app import benchmark
from app.benchmark import check_upload, corpus_from_folder, run_benchmark, synthetic_corpus

TEXT = b"the quick brown fox jumps over the lazy dog\n" * 800


def _items(*pairs):
    return [(name, data, len(data)) for name, data in pairs]


# ---------- validation mirrors the upload route ----------

@pytest.mark.parametrize(
    "name, data, reason",
    [
        ("a.txt", TEXT, None),
        ("a.exe", b"MZ" + b"x" * 50, "extension not allowed"),
        ("a.png", b"MZ\x90\x00" + b"x" * 50, "content does not match extension"),
        ("a.txt", b"abc\x00def", "invalid text"),
        ("a.json", b"not json", "invalid text"),
        ("a.txt", b"", "empty file"),
    ],
)
def test_check_upload(name, data, reason):
    content_type, why = check_upload(name, data)
    assert why == reason
    assert (content_type is None) == (reason is not None)


def test_check_upload_enforces_size_limit():
    assert check_upload("a.txt", b"x" * 11, max_bytes=10)[1] == "too large"


# ---------- the arithmetic ----------

def test_numbers_add_up_for_a_known_corpus():
    report = run_benchmark(
        _items(
            ("one.txt", TEXT),
            ("copy.txt", TEXT),  # byte-identical re-upload
            ("fake.png", b"MZ\x90\x00" + b"x" * 40),  # rejected
            ("tool.exe", b"MZ" + b"x" * 40),  # rejected
        )
    )
    assert report.uploads == 2 and report.unique_files == 1 and report.duplicate_uploads == 1
    assert dict(report.rejected) == {
        "content does not match extension": 1,
        "extension not allowed": 1,
    }
    assert report.logical_bytes == 2 * len(TEXT)
    assert report.unique_original_bytes == len(TEXT)
    assert report.dedupe_only_reduction == pytest.approx(0.5)
    stored = report.physical_bytes
    assert stored < len(TEXT) / 5  # zstd shrinks repetitive text a lot
    assert report.compression_only_bytes == 2 * stored  # each upload compressed alone
    assert report.compression_only_reduction == pytest.approx(1 - stored / len(TEXT))
    assert report.combined_reduction == pytest.approx(1 - stored / (2 * len(TEXT)))
    # Combining can never save less than either part alone.
    assert report.combined_reduction >= report.compression_only_reduction
    assert report.combined_reduction >= report.dedupe_only_reduction


def test_sensitivity_formula():
    report = run_benchmark(_items(("a.txt", TEXT)))
    r = report.compression_on_unique
    by_rate = {row["duplicate_share"]: row["combined_reduction"] for row in report.sensitivity()}
    assert by_rate[0.0] == pytest.approx(r)
    assert by_rate[0.30] == pytest.approx(1 - (1 - r) * 0.7)
    assert report.sensitivity() == sorted(report.sensitivity(), key=lambda x: x["combined_reduction"])


def test_cost_estimate_scales_with_price():
    report = run_benchmark(_items(("a.txt", TEXT)))
    cheap, dear = report.cost(0.01), report.cost(0.02)
    assert dear["saved_per_month"] == pytest.approx(2 * cheap["saved_per_month"])
    assert cheap["per_tb_with"] < cheap["per_tb_without"]


def test_incompressible_data_is_never_counted_as_a_loss():
    tiny = b"x"
    report = run_benchmark(_items(("tiny.txt", tiny)))
    assert report.physical_bytes == len(tiny)  # stored unchanged
    assert report.combined_reduction == 0.0


def test_undecodable_image_is_rejected_not_counted():
    fake = b"\x89PNG\r\n\x1a\n" + b"junk" * 50
    report = run_benchmark(_items(("bad.png", fake), ("ok.txt", TEXT)))
    assert report.rejected["invalid image"] == 1
    assert report.uploads == 1 and report.logical_bytes == len(TEXT)


def test_lossless_saves_less_than_lossy_on_photos():
    rng = __import__("random").Random(5)
    buf = io.BytesIO()
    benchmark._photo(rng, 400, 300).save(buf, "PNG")
    items = _items(("p.png", buf.getvalue()))
    lossy = run_benchmark(items, image_mode="lossy")
    lossless = run_benchmark(items, image_mode="lossless")
    assert lossy.physical_bytes < lossless.physical_bytes


# ---------- synthetic corpus ----------

def _shas(files, dup_rate, seed):
    import hashlib

    return [hashlib.sha256(d).hexdigest() for _, d, _ in synthetic_corpus(files, dup_rate, seed)]


def test_synthetic_corpus_is_reproducible():
    assert _shas(14, 0.2, seed=3) == _shas(14, 0.2, seed=3)
    assert _shas(14, 0.2, seed=3) != _shas(14, 0.2, seed=4)


def test_synthetic_duplicate_rate_behaves():
    none = _shas(30, 0.0, seed=1)
    assert len(set(none)) == len(none)
    many = _shas(30, 0.6, seed=1)
    assert len(set(many)) < len(many)


def test_synthetic_corpus_passes_our_own_validation():
    report = run_benchmark(synthetic_corpus(files=20, dup_rate=0.2, seed=2))
    assert dict(report.rejected) == {}, "the generator must only produce acceptable files"
    assert report.uploads == 20
    kinds = set(report.by_type)
    assert {".jpg", ".txt"} <= kinds or len(kinds) >= 3  # a real mix, not one type
    assert 0 < report.combined_reduction < 1


# ---------- reading a folder ----------

def test_folder_corpus_walks_nested_dirs_skips_symlinks_and_huge_files(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_bytes(TEXT)
    (tmp_path / "sub" / "b.txt").write_bytes(TEXT + b"more\n")
    os.symlink(tmp_path / "a.txt", tmp_path / "link.txt")
    with open(tmp_path / "huge.csv", "wb") as f:
        f.truncate(benchmark.DEFAULT_MAX_BYTES + 1)  # sparse: costs no disk
    names = {name: data for name, data, _ in corpus_from_folder(str(tmp_path))}
    assert set(names) == {"a.txt", os.path.join("sub", "b.txt"), "huge.csv"}
    assert names["huge.csv"] is None  # reported as too large without being read
    report = run_benchmark(corpus_from_folder(str(tmp_path)))
    assert report.uploads == 2 and report.rejected["too large"] == 1


# ---------- command line ----------

def test_cli_synthetic_writes_json(tmp_path, capsys):
    out = tmp_path / "result.json"
    code = benchmark.main(["--synthetic", "--files", "10", "--json", str(out)])
    assert code == 0
    printed = capsys.readouterr().out
    assert "STORAGE BENCHMARK" in printed and "SENSITIVITY" in printed
    data = json.loads(out.read_text())
    assert data["uploads"] == 10
    assert {"compression_only", "dedupe_only", "combined"} <= set(data["reduction"])


def test_cli_rejects_missing_or_empty_folder(tmp_path, capsys):
    assert benchmark.main(["--corpus", str(tmp_path / "nope")]) == 2
    assert benchmark.main(["--corpus", str(tmp_path)]) == 1  # exists but has nothing usable
    assert "No acceptable files" in capsys.readouterr().out
