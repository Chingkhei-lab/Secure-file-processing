"""Storage-savings benchmark.

Runs a set of files through the SAME validation and compression code the
service uses, and measures how many bytes would be stored versus uploaded.

    python -m app.benchmark --corpus /path/to/your/files
    python -m app.benchmark --synthetic --files 60 --dup-rate 0.15
    ... add  --json results.json  to save the numbers
    ... add  --image-mode lossless  to see the cost of keeping exact pixels

It needs no database, Redis or S3, so it is fast and repeatable.

HOW TO READ THE RESULT
  * "compression only" is the defensible number: every upload compressed on its
    own. It depends only on what kind of files you have.
  * "dedupe" depends entirely on how often the same file is uploaded twice. In
    a folder of your own files that is whatever happens to be in there; in
    --synthetic mode it is the --dup-rate you chose (an ASSUMPTION).
  * The sensitivity table shows the combined saving for other duplicate rates,
    so you can see how much the headline depends on that assumption.
  * Lossy WebP changes image pixels. Run with --image-mode lossless to see the
    saving without altering any image.
"""
import argparse
import hashlib
import io
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from PIL import Image

from app import compression
from app.validation import ALLOWED, HEAD_BYTES, TextValidator, extension_of, matches_magic

GIB = 1024**3
DEFAULT_MAX_BYTES = 25 * 1024 * 1024  # mirrors MAX_UPLOAD_BYTES


# ---------- validation (same rules as the upload route) ----------

def check_upload(name: str, data: bytes, max_bytes: int = DEFAULT_MAX_BYTES) -> tuple[str | None, str | None]:
    """Returns (content_type, None) if the service would accept the file,
    otherwise (None, reason)."""
    ext = extension_of(name)
    ftype = ALLOWED.get(ext)
    if ftype is None:
        return None, "extension not allowed"
    if len(data) == 0:
        return None, "empty file"
    if len(data) > max_bytes:
        return None, "too large"
    if ftype.kind == "binary":
        if not matches_magic(ftype, data[:HEAD_BYTES]):
            return None, "content does not match extension"
    else:
        checker = TextValidator(expect_json=(ext == ".json"))
        try:
            checker.feed(data)
            checker.finish()
        except ValueError:
            return None, "invalid text"
    return ftype.content_type, None


# ---------- results ----------

@dataclass
class TypeStats:
    unique_files: int = 0
    original: int = 0
    stored: int = 0
    methods: Counter = field(default_factory=Counter)


@dataclass
class Report:
    uploads: int = 0
    unique_files: int = 0
    duplicate_uploads: int = 0
    rejected: Counter = field(default_factory=Counter)
    logical_bytes: int = 0  # everything uploaded, duplicates included
    unique_original_bytes: int = 0  # after dedupe only
    compression_only_bytes: int = 0  # each upload compressed, no dedupe
    physical_bytes: int = 0  # dedupe + compression: what S3 would hold
    seconds: float = 0.0
    by_type: dict[str, TypeStats] = field(default_factory=lambda: defaultdict(TypeStats))
    settings: dict = field(default_factory=dict)

    # -- derived numbers --
    @staticmethod
    def _reduction(after: int, before: int) -> float:
        return 0.0 if before == 0 else 1 - after / before

    @property
    def compression_only_reduction(self) -> float:
        return self._reduction(self.compression_only_bytes, self.logical_bytes)

    @property
    def dedupe_only_reduction(self) -> float:
        return self._reduction(self.unique_original_bytes, self.logical_bytes)

    @property
    def combined_reduction(self) -> float:
        return self._reduction(self.physical_bytes, self.logical_bytes)

    @property
    def compression_on_unique(self) -> float:
        return self._reduction(self.physical_bytes, self.unique_original_bytes)

    def sensitivity(self, rates=(0.0, 0.10, 0.20, 0.30)) -> list[dict]:
        """Combined saving if a fraction `d` of uploaded bytes were duplicates,
        keeping this corpus's compression ratio: 1 - (1 - r) * (1 - d)."""
        r = self.compression_on_unique
        return [{"duplicate_share": d, "combined_reduction": 1 - (1 - r) * (1 - d)} for d in rates]

    def cost(self, price_per_gb_month: float) -> dict:
        logical = self.logical_bytes / GIB * price_per_gb_month
        physical = self.physical_bytes / GIB * price_per_gb_month
        return {
            "price_per_gb_month": price_per_gb_month,
            "without_optimisation_per_month": logical,
            "with_optimisation_per_month": physical,
            "saved_per_month": logical - physical,
            "per_tb_without": 1024 * price_per_gb_month,
            "per_tb_with": 1024 * price_per_gb_month * (1 - self.combined_reduction),
        }

    def to_dict(self, price_per_gb_month: float = 0.023) -> dict:
        return {
            "uploads": self.uploads,
            "unique_files": self.unique_files,
            "duplicate_uploads": self.duplicate_uploads,
            "rejected": dict(self.rejected),
            "logical_bytes": self.logical_bytes,
            "unique_original_bytes": self.unique_original_bytes,
            "compression_only_bytes": self.compression_only_bytes,
            "physical_bytes": self.physical_bytes,
            "reduction": {
                "compression_only": self.compression_only_reduction,
                "dedupe_only": self.dedupe_only_reduction,
                "combined": self.combined_reduction,
                "compression_on_unique_files": self.compression_on_unique,
            },
            "sensitivity": self.sensitivity(),
            "cost": self.cost(price_per_gb_month),
            "by_type": {
                ext: {
                    "unique_files": t.unique_files,
                    "original_bytes": t.original,
                    "stored_bytes": t.stored,
                    "reduction": self._reduction(t.stored, t.original),
                    "methods": dict(t.methods),
                }
                for ext, t in sorted(self.by_type.items())
            },
            "seconds": self.seconds,
            "settings": self.settings,
        }


# ---------- the benchmark itself ----------

def run_benchmark(
    items: Iterable[tuple[str, bytes | None, int]],
    *,
    image_mode: str = "lossy",
    image_quality: int = 80,
    zstd_level: int = 15,
    max_image_pixels: int = 50_000_000,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Report:
    """`items` yields (name, data, size). `data` may be None when the file is
    already known to be too large to read (size is then used for rejection)."""
    report = Report(
        settings={
            "image_mode": image_mode,
            "image_quality": image_quality,
            "zstd_level": zstd_level,
            "max_image_pixels": max_image_pixels,
        }
    )
    seen: dict[str, int] = {}  # sha256 -> stored size
    started = time.perf_counter()

    for name, data, size in items:
        if data is None:
            report.rejected["too large"] += 1
            continue
        content_type, reason = check_upload(name, data, max_bytes)
        if reason:
            report.rejected[reason] += 1
            continue

        ext = extension_of(name)
        sha = hashlib.sha256(data).hexdigest()
        report.uploads += 1
        report.logical_bytes += len(data)

        if sha in seen:
            report.duplicate_uploads += 1
            report.compression_only_bytes += seen[sha]
            continue

        try:
            result = compression.compress(
                io.BytesIO(data),
                content_type,
                image_mode=image_mode,
                image_quality=image_quality,
                zstd_level=zstd_level,
                max_image_pixels=max_image_pixels,
            )
        except compression.InvalidContent:
            # The service would reject this file, so it is not an upload.
            report.uploads -= 1
            report.logical_bytes -= len(data)
            report.rejected["invalid image"] += 1
            continue
        try:
            stored = result.size
            method = result.method
        finally:
            result.fileobj.close()

        seen[sha] = stored
        stats = report.by_type[ext]
        stats.unique_files += 1
        stats.original += len(data)
        stats.stored += stored
        stats.methods[method] += 1
        report.unique_files += 1
        report.unique_original_bytes += len(data)
        report.physical_bytes += stored
        report.compression_only_bytes += stored

    report.seconds = time.perf_counter() - started
    return report


def corpus_from_folder(root: str) -> Iterator[tuple[str, bytes | None, int]]:
    """Walk a folder (sorted, so runs are repeatable). Symlinks are skipped."""
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for filename in sorted(filenames):
            path = os.path.join(dirpath, filename)
            if os.path.islink(path):
                continue
            try:
                size = os.path.getsize(path)
                if size > DEFAULT_MAX_BYTES:
                    yield os.path.relpath(path, root), None, size
                    continue
                with open(path, "rb") as f:
                    yield os.path.relpath(path, root), f.read(), size
            except OSError:
                continue


# ---------- synthetic corpus (Pillow + stdlib only, fully seeded) ----------

def _photo(rng: random.Random, width: int, height: int) -> Image.Image:
    """Photo-like picture: smooth multi-scale colour variation plus fine grain."""
    img = None
    for (lw, lh), weight in [((4, 3), 0.45), ((16, 12), 0.30), ((64, 48), 0.20), ((256, 192), 0.12)]:
        low = Image.frombytes("RGB", (lw, lh), rng.randbytes(lw * lh * 3))
        low = low.resize((width, height), Image.BICUBIC)
        img = low if img is None else Image.blend(img, low, weight)
    grain = Image.frombytes("L", (width, height), rng.randbytes(width * height))
    return Image.blend(img, Image.merge("RGB", (grain, grain, grain)), 0.05)


def _screenshot(rng: random.Random, width: int, height: int) -> Image.Image:
    """UI-screenshot-like picture: flat colours, blocks and rows of small marks."""
    from PIL import ImageDraw

    im = Image.new("RGB", (width, height), (245, 246, 248))
    d = ImageDraw.Draw(im)
    for _ in range(rng.randint(4, 9)):
        x0, y0 = rng.randrange(width - 60), rng.randrange(height - 40)
        d.rectangle(
            [x0, y0, x0 + rng.randint(60, width // 2), y0 + rng.randint(30, height // 3)],
            fill=(rng.randrange(256), rng.randrange(256), rng.randrange(256)),
        )
    for row in range(20, height - 20, 18):
        x = 24
        while x < width - 40:
            w = rng.randint(6, 40)
            d.rectangle([x, row, x + w, row + 7], fill=(40, 40, 48))
            x += w + rng.randint(5, 12)
    return im


def _encode(im: Image.Image, fmt: str, **kw) -> bytes:
    buf = io.BytesIO()
    im.save(buf, fmt, **kw)
    return buf.getvalue()


def _make_photo_jpg(rng, i):
    w, h = rng.choice([(1024, 768), (1280, 960), (1600, 1200)])
    return f"photo_{i:03d}.jpg", _encode(_photo(rng, w, h), "JPEG", quality=88)


def _make_photo_png(rng, i):
    w, h = rng.choice([(640, 480), (800, 600)])
    return f"photo_{i:03d}.png", _encode(_photo(rng, w, h), "PNG")


def _make_screenshot(rng, i):
    return f"screenshot_{i:03d}.png", _encode(_screenshot(rng, 1280, 800), "PNG")


def _make_log(rng, i):
    levels = ["INFO"] * 8 + ["WARN", "ERROR"]
    paths = ["/api/items", "/api/users", "/login", "/health", "/api/orders"]
    lines, t = [], 1_700_000_000 + rng.randrange(10**6)
    for _ in range(rng.randint(1500, 5000)):
        t += rng.randint(0, 3)
        lines.append(
            f"{t} {rng.choice(levels)} 10.0.{rng.randrange(8)}.{rng.randrange(256)} "
            f"GET {rng.choice(paths)} {rng.choice([200, 200, 200, 304, 404, 500])} "
            f"{rng.randint(2, 900)}ms\n"
        )
    return f"server_{i:03d}.txt", "".join(lines).encode()


def _make_csv(rng, i):
    names = [f"customer{n}" for n in range(60)]
    rows = ["id,name,region,amount,score\n"]
    for n in range(rng.randint(1500, 6000)):
        rows.append(
            f"{n},{rng.choice(names)},{rng.choice(['north', 'south', 'east', 'west'])},"
            f"{rng.uniform(1, 5000):.2f},{rng.randint(0, 100)}\n"
        )
    return f"export_{i:03d}.csv", "".join(rows).encode()


def _make_json(rng, i):
    recs = [
        {
            "id": n,
            "user": {"name": f"user{rng.randrange(80)}", "active": rng.random() < 0.8},
            "tags": rng.sample(["a", "b", "c", "d", "e", "f"], k=rng.randint(1, 4)),
            "value": round(rng.uniform(0, 1000), 3),
        }
        for n in range(rng.randint(800, 3000))
    ]
    return f"records_{i:03d}.json", json.dumps(recs).encode()


def _make_pdf(rng, i):
    """PDFs built by Pillow already use JPEG/Flate, like most real-world PDFs,
    so there is little left for the optimiser to find (that is the point)."""
    im = _screenshot(rng, 1240, 1754) if rng.random() < 0.5 else _photo(rng, 1000, 1400)
    # Pillow stamps the current time into PDFs; pin it so the corpus is identical every run.
    epoch = time.gmtime(0)
    return f"document_{i:03d}.pdf", _encode(im, "PDF", resolution=100.0, creationDate=epoch, modDate=epoch)


_MAKERS = [
    (0.28, _make_photo_jpg),
    (0.10, _make_photo_png),
    (0.10, _make_screenshot),
    (0.14, _make_log),
    (0.10, _make_csv),
    (0.10, _make_json),
    (0.18, _make_pdf),
]


def synthetic_corpus(
    files: int = 60, dup_rate: float = 0.15, seed: int = 1
) -> Iterator[tuple[str, bytes | None, int]]:
    """A reproducible mixed corpus of `files` uploads. Roughly `dup_rate` of
    them are byte-identical re-uploads of an earlier file."""
    rng = random.Random(seed)
    weights = [w for w, _ in _MAKERS]
    originals: list[tuple[str, bytes]] = []
    for i in range(files):
        if originals and rng.random() < dup_rate:
            name, data = rng.choice(originals)
            yield f"again_{i:03d}_{name}", data, len(data)
            continue
        _, maker = rng.choices(_MAKERS, weights=weights)[0]
        name, data = maker(rng, i)
        originals.append((name, data))
        yield name, data, len(data)


# ---------- reporting ----------

def _mb(n: int) -> str:
    return f"{n / 1_048_576:,.2f} MB"


def _pct(x: float) -> str:
    return f"{x * 100:5.1f}%"


def format_report(r: Report, price_per_gb_month: float, source: str) -> str:
    out = [f"STORAGE BENCHMARK  ({source})", "=" * 64]
    out.append(
        f"uploads accepted: {r.uploads}   unique files: {r.unique_files}   "
        f"duplicate uploads: {r.duplicate_uploads}"
    )
    if r.rejected:
        out.append("rejected by validation: " + ", ".join(f"{k}: {v}" for k, v in r.rejected.items()))
    s = r.settings
    out.append(
        f"settings: images {s['image_mode']}"
        + (f" q{s['image_quality']}" if s["image_mode"] == "lossy" else "")
        + f", zstd level {s['zstd_level']}"
    )
    out += ["", "BYTES"]
    out.append(f"  uploaded (logical)             {_mb(r.logical_bytes):>14}")
    out.append(f"  after compression only         {_mb(r.compression_only_bytes):>14}   saved {_pct(r.compression_only_reduction)}")
    out.append(f"  after dedupe only              {_mb(r.unique_original_bytes):>14}   saved {_pct(r.dedupe_only_reduction)}")
    out.append(f"  after both (what S3 holds)     {_mb(r.physical_bytes):>14}   saved {_pct(r.combined_reduction)}")
    out += ["", "BY FILE TYPE (unique files)", f"  {'type':6} {'files':>5} {'original':>12} {'stored':>12} {'saved':>7}  methods"]
    for ext, t in sorted(r.by_type.items()):
        saved = r._reduction(t.stored, t.original)
        methods = ", ".join(f"{m}:{n}" for m, n in t.methods.most_common())
        out.append(f"  {ext:6} {t.unique_files:>5} {_mb(t.original):>12} {_mb(t.stored):>12} {_pct(saved):>7}  {methods}")
    out += ["", "SENSITIVITY: combined saving if this share of uploaded bytes were duplicates"]
    for row in r.sensitivity():
        out.append(f"  {row['duplicate_share'] * 100:4.0f}% duplicates -> {_pct(row['combined_reduction'])}")
    c = r.cost(price_per_gb_month)
    out += [
        "",
        f"ESTIMATED S3 STORAGE COST (at ${c['price_per_gb_month']}/GB-month; storage line only)",
        f"  per TB uploaded:  ${c['per_tb_without']:,.2f}/month  ->  ${c['per_tb_with']:,.2f}/month",
        "",
        f"Took {r.seconds:.1f}s ({r.logical_bytes / 1_048_576 / max(r.seconds, 1e-9):.1f} MB/s uploaded).",
        "",
        "NOTES",
        "  - Quote 'compression only' as the dependable figure; the dedupe share is an",
        "    assumption about your traffic. Use the sensitivity table to show that.",
        "  - Lossy WebP alters image pixels. Re-run with --image-mode lossless for the",
        "    no-quality-loss figure.",
        "  - Savings cover S3 storage bytes only: request fees and the worker's compute",
        "    cost are not included.",
    ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="app.benchmark", description=__doc__.split("\n\n")[0])
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--corpus", metavar="DIR", help="folder of your own files")
    src.add_argument("--synthetic", action="store_true", help="generate a reproducible mixed corpus")
    p.add_argument("--files", type=int, default=60, help="synthetic: number of uploads")
    p.add_argument("--dup-rate", type=float, default=0.15, help="synthetic: share of repeated uploads")
    p.add_argument("--seed", type=int, default=1, help="synthetic: random seed")
    p.add_argument("--image-mode", choices=["lossy", "lossless"], default="lossy")
    p.add_argument("--image-quality", type=int, default=80)
    p.add_argument("--zstd-level", type=int, default=15)
    p.add_argument("--price-per-gb-month", type=float, default=0.023)
    p.add_argument("--json", metavar="PATH", help="also write the numbers to this file")
    args = p.parse_args(argv)

    if args.corpus:
        if not os.path.isdir(args.corpus):
            print(f"not a folder: {args.corpus}", file=sys.stderr)
            return 2
        items, source = corpus_from_folder(args.corpus), f"folder {args.corpus}"
    else:
        items = synthetic_corpus(args.files, args.dup_rate, args.seed)
        source = f"synthetic, {args.files} uploads, duplicate rate {args.dup_rate:.0%} (assumed), seed {args.seed}"

    report = run_benchmark(
        items,
        image_mode=args.image_mode,
        image_quality=args.image_quality,
        zstd_level=args.zstd_level,
    )
    if report.uploads == 0:
        print("No acceptable files found (allowed: " + ", ".join(sorted(ALLOWED)) + ").")
        if report.rejected:
            print("Rejected:", dict(report.rejected))
        return 1
    print(format_report(report, args.price_per_gb_month, source))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report.to_dict(args.price_per_gb_month), f, indent=2)
        print(f"\nNumbers written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
