# Secure File Processing Service

An asynchronous file-ingestion service. Uploads are authenticated, quarantined,
virus-scanned, compressed, deduplicated, and stored in S3. Built with Python,
FastAPI, PostgreSQL, Redis, ClamAV, and Docker.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|signed POST| API[FastAPI ingestion route]
    API -->|verify HMAC, timestamp, replay, rate limit| R[(Redis)]
    API -->|stream, validate, quarantine copy| Q[(S3 quarantine/)]
    API -->|job row: quarantined| DB[(PostgreSQL)]
    API -->|enqueue| R
    R --> W[ARQ worker]
    W -->|INSTREAM| AV[ClamAV]
    AV -->|infected| X[Discard + mark infected]
    AV -->|clean| W
    W -->|compress, dedupe by SHA-256| S[(S3 objects/)]
    W -->|job row: stored| DB
    SW[Sweeper cron] -.->|recover lost jobs| W
    M[Nightly maintenance] -.->|purge failed files, archive old rows| DB
```

Job states: `quarantined` -> `scanning` -> `clean` -> `storing` -> `stored`,
with `infected` and `failed` as terminal states.

## Results (measured)

### Security / load test

`python -m app.loadtest --uploads 1000 --concurrency 50` on a 6-core, 7 GB
laptop (Docker Compose, one API process, one worker):

| Measure | Result |
|---|---|
| Uploads sent / accepted | 1000 / 1000 |
| EICAR test files planted / detected | 57 / 57 |
| Malware files stored | **0** |
| Clean files stored | 943 of 943 |
| Burst accepted in | 24.6 s |
| Everything fully processed in | 247 s (median 185 s per job, one worker) |

1,000 uploads a day is roughly one every 86 seconds; this test sends a full
day of volume in minutes. EICAR is a standard antivirus test file: it proves
the scan-and-quarantine pipeline works, not ClamAV's detection rate on
real-world malware. Raw results are in `docs/results/`.

### Storage reduction

`python -m app.benchmark --corpus <folder>` on a 17-file mixed corpus
(1.43 MB; no duplicate uploads, so these figures are compression only):

| File type | Files | Saved |
|---|---|---|
| CSV | 4 | 52.3% |
| JSON | 2 | 81.0% |
| TXT | 3 | 43.8% |
| PNG | 4 | 80.4% lossy WebP / 43.1% lossless |
| PDF | 4 | 4.9% |
| **All files** | 17 | **43.4% lossy / 26.3% lossless** |

This is a small corpus, so treat it as indicative, not a guarantee. Savings
depend heavily on the file mix: text and structured data compress well, images
depend on the image mode, and PDFs barely shrink. Lossy WebP changes image
pixels. Deduplication adds more only when the same file is uploaded repeatedly,
which depends on traffic, so it is reported separately. The corpus is not
included in this repository; `--synthetic` generates a demo corpus, but its
numbers are illustrative only. Estimated cost uses an S3 Standard price of
$0.023/GB-month, an assumption to check against AWS pricing.

## Features

* HMAC-signed requests with replay protection and per-key rate limits
* Streaming uploads with size, type, and content validation
* ClamAV scanning, failing closed
* Compression: images to WebP, text/CSV/JSON with zstd, PDFs optimised
* Global dedupe by SHA-256
* Archive table, nightly maintenance, index audit, and S3 lifecycle setup
* 211 automated tests against real PostgreSQL, Redis and S3

See [SECURITY.md](SECURITY.md) for the threat model and its limits.

## Quick start

```bash
cp .env.example .env        # then set POSTGRES_PASSWORD and SIGNING_MASTER_SECRET
Copy-Item .env.example .env # PowerShell alternative
docker compose up -d --build
docker compose exec api alembic upgrade head
docker compose exec api python -m app.s3setup apply
docker compose exec api python -m app.cli create-key --name demo
```

Upload with the reference client:

```bash
docker compose cp photo.png api:/tmp/photo.png
docker compose exec api python -m app.client upload --key <KEY> --secret <SECRET> --file /tmp/photo.png
```

Note: the load test sends 1000 signed uploads in minutes, which exceeds the
default rate limit. Set `RATE_LIMIT_PER_MINUTE=100000` for the test only, then
restore it.

## Tools

| Command | Purpose |
|---|---|
| `python -m app.stats` | live savings from what is actually stored |
| `python -m app.benchmark --corpus DIR` | compression and dedupe benchmark |
| `python -m app.loadtest --key K --secret S` | burst test plus malware verification |
| `python -m app.dbaudit` | confirm every hot query uses an index |
| `python -m app.janitor reconcile` | compare S3 with the database |
| `python -m app.s3setup apply` | bucket hardening and lifecycle rules |

## Tests

```bash
docker compose exec api pytest -q
```

## Limitations

Single-region, no download route, development TLS only, and signature-based
malware detection only. See SECURITY.md.

## License

MIT