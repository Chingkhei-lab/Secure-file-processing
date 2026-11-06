# Security design

## Threat model

The service accepts files from authenticated API clients over HTTP. The main
risks are malware reaching storage, forged or replayed requests, hostile files
(decompression bombs, fake extensions), and one customer learning about
another's data.

## Controls

| Risk | Control |
|---|---|
| Malware in storage | Every upload lands in a quarantine prefix, is scanned by ClamAV, and is copied to its final location only if clean. The scanner fails closed: if ClamAV is unreachable the job fails and is never stored. The file's SHA-256 is re-verified before the scan and again before the store. |
| Forged requests | Each request carries an HMAC-SHA256 signature over timestamp, method, path and query, and body hash. The signing secret is derived per key from a master secret. |
| Replay | 5-minute timestamp window plus a Redis replay cache. |
| Stolen database | API keys are stored only as SHA-256 hashes. |
| Brute force | Per-key rate limit. Every authentication failure returns the same generic 401. |
| Hostile files | Size cap enforced on the bytes actually received, extension allowlist, magic-byte and UTF-8 checks, a pixel cap against decompression bombs, sanitised filenames, S3 keys built only from server-generated IDs. |
| Cross-tenant leakage | Per-key job visibility. The API never reveals whether a file was deduplicated, because that would let a caller test whether another customer holds a given file. |
| Lost work | Atomic state transitions, a sweeper that recovers lost queue messages and dead workers, and a nightly janitor. |
| Bucket exposure | `app.s3setup` blocks public access, enables encryption, and expires leftover quarantine objects. |

## What this does NOT protect against

* **Unknown malware.** ClamAV detects known signatures only. The load test uses
  the EICAR *test* file, which proves the pipeline works, not ClamAV's
  detection rate on real-world samples.
* **A leaked API key and secret.** Anyone holding both can upload as that
  customer until the key is revoked.
* **Transport security.** Run behind a TLS-terminating proxy in production.
  The development compose file serves plain HTTP.
* **Dependency vulnerabilities.** Pin and update dependencies and rebuild images regularly.
* **Secret management.** `.env` is for development. Production should use a
  secrets manager and IAM roles rather than static AWS keys.

## Reporting a vulnerability

Open a private security advisory on this repository.