"""Load test: a burst of signed uploads, then verify what the pipeline did.

    docker compose exec api python -m app.cli create-key --name loadtest   # note key + secret
    docker compose exec api python -m app.loadtest --key K --secret S --uploads 1000 --concurrency 50

What it proves (the numbers go in your README):
  * throughput and latency of the upload route (p50 / p95 / p99)
  * every upload reached a terminal state, and how long that took
  * EVERY planted EICAR test file ended 'infected' and NONE was stored
  * every clean file ended 'stored'

1000 uploads a day is about one every 86 seconds. This test sends the whole
day's volume in a few minutes, so a pass is a large safety margin.
"""
import argparse
import asyncio
import json
import os
import random
import statistics
import time
from urllib.parse import quote

import httpx

from app.client import sign_headers

EICAR = rb"X5O!P%@AP[4\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
TERMINAL = {"stored", "infected", "failed"}


def _payload(i: int, rng: random.Random, malware: bool, duplicate_of: int | None):
    """(filename, bytes). Content is unique unless duplicate_of is set; the
    filename is always unique per upload so a duplicate-content upload never
    collides with the original under replay protection.

    Malware is the exact 68-byte EICAR string. ClamAV's EICAR signature only
    matches when that string sits at the start of the file followed by nothing
    (or a single newline); appending a suffix defeats it. Infected jobs are
    never stored, so using identical bytes for every planted sample is safe.
    """
    if malware:
        return f"eicar_{i:05d}.txt", EICAR
    n = duplicate_of if duplicate_of is not None else i
    kind = n % 3
    if kind == 0:
        text = "\n".join(f"line {n} {rng.random()}" for _ in range(0)) or f"report {n}\n" + "lorem ipsum " * 400
        return f"note_{i:05d}.txt", text.encode()
    if kind == 1:
        rows = ["id,value"] + [f"{k},{(n * 31 + k) % 997}" for k in range(300)]
        return f"data_{i:05d}.csv", "\n".join(rows).encode()
    return f"doc_{i:05d}.json", json.dumps({"n": n, "items": list(range(200))}).encode()


async def _upload(client, base, key, secret, name, body, sem, results):
    target = "/v1/uploads?filename=" + quote(name, safe="")
    async with sem:
        headers = sign_headers(key, secret, "POST", target, body)
        t0 = time.perf_counter()
        try:
            r = await client.post(base + target, headers=headers, content=body, timeout=60)
            elapsed = time.perf_counter() - t0
            ok = r.status_code == 202
            results.append({
                "name": name, "ok": ok, "status": r.status_code, "latency": elapsed,
                "job_id": r.json().get("job_id") if ok else None,
            })
        except Exception as exc:  # noqa: BLE001
            results.append({"name": name, "ok": False, "status": repr(exc), "latency": time.perf_counter() - t0, "job_id": None})


async def _poll(client, base, key, secret, jobs: dict[str, dict], timeout_s: float):
    """Poll until every job is terminal. Sleeps >= 1s per round: an identical
    signed request repeated within the same second would be rejected as a replay."""
    deadline = time.monotonic() + timeout_s
    pending = set(jobs)
    sem = asyncio.Semaphore(25)

    async def one(job_id):
        target = f"/v1/uploads/{job_id}"
        async with sem:
            r = await client.get(base + target, headers=sign_headers(key, secret, "GET", target), timeout=30)
        if r.status_code == 200:
            data = r.json()
            jobs[job_id]["final"] = data["status"]
            if data["status"] in TERMINAL:
                jobs[job_id]["done_at"] = time.perf_counter()
                return job_id
        return None

    while pending and time.monotonic() < deadline:
        finished = await asyncio.gather(*(one(j) for j in list(pending)))
        pending -= {j for j in finished if j}
        if pending:
            await asyncio.sleep(2)
    return pending


def _pct(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


async def run(args) -> int:
    rng = random.Random(args.seed)
    plan = []
    for i in range(args.uploads):
        malware = rng.random() < args.malware_rate
        dup = rng.randrange(0, max(1, i)) if (not malware and i > 0 and rng.random() < args.dup_rate) else None
        plan.append((malware, *_payload(i, rng, malware, dup)))

    results: list[dict] = []
    sem = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(max_connections=args.concurrency + 10)
    async with httpx.AsyncClient(limits=limits) as client:
        t_start = time.perf_counter()
        await asyncio.gather(*(
            _upload(client, args.url, args.key, args.secret, name, body, sem, results)
            for _, name, body in plan
        ))
        upload_seconds = time.perf_counter() - t_start

        malware_names = {name for m, name, _ in plan if m}
        jobs = {r["job_id"]: {"name": r["name"], "t0": t_start} for r in results if r["ok"]}
        stuck = await _poll(client, args.url, args.key, args.secret, jobs, args.timeout)
        total_seconds = time.perf_counter() - t_start

    accepted = [r for r in results if r["ok"]]
    rejected = [r for r in results if not r["ok"]]
    lat = [r["latency"] for r in accepted]
    finals = {"stored": 0, "infected": 0, "failed": 0, "other": 0}
    leaked, missed = [], []
    for job_id, j in jobs.items():
        f = j.get("final")
        finals[f if f in finals else "other"] += 1
        if j["name"] in malware_names and f == "stored":
            leaked.append(j["name"])  # malware that reached storage = a breach
        if j["name"] in malware_names and f != "infected":
            missed.append((j["name"], f))
    clean_not_stored = [j["name"] for j in jobs.values() if j["name"] not in malware_names and j.get("final") != "stored"]
    times = [j["done_at"] - j["t0"] for j in jobs.values() if "done_at" in j]

    report = {
        "uploads_sent": len(plan),
        "malware_planted": len(malware_names),
        "accepted": len(accepted),
        "rejected": len(rejected),
        "reject_statuses": sorted({str(r["status"]) for r in rejected}),
        "upload_phase_seconds": round(upload_seconds, 2),
        "upload_rate_per_second": round(len(accepted) / upload_seconds, 1) if upload_seconds else 0,
        "projected_uploads_per_day_at_that_rate": int(len(accepted) / upload_seconds * 86400) if upload_seconds else 0,
        "upload_latency_ms": {
            "p50": round(_pct(lat, 50) * 1000, 1), "p95": round(_pct(lat, 95) * 1000, 1),
            "p99": round(_pct(lat, 99) * 1000, 1), "max": round(max(lat, default=0) * 1000, 1),
        },
        "all_processed_seconds": round(total_seconds, 2),
        "time_to_final_state_s": {
            "median": round(statistics.median(times), 2) if times else 0, "p95": round(_pct(times, 95), 2),
        },
        "final_states": finals,
        "not_finished": len(stuck),
        "MALWARE_STORED": len(leaked),
        "malware_not_marked_infected": missed,
        "clean_files_not_stored": len(clean_not_stored),
    }
    print(json.dumps(report, indent=2))
    ok = not leaked and not missed and not stuck and not rejected and not clean_not_stored
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> None:
    p = argparse.ArgumentParser(prog="app.loadtest")
    p.add_argument("--url", default=os.environ.get("LOADTEST_URL", "http://localhost:8000"))
    p.add_argument("--key", required=True)
    p.add_argument("--secret", required=True)
    p.add_argument("--uploads", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--malware-rate", type=float, default=0.05, help="share of uploads that are EICAR test files")
    p.add_argument("--dup-rate", type=float, default=0.10)
    p.add_argument("--timeout", type=float, default=600, help="seconds to wait for processing")
    p.add_argument("--seed", type=int, default=1)
    raise SystemExit(asyncio.run(run(p.parse_args())))


if __name__ == "__main__":
    main()