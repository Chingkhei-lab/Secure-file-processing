"""Reference client: shows exactly how a caller signs requests.

Run inside the api container:

    docker compose exec api python -m app.client whoami --key K --secret S
    docker compose cp photo.png api:/tmp/photo.png
    docker compose exec api python -m app.client upload --key K --secret S --file /tmp/photo.png
    docker compose exec api python -m app.client status --key K --secret S --job-id <id>
    docker compose exec api python -m app.client list --key K --secret S
"""
import argparse
import hashlib
import os
import time
from urllib.parse import quote

import httpx

from app.security import compute_signature


def sign_headers(
    api_key: str,
    signing_secret: str,
    method: str,
    target: str,
    body: bytes = b"",
    timestamp: int | None = None,
) -> dict[str, str]:
    """Build the auth headers. `target` is the path plus '?query' if any."""
    ts = str(timestamp if timestamp is not None else int(time.time()))
    content_sha256 = hashlib.sha256(body).hexdigest()
    return {
        "X-API-Key": api_key,
        "X-Timestamp": ts,
        "X-Content-SHA256": content_sha256,
        "X-Signature": compute_signature(signing_secret, ts, method, target, content_sha256),
    }


def upload_target(filename: str) -> str:
    """Path + query for an upload. The filename travels in the query string so
    that it is covered by the request signature."""
    return "/v1/uploads?filename=" + quote(filename, safe="")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["whoami", "upload", "status", "list"])
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--key", required=True)
    parser.add_argument("--secret", required=True)
    parser.add_argument("--file")
    parser.add_argument("--job-id")
    args = parser.parse_args()

    if args.command == "upload":
        if not args.file:
            parser.error("upload needs --file")
        with open(args.file, "rb") as f:
            body = f.read()
        target = upload_target(os.path.basename(args.file))
        headers = sign_headers(args.key, args.secret, "POST", target, body)
        resp = httpx.post(args.url + target, headers=headers, content=body, timeout=60)
    else:
        target = {
            "whoami": "/v1/whoami",
            "list": "/v1/uploads",
            "status": f"/v1/uploads/{args.job_id}",
        }[args.command]
        headers = sign_headers(args.key, args.secret, "GET", target)
        resp = httpx.get(args.url + target, headers=headers)
    print(resp.status_code, resp.text)


if __name__ == "__main__":
    main()
