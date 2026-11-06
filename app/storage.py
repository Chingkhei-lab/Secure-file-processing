"""S3 helpers. boto3 is synchronous, so async code calls these through
asyncio.to_thread (see app/routers/uploads.py)."""
import hashlib
from functools import lru_cache
from typing import BinaryIO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config import get_settings


@lru_cache
def get_s3():
    s = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint_url,
        region_name=s.s3_region,
        config=Config(
            connect_timeout=5,
            read_timeout=60,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def quarantine_key(job_id) -> str:
    """Quarantine objects are named by job ID only, never by user input."""
    return f"quarantine/{job_id}"


def final_key(sha256: str) -> str:
    """Permanent location of a unique blob, named by the SHA-256 of its
    original bytes (so identical files can only ever map to one key)."""
    return f"objects/{sha256[:2]}/{sha256}"


def ensure_bucket() -> None:
    s = get_settings()
    client = get_s3()
    try:
        client.head_bucket(Bucket=s.s3_bucket)
    except ClientError:
        if s.s3_region == "us-east-1":
            client.create_bucket(Bucket=s.s3_bucket)
        else:
            client.create_bucket(
                Bucket=s.s3_bucket,
                CreateBucketConfiguration={"LocationConstraint": s.s3_region},
            )


def put_fileobj(fileobj: BinaryIO, key: str) -> None:
    # Always stored as opaque bytes, so a browser or proxy can never be
    # tricked into rendering a quarantined file as HTML or script.
    get_s3().upload_fileobj(
        fileobj,
        get_settings().s3_bucket,
        key,
        ExtraArgs={"ContentType": "application/octet-stream"},
    )


def get_object_bytes(key: str) -> bytes:
    return get_s3().get_object(Bucket=get_settings().s3_bucket, Key=key)["Body"].read()


class ObjectNotFound(Exception):
    pass


def download_to_fileobj(key: str, fileobj: BinaryIO) -> tuple[str, int]:
    """Stream an object into `fileobj`. Returns (sha256 hex, size in bytes),
    computed over the bytes actually received."""
    try:
        body = get_s3().get_object(Bucket=get_settings().s3_bucket, Key=key)["Body"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            raise ObjectNotFound(key) from None
        raise
    hasher = hashlib.sha256()
    size = 0
    for chunk in body.iter_chunks(chunk_size=1024 * 1024):
        hasher.update(chunk)
        size += len(chunk)
        fileobj.write(chunk)
    return hasher.hexdigest(), size


def object_exists(key: str) -> bool:
    try:
        get_s3().head_object(Bucket=get_settings().s3_bucket, Key=key)
        return True
    except ClientError:
        return False


def delete_object(key: str) -> None:
    get_s3().delete_object(Bucket=get_settings().s3_bucket, Key=key)
