"""One-time S3 bucket hardening and cost rules.

    docker compose exec api python -m app.s3setup show
    docker compose exec api python -m app.s3setup apply

`apply` is safe to run repeatedly. It sets:

  * Block ALL public access (a stored file can never be made public by accident).
  * Default encryption at rest (SSE-S3, AES-256).
  * Lifecycle rules:
      - quarantine/   deleted after QUARANTINE_EXPIRE_DAYS. A safety net: files
                      that were never cleaned up cannot sit there forever.
      - objects/      Standard -> Standard-IA after LIFECYCLE_IA_DAYS, then
                      Glacier Instant Retrieval after LIFECYCLE_GLACIER_DAYS.
      - whole bucket  incomplete multipart uploads aborted after 1 day. Large
                      uploads that were interrupted leave invisible parts that
                      are billed until removed.

Notes on the tiering rule: S3 only transitions objects of 128 KB or more by
default, so small files stay in Standard (that is the cheaper choice for them).
Standard-IA charges a retrieval fee, so it suits files that are rarely read.
LocalStack stores these rules but does not run them; real AWS does.
"""
import argparse
import json
import sys

from botocore.exceptions import ClientError

from app import storage
from app.config import Settings, get_settings

PUBLIC_ACCESS_BLOCK = {
    "BlockPublicAcls": True,
    "IgnorePublicAcls": True,
    "BlockPublicPolicy": True,
    "RestrictPublicBuckets": True,
}
ENCRYPTION = {
    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
}


def build_lifecycle(
    *, quarantine_expire_days: int, ia_days: int, glacier_days: int
) -> dict:
    """Build the lifecycle configuration, rejecting combinations S3 would refuse
    or that would quietly cost money."""
    if quarantine_expire_days < 1:
        raise ValueError("quarantine_expire_days must be at least 1")
    if ia_days and ia_days < 30:
        raise ValueError("S3 requires at least 30 days before moving to Standard-IA")
    if glacier_days and ia_days and glacier_days < ia_days + 30:
        raise ValueError(
            "glacier_days must be at least 30 days after ia_days (Standard-IA minimum storage)"
        )
    if glacier_days and glacier_days < 1:
        raise ValueError("glacier_days must be positive")

    rules = [
        {
            "ID": "expire-leftover-quarantine",
            "Status": "Enabled",
            "Filter": {"Prefix": "quarantine/"},
            "Expiration": {"Days": quarantine_expire_days},
        },
        {
            "ID": "abort-incomplete-multipart-uploads",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
        },
    ]
    transitions = []
    if ia_days:
        transitions.append({"Days": ia_days, "StorageClass": "STANDARD_IA"})
    if glacier_days:
        transitions.append({"Days": glacier_days, "StorageClass": "GLACIER_IR"})
    if transitions:
        rules.append(
            {
                "ID": "tier-stored-objects",
                "Status": "Enabled",
                "Filter": {"Prefix": "objects/"},
                "Transitions": transitions,
            }
        )
    return {"Rules": rules}


def lifecycle_from_settings(settings: Settings) -> dict:
    return build_lifecycle(
        quarantine_expire_days=settings.quarantine_expire_days,
        ia_days=settings.lifecycle_ia_days,
        glacier_days=settings.lifecycle_glacier_days,
    )


def apply_setup(client, bucket: str, lifecycle: dict) -> list[str]:
    """Apply all three settings. Returns what was done."""
    client.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration=PUBLIC_ACCESS_BLOCK)
    client.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration=ENCRYPTION)
    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
    return [
        "blocked all public access",
        "enabled default encryption (AES-256)",
        f"set {len(lifecycle['Rules'])} lifecycle rules",
    ]


def _try(call):
    try:
        return call()
    except ClientError as exc:
        return {"not_set": exc.response.get("Error", {}).get("Code", "error")}


def show_setup(client, bucket: str) -> dict:
    pab = _try(lambda: client.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"])
    enc = _try(
        lambda: client.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]
    )
    life = _try(lambda: {"Rules": client.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]})
    return {"public_access_block": pab, "encryption": enc, "lifecycle": life}


def main() -> int:
    p = argparse.ArgumentParser(prog="app.s3setup")
    p.add_argument("command", choices=["show", "apply"])
    args = p.parse_args()
    settings = get_settings()
    client = storage.get_s3()
    if args.command == "show":
        print(json.dumps(show_setup(client, settings.s3_bucket), indent=2, default=str))
        return 0
    try:
        lifecycle = lifecycle_from_settings(settings)
    except ValueError as exc:
        print(f"Invalid settings: {exc}", file=sys.stderr)
        return 2
    for step in apply_setup(client, settings.s3_bucket, lifecycle):
        print("-", step)
    return 0


if __name__ == "__main__":
    sys.exit(main())
