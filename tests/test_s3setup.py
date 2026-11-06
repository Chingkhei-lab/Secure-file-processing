"""S3 setup tests: rule building (pure) and applying to a throwaway bucket."""
import uuid

import pytest
from botocore.exceptions import ClientError

from app import s3setup, storage
from app.config import get_settings

settings = get_settings()


def _rules(**kw):
    args = dict(quarantine_expire_days=7, ia_days=30, glacier_days=90)
    args.update(kw)
    return {r["ID"]: r for r in s3setup.build_lifecycle(**args)["Rules"]}


# ---------- building rules ----------

def test_default_rules():
    rules = _rules()
    assert set(rules) == {
        "expire-leftover-quarantine",
        "abort-incomplete-multipart-uploads",
        "tier-stored-objects",
    }
    assert rules["expire-leftover-quarantine"]["Filter"] == {"Prefix": "quarantine/"}
    assert rules["expire-leftover-quarantine"]["Expiration"] == {"Days": 7}
    assert rules["abort-incomplete-multipart-uploads"]["AbortIncompleteMultipartUpload"] == {
        "DaysAfterInitiation": 1
    }
    tier = rules["tier-stored-objects"]
    assert tier["Filter"] == {"Prefix": "objects/"}
    assert tier["Transitions"] == [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 90, "StorageClass": "GLACIER_IR"},
    ]


def test_quarantine_is_never_tiered_and_objects_never_expire():
    rules = _rules()
    assert "Transitions" not in rules["expire-leftover-quarantine"]
    assert "Expiration" not in rules["tier-stored-objects"]  # stored files are kept


def test_tiering_can_be_disabled():
    assert "tier-stored-objects" not in _rules(ia_days=0, glacier_days=0)
    only_ia = _rules(glacier_days=0)["tier-stored-objects"]["Transitions"]
    assert only_ia == [{"Days": 30, "StorageClass": "STANDARD_IA"}]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"quarantine_expire_days": 0},
        {"ia_days": 10},  # S3 refuses < 30 days to Standard-IA
        {"ia_days": 30, "glacier_days": 45},  # needs 30 more days in Standard-IA
        {"ia_days": -5},
    ],
)
def test_invalid_combinations_are_rejected(kwargs):
    with pytest.raises(ValueError):
        s3setup.build_lifecycle(**{"quarantine_expire_days": 7, "ia_days": 30, "glacier_days": 90, **kwargs})


def test_rules_come_from_settings():
    cfg = s3setup.lifecycle_from_settings(settings)
    assert any(r["ID"] == "tier-stored-objects" for r in cfg["Rules"])


# ---------- applying to a real (throwaway) bucket ----------

@pytest.fixture
def temp_bucket():
    client = storage.get_s3()
    name = f"test-s3setup-{uuid.uuid4().hex[:12]}"
    if settings.s3_region == "us-east-1":
        client.create_bucket(Bucket=name)
    else:
        client.create_bucket(
            Bucket=name, CreateBucketConfiguration={"LocationConstraint": settings.s3_region}
        )
    yield client, name
    try:
        client.delete_bucket(Bucket=name)
    except ClientError:
        pass


def test_apply_then_show_round_trip(temp_bucket):
    client, bucket = temp_bucket
    cfg = s3setup.lifecycle_from_settings(settings)
    steps = s3setup.apply_setup(client, bucket, cfg)
    assert len(steps) == 3

    shown = s3setup.show_setup(client, bucket)
    assert shown["public_access_block"] == s3setup.PUBLIC_ACCESS_BLOCK
    algo = shown["encryption"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    assert algo == "AES256"
    got = {r["ID"]: r for r in shown["lifecycle"]["Rules"]}
    assert set(got) == {r["ID"] for r in cfg["Rules"]}
    assert got["expire-leftover-quarantine"]["Expiration"]["Days"] == settings.quarantine_expire_days
    assert [t["StorageClass"] for t in got["tier-stored-objects"]["Transitions"]] == [
        "STANDARD_IA",
        "GLACIER_IR",
    ]


def test_apply_is_repeatable(temp_bucket):
    client, bucket = temp_bucket
    cfg = s3setup.lifecycle_from_settings(settings)
    s3setup.apply_setup(client, bucket, cfg)
    s3setup.apply_setup(client, bucket, cfg)  # must not fail or duplicate rules
    rules = s3setup.show_setup(client, bucket)["lifecycle"]["Rules"]
    assert len(rules) == len(cfg["Rules"])


def test_show_reports_unconfigured_bucket(temp_bucket):
    client, bucket = temp_bucket
    shown = s3setup.show_setup(client, bucket)
    assert "not_set" in shown["lifecycle"]
