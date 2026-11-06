"""Tests for the load test's own logic, so a bug there can't produce a false PASS."""
import random

from app.loadtest import EICAR, _payload, _pct


def test_malware_payload_contains_eicar():
    name, body = _payload(1, random.Random(1), malware=True, duplicate_of=None)
    assert EICAR in body and name.startswith("eicar_")


def test_duplicate_payload_is_identical_content():
    a = _payload(10, random.Random(1), malware=False, duplicate_of=4)
    b = _payload(4, random.Random(1), malware=False, duplicate_of=None)
    assert a[1] == b[1]


def test_unique_payloads_differ():
    r = random.Random(1)
    bodies = {_payload(i, r, False, None)[1] for i in range(30)}
    assert len(bodies) == 30


def test_percentile():
    assert _pct([1, 2, 3, 4, 100], 50) == 3
    assert _pct([], 95) == 0.0